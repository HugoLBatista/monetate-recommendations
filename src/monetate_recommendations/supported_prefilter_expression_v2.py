from sqlalchemy import and_, literal_column, not_, or_, text, func, collate, literal
from .precompute_constants import SUPPORTED_PREFILTER_FIELDS, SUPPORTED_PREFILTER_FUNCTIONS, \
    UNSUPPORTED_PREFILTER_FIELDS, SUPPORTED_DATA_TYPES, DATA_TYPE_TO_SNOWFLAKE_TYPE
import json
import six

# we need to cast the data type for custom catalog attributes since the columns are 
# stored in the variant column in snowflake
def get_column(field, prefix, catalog_fields):
    catalog_field = next(catalog_field for catalog_field in catalog_fields
                         if catalog_field["name"].lower() == field.lower())
    return (prefix + "." + field) if field in SUPPORTED_PREFILTER_FIELDS else\
        (prefix + ".custom:" + field + "::" + DATA_TYPE_TO_SNOWFLAKE_TYPE[catalog_field["data_type"].lower()])

# Assumptions:
# We don't want to do assertions or validation in this code. That should be done in WebUI.
# Field names should only be "product_type"


def collab_boolean(expression, catalog_fields):
    # get the supported filters here
    expression_type = expression["type"]
    supported_filters = get_supported_collab_filters(expression["filters"], catalog_fields)
    # if we have an "or" and also have unsupported filters, we can't precompute this at all
    if expression_type == "or" and len(supported_filters) != len(expression["filters"]):
        return text("1 = 1")  # we can't apply any early filters, so just don't apply filters
    if len(supported_filters) == 0:  # everything matches if we have no filters
        return text("1 = 1")
    sqlalchemy_type = and_ if expression_type == "and" else or_

    converted_filters = []
    for sub_expression in supported_filters:
        sub_result = convert(sub_expression, catalog_fields)
        if sub_result is not None:
            converted_filters.append(sub_result)

    return sqlalchemy_type(*converted_filters) if converted_filters else None

def startswith_expression(expression, catalog_fields):
    # converts a startswith filter JSON into a sql clause
    field = expression["left"]["field"]
    value = expression["right"]["value"]

    if field == "product_type" and expression["right"]["type"] == "function":
        return text("any_startswith_udf(recommendation.product_type, context.product_type)")

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(literal_column(get_column(field, "recommendation", catalog_fields)).startswith(literal(i)))
            if field == 'product_type':
                like_statements.append(literal_column(get_column(field, "recommendation", catalog_fields)).contains(',' + literal(i)))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)


def not_startswith_expression(expression, catalog_fields):
    return not_(startswith_expression(expression, catalog_fields))


def contains_expression(expression, catalog_fields):

    field = expression["left"]["field"]
    value = expression["right"]["value"]

    if field == "product_type" and expression["right"]["type"] == "function":
        return text("any_contains_udf(recommendation.product_type, context.product_type)")

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(func.lower(literal_column(get_column(field, "recommendation", catalog_fields))).contains(i.lower()))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)


def not_contains_expression(expression, catalog_fields):
    """Converts a 'not contains' expression to a sqlalchemy expression by wrapping it in a not clause.

    Single-value comparisons are converted to a "NOT LIKE" instead of being wrapped.
    Falsey comparisons (empty list) are rendered as "NOT 1 = 2".
    """

    return not_(contains_expression(expression, catalog_fields))


def get_field_and_lower_val(expression):
    return expression["left"]["field"], [(v.lower() if isinstance(v, six.string_types) else v) for v in expression["right"]["value"]]


def in_expression(expression, catalog_fields):
    field, value = get_field_and_lower_val(expression)
    # product type uses == for true equality
    if expression['right']['type'] == 'function':
        return literal_column(get_column(field, "recommendation", catalog_fields)).__eq__(literal_column(get_column(field, "context", catalog_fields)))
    return func.lower(literal_column(get_column(field, "recommendation", catalog_fields))).in_(value)


def not_in_expression(expression, catalog_fields):
    return not_(in_expression(expression, catalog_fields))


SQL_COMPARISON_TO_PYTHON_COMPARISON = {
    "==": "__eq__",
    "!=": "__ne__",
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
}


def direct_sql_expression(expression, catalog_fields):
    field = expression["left"]["field"]
    value = expression["right"]["value"]
    # each of these direct sql expressions simply has a function that matches what we are looking for. see the mapping
    python_expr_equivalent = SQL_COMPARISON_TO_PYTHON_COMPARISON[expression["type"]]
    if field == 'product_type' and expression['right']['type'] == 'function':
        return literal_column(get_column(field, "recommendation", catalog_fields)).__eq__(literal_column(get_column(field, "context", catalog_fields)))
    # iterate through each item in the list of values and getattr to invoke the right comparison function
    statements = [getattr(literal_column(get_column(field, "recommendation", catalog_fields)), python_expr_equivalent)(literal(i)) for i in value if i is not None]\
        if type(value) is list else [getattr(literal_column(get_column(field, "recommendation", catalog_fields)), python_expr_equivalent)(literal(value))]
    # Multiple statements must be OR'ed together. Empty lists should return always false (1 = 2)
    return or_(*statements) if statements else text("1 = 2")


COLLAB_FILTER_MAP = {
    "and": collab_boolean,
    "or": collab_boolean,
    "startswith": startswith_expression,
    "not startswith": not_startswith_expression,
    "contains": contains_expression,
    "not contains": not_contains_expression,
    "in": in_expression,
    "not in": not_in_expression,
    "==": direct_sql_expression,
    "!=": direct_sql_expression,
    ">": direct_sql_expression,
    ">=": direct_sql_expression,
    "<": direct_sql_expression,
    "<=": direct_sql_expression,
}


def convert(expression, catalog_fields):
    expression_type = expression["type"]
    return COLLAB_FILTER_MAP[expression_type](expression, catalog_fields)

def is_supported_collab_filter(f, catalog_fields):
    field = f['left']['field'].lower()
    catalog_field = next((catalog_field for catalog_field in catalog_fields
                          if catalog_field["name"].lower() == field.lower()), None)
    is_supported_field = (field not in UNSUPPORTED_PREFILTER_FIELDS) and catalog_field and catalog_field["data_type"].lower() in SUPPORTED_DATA_TYPES
    is_supported_function = f['right']['type'] != 'function' or f['right']['value'] in SUPPORTED_PREFILTER_FUNCTIONS
    return is_supported_field and is_supported_function

def get_supported_collab_filters(filters, catalog_fields):
    return [filter for filter in filters if is_supported_collab_filter(filter, catalog_fields)]


def get_query_and_variables_collab(recset_filter, global_filter, catalog_fields):

    sql_expression = and_(convert(json.loads(recset_filter), catalog_fields), convert(json.loads(global_filter), catalog_fields))
    params = sql_expression.compile().params if sql_expression is not None else {}
    return ("WHERE " + str(sql_expression)), params

