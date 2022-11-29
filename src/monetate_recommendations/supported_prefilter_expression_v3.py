from sqlalchemy import and_, literal_column, not_, or_, text, func, collate, literal
from .precompute_constants import SUPPORTED_PREFILTER_FIELDS, SUPPORTED_PREFILTER_FUNCTIONS, \
    UNSUPPORTED_PREFILTER_FIELDS, SUPPORTED_DATA_TYPES, DATA_TYPE_TO_SNOWFLAKE_TYPE
import json
import six


# we need to cast the data type for custom catalog attributes since the columns are 
# stored in the variant column in snowflake
def get_column(field, prefix, catalog_fields, is_collab):
    if not is_collab and field == 'product_type':
        return 'product_type'
    catalog_field = next(catalog_field for catalog_field in catalog_fields
                         if catalog_field["name"].lower() == field.lower())
    return (prefix + "." + field) if field in SUPPORTED_PREFILTER_FIELDS else\
        (prefix + ".custom:" + field + "::" + DATA_TYPE_TO_SNOWFLAKE_TYPE[catalog_field["data_type"].lower()])

# Assumptions:
# We don't want to do assertions or validation in this code. That should be done in WebUI.
# Field names should only be "product_type"

def boolean(expression, catalog_fields, is_collab):
    # get the supported filters here
    expression_type = expression["type"]
    supported_filters = expression['filters']
    if len(supported_filters) == 0:  # everything matches if we have no filters
        return text("1 = 1")
    sqlalchemy_type = and_ if expression_type == "and" else or_

    converted_filters = []
    for sub_expression in supported_filters:
        sub_result = convert(sub_expression, catalog_fields, is_collab)
        if sub_result is not None:
            converted_filters.append(sub_result)

    return sqlalchemy_type(*converted_filters) if converted_filters else None


def startswith_expression(expression, catalog_fields, is_collab):
    """Convert `startswith` type `filter_json` expressions to SQLAlchemy `BooleanClauseList`.
    NB:
    The query effectively matches COMPARISON_OPERATIONS_STRING_TO_LIST['startswith'] in filter_json.json_expression

    So, the `filter_json` expression
    ```
    {
            "type": "startswith",
            "left": {
                "type": "field",
                "field": "product_type"
            },
            "right": {
                "type": "value",
                "value": ["Apparel > Jeans", "Halloween > Texas"]
            }
        }
    ```
    can be rendered as an SQL clause
    product_type_1 = 'Apparel > Jeans'
    product_type_2 = ',Apparel > Jeans'
    product_type_3 = 'Halloween > Texas'
    product_type_4 = ',Halloween > Texas
    ```sql
    (product_type LIKE :product_type_1 || '%%')
    OR
    (product_type LIKE '%%' || :product_type_2 || '%%')
    OR
    (product_type LIKE :product_type_3 || '%%')
    OR
    (product_type LIKE '%%' || :product_type_4 || '%%')
    ```

    with values provided using bound parameters `["Apparel > Jeans", "Apparel > Jeans"]`.

    `value` must be a python list of strings.
    - Each string is compared against for the `startswith` operation and the results are OR'ed
      (i.e. if any string matches, the result matches).
    - Empty lists do not match any value.
    - Any None values in the list are ignored.
    """

    field = expression["left"]["field"]
    value = expression["right"]["value"]
    if is_collab and field == "product_type" and expression["right"]["type"] == "function":
        return text("any_startswith_udf(parse_csv_string_udf(recommendation.product_type), parse_csv_string_udf(context.product_type))")

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(literal_column(get_column(field, "lc", catalog_fields, is_collab)).startswith(literal(i)))
            if field == 'product_type':
                like_statements.append(literal_column(get_column(field, "lc", catalog_fields, is_collab)).contains(',' + literal(i)))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)

def not_startswith_expression(expression, catalog_fields, is_collab):
    """Converts a 'not startswith' expression to a sqlalchemy expression by wrapping it in a not clause.
    {
        "type": "not startswith",
        "left": {
            "type": "field",
            "field": "product_type"
        },
        "right": {
            "type": "value",
            "value": ["Apparel > Jeans", "Halloween > Texas"]
        }
    }
    -->
    NOT (
        (product_type LIKE :product_type_1 || '%%')
        OR
        (product_type LIKE '%%' || :product_type_2 || '%%')
        OR
        (product_type LIKE :product_type_3 || '%%')
        OR
        (product_type LIKE '%%' || :product_type_4 || '%%')
    )

    Single-value comparisons are converted to a "NOT LIKE" instead of being wrapped.
    Falsey comparisons (empty list) are rendered as "NOT 1 = 2".
    """

    return not_(startswith_expression(expression, catalog_fields, is_collab))

def contains_expression(expression, catalog_fields, is_collab):
    """Convert `contains` type `filter_json` expressions to SQLAlchemy `BooleanClauseList`.
    NB:
    The query effectively matches COMPARISON_OPERATIONS_STRING_TO_LIST['contains'] in filter_json.json_expression

    So, the `filter_json` expression
    ```
    {
            "type": "contains",
            "left": {
                "type": "field",
                "field": "product_type"
            },
            "right": {
                "type": "value",
                "value": ["red"]
            }
        }
    ```
    can be rendered as an SQL clause
    ```sql
    (lower(product_type) LIKE '%%' red '%%')
    ```

    `value` must be a python list of strings.
    - Each string is compared against for the `contains` operation and the results are OR'ed
      (i.e. if any string matches, the result matches).
    - Empty lists do not match any value.
    - Any None values in the list are ignored.
    """

    field = expression["left"]["field"]
    value = expression["right"]["value"]
    if is_collab and field == "product_type" and expression["right"]["type"] == "function":
        return text("any_contains_udf(parse_csv_string_udf(recommendation.product_type), parse_csv_string_udf(context.product_type))")

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(func.lower(literal_column(get_column(field, "lc", catalog_fields, is_collab))).contains(i.lower()))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)

def not_contains_expression(expression, catalog_fields, is_collab):
    """Converts a 'not contains' expression to a sqlalchemy expression by wrapping it in a not clause.

    Single-value comparisons are converted to a "NOT LIKE" instead of being wrapped.
    Falsey comparisons (empty list) are rendered as "NOT 1 = 2".
    """

    return not_(contains_expression(expression, catalog_fields, is_collab))

def get_field_and_lower_val(expression):
    return expression["left"]["field"], [(v.lower() if isinstance(v, six.string_types) else v) for v in expression["right"]["value"]]

def in_expression(expression, catalog_fields, is_collab):
    field, value = get_field_and_lower_val(expression)
    # product type uses == for true equality
    if is_collab and  expression['right']['type'] == 'function':
        return literal_column(get_column(field, "recommendation", catalog_fields, is_collab)).__eq__(literal_column(get_column(field, "context", catalog_fields, is_collab)))
    return func.lower(literal_column(get_column(field, "lc", catalog_fields, is_collab))).in_(value)


def not_in_expression(expression, catalog_fields, is_collab):
    return not_(in_expression(expression, catalog_fields, is_collab))

SQL_COMPARISON_TO_PYTHON_COMPARISON = {
    "==": "__eq__",
    "!=": "__ne__",
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
}

def direct_sql_expression(expression, catalog_fields, is_collab):
    field = expression["left"]["field"]
    value = expression["right"]["value"]
    # each of these direct sql expressions simply has a function that matches what we are looking for. see the mapping
    python_expr_equivalent = SQL_COMPARISON_TO_PYTHON_COMPARISON[expression["type"]]
    if is_collab and field == 'product_type' and expression['right']['type'] == 'function':
        return literal_column(get_column(field, "recommendation", catalog_fields, is_collab)).__eq__(literal_column(get_column(field, "context", catalog_fields, is_collab)))
    # iterate through each item in the list of values and getattr to invoke the right comparison function
    statements = [getattr(literal_column(get_column(field, "lc", catalog_fields, is_collab)), python_expr_equivalent)(literal(i)) for i in value if i is not None]\
        if type(value) is list else [getattr(literal_column(get_column(field, "lc", catalog_fields, is_collab)), python_expr_equivalent)(literal(value))]
    # Multiple statements must be OR'ed together. Empty lists should return always false (1 = 2)
    return or_(*statements) if statements else text("1 = 2")


FILTER_MAP = {
    "and": boolean,
    "or": boolean,
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


def convert(expression, catalog_fields, is_collab):
    expression_type = expression["type"]
    return FILTER_MAP[expression_type](expression, catalog_fields, is_collab)


def and_with_convert_without_null(first_expression, second_expression, catalog_fields):
    converted_first_expression = convert(first_expression, catalog_fields, False)
    converted_second_expression = convert(second_expression, catalog_fields, False)
    if converted_first_expression is None:
        return converted_second_expression
    elif converted_second_expression is None:
        return converted_first_expression
    else:
        return and_(converted_first_expression, converted_second_expression)


def get_query_and_variables_collab(recset_filter, global_filter, catalog_fields):
    if recset_filter['filters'] and global_filter['filters']:
        sql_expression = and_(convert(recset_filter, catalog_fields, True), convert(global_filter, catalog_fields, True))
    elif recset_filter['filters']:
        sql_expression = convert(recset_filter, catalog_fields, True)
    else:
        sql_expression = convert(global_filter, catalog_fields, True)
    params = sql_expression.compile().params if sql_expression is not None else {}
    return str(sql_expression), params

# non_product_type expressions are the early filter, product_type expressions are the late filter
def get_query_and_variables_non_collab(non_product_type_expression, product_type_expression, second_non_product_type_expression,
                            second_product_type_expression, catalog_fields):
    # we collate here to make sure that variable names dont get reused, e.g. "lower_1" showing up twice
    sql_expression = collate(and_with_convert_without_null(non_product_type_expression,
                                                           second_non_product_type_expression, catalog_fields),
                             and_with_convert_without_null(product_type_expression,
                                                           second_product_type_expression, catalog_fields))
    [early_filter, late_filter] = str(sql_expression).split(" COLLATE ")
    params = sql_expression.compile().params if sql_expression is not None else {}
    return ("WHERE " + early_filter) if early_filter != "NULL" else '', \
           ("WHERE " + late_filter) if late_filter != "NULL" else '', \
           params