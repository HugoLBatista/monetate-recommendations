from sqlalchemy import and_, literal_column, not_, or_, text, func, collate, literal
import json
# NOTE: availability/availability_date/expiration_date/sale_price_effective_date_begin/sale_price_effective_date_end
# not included so that such filters make the results update quickly
NON_PRODUCT_TYPE_PREFILTER_FIELDS = [
    'shipping_label', 'description', 'shipping_height', 'mpn', 'price', 'material', 'tax', 'shipping_weight',
    'image_link', 'color', 'link', 'adult', 'promotion_id', 'multipack', 'age_group', 'id', 'condition', 'size',
    'shipping', 'shipping_length', 'product_type', 'energy_efficiency_class', 'title', 'gender', 'size_type',
    'shipping_width', 'is_bundle', 'additional_image_link', 'loyalty_points', 'pattern', 'sale_price', 'mobile_link',
    'brand', 'item_group_id', 'availability']
SUPPORTED_PREFILTER_FIELDS = NON_PRODUCT_TYPE_PREFILTER_FIELDS + ['product_type']

SUPPORTED_PREFILTER_FUNCTIONS = ['items_from_base_recommendation_on']
# Assumptions:
# We don't want to do assertions or validation in this code. That should be done in WebUI.
# Field names should only be "product_type"


def collab_boolean(expression):
    # get the supported filters here
    expression_type = expression["type"]
    supported_filters = get_supported_collab_filters(expression["filters"])
    # if we have an "or" and also have unsupported filters, we can't precompute this at all
    if expression_type == "or" and len(supported_filters) != len(expression["filters"]):
        return text("1 = 1")  # we can't apply any early filters, so just don't apply filters
    if len(supported_filters) == 0:  # everything matches if we have no filters
        return text("1 = 1")
    sqlalchemy_type = and_ if expression_type == "and" else or_

    converted_filters = []
    for sub_expression in supported_filters:
        sub_result = convert(sub_expression)
        if sub_result is not None:
            converted_filters.append(sub_result)

    return sqlalchemy_type(*converted_filters) if converted_filters else None

def startswith_expression(expression):
    # converts a startswith filter JSON into a sql clause
    field = expression["left"]["field"]
    value = expression["right"]["value"]

    if field == "product_type" and expression["right"]["type"] == "function":
        return text("udf_startswith(recommendation.product_type, context.product_type)")

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(literal_column("recommendation." + field).startswith(literal(i)))
            if field == 'product_type':
                like_statements.append(literal_column("recommendation." + field).contains(',' + literal(i)))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)


def not_startswith_expression(expression):
    return not_(startswith_expression(expression))


def contains_expression(expression):

    field = expression["left"]["field"]
    value = expression["right"]["value"]

    if field == "product_type" and expression["right"]["type"] == "function":
        return text("udf_contains(recommendation.product_type, context.product_type)")

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(func.lower(literal_column("recommendation." + field)).contains(i.lower()))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)


def not_contains_expression(expression):
    """Converts a 'not contains' expression to a sqlalchemy expression by wrapping it in a not clause.

    Single-value comparisons are converted to a "NOT LIKE" instead of being wrapped.
    Falsey comparisons (empty list) are rendered as "NOT 1 = 2".
    """

    return not_(contains_expression(expression))


def get_field_and_lower_val(expression):
    return expression["left"]["field"], [(v.lower() if isinstance(v, basestring) else v) for v in expression["right"]["value"]]


def in_expression(expression):
    field, value = get_field_and_lower_val(expression)
    # product type uses == for true equality
    if expression['right']['type'] == 'function':
        return literal_column("recommendation." + field).__eq__(literal_column("context." + field))
    return func.lower(literal_column("recommendation." + field)).in_(value)


def not_in_expression(expression):
    return not_(in_expression(expression))


SQL_COMPARISON_TO_PYTHON_COMPARISON = {
    "==": "__eq__",
    "!=": "__ne__",
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
}


def direct_sql_expression(expression):
    field = expression["left"]["field"]
    value = expression["right"]["value"]
    # each of these direct sql expressions simply has a function that matches what we are looking for. see the mapping
    python_expr_equivalent = SQL_COMPARISON_TO_PYTHON_COMPARISON[expression["type"]]
    if field == 'product_type' and expression['right']['type'] == 'function':
        return literal_column("recommendation." + field).equals(literal_column("context." + field))
    # iterate through each item in the list of values and getattr to invoke the right comparison function
    statements = [getattr(literal_column("recommendation." + field), python_expr_equivalent)(literal(i)) for i in value if i is not None]\
        if type(value) is list else [getattr(literal_column("recommendation." + field), python_expr_equivalent)(literal(value))]
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


def convert(expression):
    expression_type = expression["type"]
    return COLLAB_FILTER_MAP[expression_type](expression)

def is_supported_collab_filter(f):
    is_supported_field = f['left']['field'].lower() in SUPPORTED_PREFILTER_FIELDS
    is_supported_function = f['right']['type'] != 'function' or f['right']['value'] in SUPPORTED_PREFILTER_FUNCTIONS
    return is_supported_field and is_supported_function

def get_supported_collab_filters(filters):
    return [filter for filter in filters if is_supported_collab_filter(filter)]


def get_query_and_variables_collab(recset_filter, global_filter):

    sql_expression = and_(convert(json.loads(recset_filter)), convert(json.loads(global_filter)))
    params = sql_expression.compile().params if sql_expression is not None else {}
    return ("WHERE " + str(sql_expression)), params

