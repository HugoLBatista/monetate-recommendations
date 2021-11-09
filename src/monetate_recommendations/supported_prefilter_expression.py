from sqlalchemy import and_, literal_column, not_, or_, text, func, collate, literal

# NOTE: availability/availability_date/expiration_date/sale_price_effective_date_begin/sale_price_effective_date_end
# not included so that such filters make the results update quickly
NON_PRODUCT_TYPE_PREFILTER_FIELDS = [
    'shipping_label', 'description', 'shipping_height', 'mpn', 'price', 'material', 'tax', 'shipping_weight',
    'image_link', 'color', 'link', 'adult', 'promotion_id', 'multipack', 'age_group', 'id', 'condition', 'size',
    'shipping', 'shipping_length', 'product_type', 'energy_efficiency_class', 'title', 'gender', 'size_type',
    'shipping_width', 'is_bundle', 'additional_image_link', 'loyalty_points', 'pattern', 'sale_price', 'mobile_link',
    'brand', 'item_group_id']
SUPPORTED_PREFILTER_FIELDS = NON_PRODUCT_TYPE_PREFILTER_FIELDS + ['product_type']


# Assumptions:
# We don't want to do assertions or validation in this code. That should be done in WebUI.
# Field names should only be "product_type"

def boolean(expression):
    filters = expression["filters"]
    expression_type = expression["type"]
    sqlalchemy_type = and_ if expression_type == "and" else or_

    converted_filters = []
    for sub_expression in filters:
        sub_result = convert(sub_expression)
        if sub_result is not None:
            converted_filters.append(sub_result)

    return sqlalchemy_type(*converted_filters) if converted_filters else None


def startswith_expression(expression):
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

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(literal_column(field).startswith(i))
            if field == 'product_type':
                like_statements.append(literal_column(field).startswith(i))
                like_statements.append(literal_column(field).contains(',' + i))
            else:
                like_statements.append(literal_column("c." + field).startswith(i))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)


def not_startswith_expression(expression):
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

    return not_(startswith_expression(expression))


def contains_expression(expression):
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

    like_statements = []
    for i in value:
        if i is not None:
            if field == "product_type":
                like_statements.append(func.lower(literal_column(field)).contains(i.lower()))
            else:
                like_statements.append(func.lower(literal_column("c." + field)).contains(i.lower()))
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
    if field == "product_type":
        return func.lower(literal_column(field)).in_(value)
    else:
        return func.lower(literal_column("c." + field)).in_(value)


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
    # iterate through each item in the list of values and getattr to invoke the right comparison function
    if field == "product_type":
        statements = [getattr(literal_column(field), python_expr_equivalent)(i) for i in value if i is not None]\
            if type(value) is list else [getattr(literal_column(field), python_expr_equivalent)(value)]
    else:
        statements = [getattr(literal_column("c." + field), python_expr_equivalent)(literal(i)) for i in value if i is not None] \
            if type(value) is list else [getattr(literal_column("c." + field), python_expr_equivalent)(literal(value))]
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


def convert(expression):
    expression_type = expression["type"]
    return FILTER_MAP[expression_type](expression)


def and_with_convert_without_null(first_expression, second_expression):
    converted_first_expression = convert(first_expression)
    converted_second_expression = convert(second_expression)
    if converted_first_expression is None:
        return converted_second_expression
    elif converted_second_expression is None:
        return converted_first_expression
    else:
        return and_(converted_first_expression, converted_second_expression)


# non_product_type expressions are the early filter, product_type expressions are the late filter
def get_query_and_variables(non_product_type_expression, product_type_expression, second_non_product_type_expression,
                            second_product_type_expression):
    # we collate here to make sure that variable names dont get reused, e.g. "lower_1" showing up twice
    sql_expression = collate(and_with_convert_without_null(non_product_type_expression,
                                                           second_non_product_type_expression),
                             and_with_convert_without_null(product_type_expression,
                                                           second_product_type_expression))
    [early_filter, late_filter] = str(sql_expression).split(" COLLATE ")
    params = sql_expression.compile().params if sql_expression is not None else {}
    return ("AND " + early_filter) if early_filter != "NULL" else '', \
           ("WHERE " + late_filter) if late_filter != "NULL" else '', \
           params
