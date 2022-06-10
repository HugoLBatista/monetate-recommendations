from sqlalchemy import and_, literal_column, not_, or_, text, func, collate, literal

# NOTE: availability/availability_date/expiration_date/sale_price_effective_date_begin/sale_price_effective_date_end
# not included so that such filters make the results update quickly
NON_PRODUCT_TYPE_PREFILTER_FIELDS = [
    'shipping_label', 'description', 'shipping_height', 'mpn', 'price', 'material', 'tax', 'shipping_weight',
    'image_link', 'color', 'link', 'adult', 'promotion_id', 'multipack', 'age_group', 'id', 'condition', 'size',
    'shipping', 'shipping_length', 'energy_efficiency_class', 'title', 'gender', 'size_type',
    'shipping_width', 'is_bundle', 'additional_image_link', 'loyalty_points', 'pattern', 'sale_price', 'mobile_link',
    'brand', 'item_group_id', 'availability']
SUPPORTED_PREFILTER_FIELDS = NON_PRODUCT_TYPE_PREFILTER_FIELDS + ['product_type']
UNSUPPORTED_PREFILTER_FIELDS = [
    'retailer_id', 'dataset_id', 'id', 'availability_date', 'expiration_date', 'sale_price_effective_date_begin',
    'sale_price_effective_date_end', 'update_time'
]
SUPPORTED_DATA_TYPES = [
    'string', 'number', 'datetime', 'boolean'
]
DATA_TYPE_TO_SNOWFLAKE_TYPE = {
    'string': 'string',
    'number': 'number',
    'datetime': 'datetime',
    'boolean': 'boolean'
}


# for non-prod type filters, we want to be more specific by using the catalog alias when referencing catalog fields
def get_column(field, catalog_fields):
    if field == 'product_type':
        return 'product_type'
    catalog_field = next(catalog_field for catalog_field in catalog_fields if catalog_field["name"].lower() == field)
    return ("c." + field) if field in SUPPORTED_PREFILTER_FIELDS else \
        ("c.custom:" + field + "::" + DATA_TYPE_TO_SNOWFLAKE_TYPE[catalog_field["data_type"].lower()])

# Assumptions:
# We don't want to do assertions or validation in this code. That should be done in WebUI.
# Field names should only be "product_type"

def boolean(expression, catalog_fields):
    filters = expression["filters"]
    expression_type = expression["type"]
    sqlalchemy_type = and_ if expression_type == "and" else or_

    converted_filters = []
    for sub_expression in filters:
        sub_result = convert(sub_expression, catalog_fields)
        if sub_result is not None:
            converted_filters.append(sub_result)

    return sqlalchemy_type(*converted_filters) if converted_filters else None


def startswith_expression(expression, catalog_fields):
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
            like_statements.append(literal_column(get_column(field, catalog_fields)).startswith(i))
            if field == 'product_type':
                like_statements.append(literal_column(field).contains(',' + i))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)


def not_startswith_expression(expression, catalog_fields):
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

    return not_(startswith_expression(expression, catalog_fields))


def contains_expression(expression, catalog_fields):
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
            like_statements.append(func.lower(literal_column(get_column(field, catalog_fields))).contains(i.lower()))
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
    return expression["left"]["field"], [(v.lower() if isinstance(v, basestring) else v) for v in expression["right"]["value"]]


def in_expression(expression, catalog_fields):
    field, value = get_field_and_lower_val(expression)
    return func.lower(literal_column(get_column(field, catalog_fields))).in_(value)


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
    # iterate through each item in the list of values and getattr to invoke the right comparison function
    statements = [getattr(literal_column(get_column(field, catalog_fields)), python_expr_equivalent)(literal(i)) for i in value if i is not None]\
        if type(value) is list else [getattr(literal_column(get_column(field, catalog_fields)), python_expr_equivalent)(literal(value))]
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


def convert(expression, catalog_fields):
    expression_type = expression["type"]
    return FILTER_MAP[expression_type](expression, catalog_fields)


def and_with_convert_without_null(first_expression, second_expression, catalog_fields):
    converted_first_expression = convert(first_expression, catalog_fields)
    converted_second_expression = convert(second_expression, catalog_fields)
    if converted_first_expression is None:
        return converted_second_expression
    elif converted_second_expression is None:
        return converted_first_expression
    else:
        return and_(converted_first_expression, converted_second_expression)


def get_query_and_variables(product_type_expression, non_product_type_expression, second_product_type_expression,
                            second_non_product_type_expression, catalog_fields):
    # we collate here to make sure that variable names dont get reused, e.g. "lower_1" showing up twice
    sql_expression = collate(and_with_convert_without_null(product_type_expression,
                                                           second_product_type_expression, catalog_fields),
                             and_with_convert_without_null(non_product_type_expression,
                                                           second_non_product_type_expression, catalog_fields))
    [early_filter, late_filter] = str(sql_expression).split(" COLLATE ")
    params = sql_expression.compile().params if sql_expression is not None else {}
    return ("AND " + early_filter) if early_filter != "NULL" else '', \
           ("WHERE " + late_filter) if late_filter != "NULL" else '', \
           params
