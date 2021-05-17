from sqlalchemy import and_, literal_column, not_, or_, text, func


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
            like_statements.append(literal_column(field).contains(',' + i))
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
            like_statements.append(func.lower(literal_column(field)).contains(i.lower()))
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


def exact_equal_expression(expression):
    """Convert `equals` type `filter_json` expressions to SQLAlchemy `BooleanClauseList`.
       NB:
       The query effectively matches COMPARISON_OPERATIONS_STRING_TO_LIST['contains'] in filter_json.json_expression

       So, the `filter_json` expression
       ```
       {
               "type": "==",
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
       (product_type == red)
       ```

       `value` must be a python list of strings.
       - Each string is compared against for the `==` operation and the results are OR'ed
         (i.e. if any string matches, the result matches).
       - Empty lists do not match any value.
       - Any None values in the list are ignored.
       """

    field = expression["left"]["field"]
    value = expression["right"]["value"]

    like_statements = []
    for i in value:
        if i is not None:
            like_statements.append(literal_column(field).__eq__(i))
    if not like_statements:
        return text("1 = 2")  # Empty lists should return always false
    # Multiple statements must be OR'ed together.
    return or_(*like_statements)


FILTER_MAP = {
    "and": boolean,
    "or": boolean,
    "startswith": startswith_expression,
    "not startswith": not_startswith_expression,
    "contains": contains_expression,
    "not contains": not_contains_expression,
    "==": exact_equal_expression,
}


def get_product_type_variables(expression):
    """Gets all product type variables to be used with a filter expression
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
    -->
    {
        "product_type_1": "Apparel > Jeans",
        "product_type_2": ",Apparel > Jeans",
        "product_type_3": "Halloween > Texas"
        "product_type_4": ",Halloween > Texas"
    }
    """
    filters = expression.get("filters")
    product_types = []
    if filters and len(filters):
        for f in filters:
            if f["left"]["field"] == "product_type":
                for value in f["right"]["value"]:
                    # Each product_type gets two LIKE statements, the second matches when startswith ','
                    product_types.append(value)
                    product_types.append(',' + value)
    variables = {}
    for i, product_type in enumerate(product_types):
        variables["product_type_{}".format(i + 1)] = product_type
        variables["lower_{}".format(i + 1)] = product_type
    return variables


def convert(expression):
    expression_type = expression["type"]
    return FILTER_MAP[expression_type](expression)


def get_query_and_variables(expression):
    filter_variables = get_product_type_variables(expression)
    filter_query = text('WHERE {}'.format(convert(expression))) if len(filter_variables.keys()) else ''
    return filter_variables, filter_query
