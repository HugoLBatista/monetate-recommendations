from monetate_recommendations.precompute_view import precompute_view_algorithm
from monetate_recommendations.precompute_purchase import precompute_purchase_algorithm
from monetate_recommendations.precompute_purchase_value import precompute_purchase_value_algorithm

FUNC_MAP = {
    'view': precompute_view_algorithm,
    'purchase': precompute_purchase_algorithm,
    'purchase_value': precompute_purchase_value_algorithm,
}
