from collections import defaultdict
from monetate_recommendations.precompute_purchase_also_purchase import precompute_purchase_also_purchase_algorithm
from monetate_recommendations.precompute_view_also_view import precompute_view_also_view_algorithm
from monetate.recs.models import PrecomputeQueue

FUNC_MAP = {
    'purchase_also_purchase': precompute_purchase_also_purchase_algorithm,
    'view_also_view': precompute_view_also_view_algorithm,
    'similar_products_v2': precompute_similar_products_v2
}


def sort_recommendation_algo(recset_group_ids):

    recommendations = PrecomputeQueue.objects.filter(
        id__in=recset_group_ids,
        algorithm__in=FUNC_MAP.keys(),
    )
    recsets = defaultdict(list)
    for recommendation in recommendations:
        recsets[recommendation.algorithm].append(recommendation)
    return recsets
