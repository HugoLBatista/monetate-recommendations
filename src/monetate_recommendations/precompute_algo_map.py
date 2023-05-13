from collections import defaultdict
from monetate.recs.models import RecommendationSet

from .precompute_purchase import precompute_purchase_algorithm
from .precompute_purchase_value import precompute_purchase_value_algorithm
from .precompute_trending import precompute_trending_algorithm
from .precompute_view import precompute_view_algorithm

FUNC_MAP = {
    'view': precompute_view_algorithm,
    'purchase': precompute_purchase_algorithm,
    'trending': precompute_trending_algorithm,
    'purchase_value': precompute_purchase_value_algorithm,
    'most_popular': precompute_view_algorithm,
}


def sort_recsets_by_algorithm(recset_ids):
    recsets = RecommendationSet.objects.filter(
        id__in=recset_ids,
        algorithm__in=FUNC_MAP.keys(),
        archived=False,
    )
    recsets_by_algorithm = defaultdict(list)
    for recset in recsets:
        recsets_by_algorithm[recset.algorithm].append(recset)
    return recsets_by_algorithm
