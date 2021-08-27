from collections import defaultdict
from monetate_recommendations.precompute_purchase_also_purchase import precompute_purchase_also_purchase_algorithm
from monetate_recommendations.precompute_view_also_view import precompute_view_also_view_algorithm
from models import PrecomputeCollab
#from monetate.recs.models import PrecomputeCollab
# todo remove above once rebuilt
FUNC_MAP = {
    'purchase_also_purchase': precompute_purchase_also_purchase_algorithm,
    'view_also_view': precompute_view_also_view_algorithm
}


def sort_recommendation_algo(collab_recommendations_ids):

    recommendations = PrecomputeCollab.objects.filter(
        id__in=collab_recommendations_ids,
        algorithm__in=FUNC_MAP.keys(),
    )
    recsets = defaultdict(list)
    for recommendation in recommendations:
        recsets[recommendation.algorithm].append(recommendation)
    return recsets
