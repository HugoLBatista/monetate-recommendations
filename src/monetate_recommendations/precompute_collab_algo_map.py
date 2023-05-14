import contextlib
import os
from collections import defaultdict
from django.conf import settings
from monetate.common import job_timing
from monetate.recs.models import PrecomputeQueue
from monetate_monitoring import log
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from .precompute_catalog_associated_pids import process_catalog_collab_algorithm
from .precompute_purchase_associated_pids import process_purchase_collab_algorithm
from .precompute_view_associated_pids import process_view_collab_algorithm

FUNC_MAP = {
    'purchase_also_purchase': process_purchase_collab_algorithm,
    'view_also_view': process_view_collab_algorithm,
    'similar_products_v2': process_catalog_collab_algorithm,
    'bought_together': process_purchase_collab_algorithm,
    'subsequently_purchased': process_purchase_collab_algorithm,
}

def initialize_collab_algorithm(queue_entries, algorithm):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_{}_algorithm'.format(algorithm)),\
            contextlib.closing(engine.connect()) as warehouse_conn:
        warehouse_conn.execute("use warehouse {}".format(
            getattr(settings, 'RECS_COLLAB_QUERY_WH', os.environ.get('RECS_COLLAB_QUERY_WH', 'QUERY4_WH'))))
        for queue_entry in queue_entries:
            if queue_entry and queue_entry.algorithm == algorithm:
                log.log_info('processing queue entry {}'.format(queue_entry.id))
                result_counts.append(
                    FUNC_MAP[algorithm](warehouse_conn, queue_entry))
    log.log_info('ending precompute_{}_algorithm process'.format(algorithm))
    return result_counts


def sort_recommendation_algo(queue_entry_ids):

    queue_entries = PrecomputeQueue.objects.filter(
        id__in=queue_entry_ids,
        algorithm__in=FUNC_MAP.keys(),
    )
    queues_group_by_algo = defaultdict(list)
    for queue_entry in queue_entries:
        queues_group_by_algo[queue_entry.algorithm].append(queue_entry)
    return queues_group_by_algo
