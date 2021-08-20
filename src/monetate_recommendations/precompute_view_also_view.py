from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_PAP_algorithm')

VIEW_ALSO_VIEW = """
CREATE TEMPORARY TABLE scratch.{algorithm}_{account_id}_{lookback}_{market_id}_{retailer_id} AS
WITH
half_scores AS (
    /* Filter out pairs existing seen only on one device. */
    SELECT
        p1.account_id account_id,
        p1.product_id pid1,
        p2.product_id pid2,
        count(*) score  /* TODO: cosine affinity N(p1p2) / N(p1)N(p2) */
    FROM scratch.filtered_device_earliest_product_view p1
    JOIN scratch.filtered_device_earliest_product_view p2
        ON p1.account_id = p2.account_id
        AND p1.mid_epoch = p2.mid_epoch
        AND p1.mid_ts = p2.mid_ts
        AND p1.mid_rnd = p2.mid_rnd
        AND p1.product_id < p2.product_id  /* lower triangle */
    GROUP BY 1, 2, 3
    HAVING count(*) > :minimum_count
)
    /* create symmetric pairs */
    /* verify if performance benefit on snowflake */
    SELECT
        account_id,
        pid1,
        pid2,
        score
    FROM half_scores
    UNION ALL
    SELECT
        account_id,
        pid2 AS pid1,
        pid1 AS pid2,
        score
    FROM half_scores
)

"""


def precompute_view_also_view_algorithm(recommendations):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_PAP_algorithm'), contextlib.closing(engine.connect()) as warehouse_conn:
        warehouse_conn.execute("use warehouse {}".format(
            getattr(settings, 'RECS_QUERY_WH', os.environ.get('RECS_QUERY_WH', 'QUERY2_WH'))))
        for recommendation in recommendations:
            if recommendation and recommendation.algorithm == 'purchase_also_purchase':
                log.log_info('processing recset {}'.format(recommendation.id))
                result_counts.append(
                    precompute_utils.process_collab_algorithm(warehouse_conn, recommendation, VIEW_ALSO_VIEW,
                                                              precompute_utils.GET_LATEST_VIEWS_PER_MID))
    log.log_info('ending precompute_PAP_algorithm process')
    return result_counts
