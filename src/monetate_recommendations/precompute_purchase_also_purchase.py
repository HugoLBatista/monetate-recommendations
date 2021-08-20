from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_PAP_algorithm')

PURCHASE_ALSO_PURCHASE = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS

    SELECT
        p1.account_id account_id,
        p1.product_id pid1,
        p2.product_id pid2,
        count(*) score
    FROM scratch.get_latest_purchase_per_mid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    JOIN config_account ON (p1.account_id=config_account.account_id)
    JOIN product_catalog pc1 ON (p1.product_id=pc1.item_group_id
                             AND config_account.retailer_id=pc1.retailer_id)
    JOIN scratch.get_latest_purchase_per_mid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p2
        ON p1.account_id = p2.account_id
        AND p1.mid_epoch = p2.mid_epoch
        AND p1.mid_ts = p2.mid_ts
        AND p1.mid_rnd = p2.mid_rnd
        AND p1.product_id != p2.product_id
    JOIN product_catalog pc2 ON (p2.product_id=pc2.item_group_id
                             AND config_account.retailer_id=pc2.retailer_id)
  GROUP BY 1, 2, 3
    HAVING count(*) >= :minimum_count
"""


def precompute_purchase_also_purchase_algorithm(recommendations):
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
                    precompute_utils.process_collab_algorithm(warehouse_conn, recommendation, PURCHASE_ALSO_PURCHASE,
                                                              precompute_utils.GET_LATEST_PURCHASES_PER_MID))
    log.log_info('ending precompute_PAP_algorithm process')
    return result_counts
