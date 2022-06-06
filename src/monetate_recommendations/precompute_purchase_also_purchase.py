from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_PAP_algorithm')
#TODO: update the join on product catalog, we are multiplying our counts with the skus
PURCHASE_ALSO_PURCHASE = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
    SELECT
        p1.account_id account_id,
        p1.product_id pid1,
        p2.product_id pid2,
        count(*) score
    FROM scratch.last_purchase_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    JOIN scratch.last_purchase_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p2
        ON p1.account_id = p2.account_id
        AND p1.mid_epoch = p2.mid_epoch
        AND p1.mid_ts = p2.mid_ts
        AND p1.mid_rnd = p2.mid_rnd
        AND p1.product_id != p2.product_id
    GROUP BY 1, 2, 3
    HAVING count(*) >= :minimum_count
"""


def precompute_purchase_also_purchase_algorithm(recsets_group):

    return precompute_utils.initialize_process_collab_algorithm(recsets_group, 'purchase_also_purchase',
                                                                PURCHASE_ALSO_PURCHASE,
                                                                precompute_utils.GET_LAST_PURCHASE_PER_MID_AND_PID)

