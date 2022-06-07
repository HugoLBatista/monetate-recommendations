from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_PAP_algorithm')

VIEW_ALSO_VIEW = """
CREATE TEMPORARY TABLE scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
WITH
half_scores AS (
    /* Filter out pairs existing seen only on one device. */
    SELECT
        p1.account_id account_id,
        p1.product_id pid1,
        p2.product_id pid2,
        count(*) score 
    FROM scratch.earliest_view_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    JOIN scratch.earliest_view_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p2
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

"""


def precompute_view_also_view_algorithm(recsets_group):
    return precompute_utils.initialize_process_collab_algorithm(recsets_group, 'view_also_view',
                                                                VIEW_ALSO_VIEW,
                                                                precompute_utils.GET_EARLIEST_VIEW_PER_MID_AND_PID)
