from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate_monitoring import log
from monetate.common import job_timing
from monetate_recommendations import precompute_utils
log.configure_script_log('precompute_purchase_algorithm')

ONLINE_PURCHASE_QUERY = """ 
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback}_{market_id}_{retailer_scope}_online AS
/* Merchandiser: {algorithm}, account {account_id}, {lookback} day,_{market_id} market, {retailer_scope} retailer_scope facts */
SELECT
    s.account_id,
    fpl.product_id,
    COALESCE(s.country_code, '') country_code,
    COALESCE(s.region, '') region,
    SUM(fpl.quantity) as subtotal
FROM m_session_first_geo s
JOIN m_dedup_purchase_line fpl
    ON fpl.account_id = s.account_id
    AND fpl.fact_time BETWEEN s.start_time and s.end_time
    AND fpl.mid_ts = s.mid_ts
    AND fpl.mid_rnd = s.mid_rnd
    AND fpl.fact_time >= :begin_fact_time
    AND fpl.fact_time < :end_fact_time
    AND fpl.product_id is NOT NULL
WHERE s.start_time >= :begin_session_time
    AND s.start_time < :end_session_time
    AND s.account_id IN (:account_ids)
GROUP BY 1, 2, 3, 4;
"""

OFFLINE_PURCHASE_QUERY = """
    CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_offline AS
    SELECT
        p1.account_id as account_id,
        '' as country_code,
        '' as region,
        p1.product_id,
        SUM(p1.quantity) as subtotal
    FROM scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    WHERE
        p1.fact_time >= :begin_fact_time
        AND p1.fact_time < :end_fact_time
        AND p1.product_id is NOT NULL
    GROUP BY 1, 2, 3, 4
"""
# scratch.{algorithm}_{metric_table_account_id}_{lookback}_{market_id}_{retailer_scope}_{purchase_data_source}

ONLINE_OFFLINE_PURCHASE_QUERY = """
    CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_{purchase_data_source} AS
    SELECT
        account_id,
        product_id,
        country_code,
        region,
        sum(subtotal) as subtotal
    FROM (
        SELECT
            account_id,
            product_id,
            country_code,
            region,
            subtotal
        FROM scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_online
        UNION ALL
        SELECT
            account_id,
            product_id,
            country_code,
            region,
            subtotal
        FROM scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_offline
    )
    GROUP BY 1, 2, 3, 4
"""

def precompute_purchase_algorithm(recsets):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_purchase_algorithm'), contextlib.closing(engine.connect()) as warehouse_conn:
        warehouse_conn.execute("use warehouse {}".format(
            getattr(settings, 'RECS_QUERY_WH', os.environ.get('RECS_QUERY_WH', 'QUERY2_WH'))))
        for recset in recsets:
            if recset and recset.algorithm == 'purchase':
                log.log_info('processing recset {}'.format(recset.id))
                result_counts.append(precompute_utils.process_noncollab_algorithm(warehouse_conn, recset,
                                                                                  ONLINE_PURCHASE_QUERY,
                                                                                  OFFLINE_PURCHASE_QUERY,
                                                                                  ONLINE_OFFLINE_PURCHASE_QUERY))
    log.log_info('ending precompute_purchase_algorithm process')
    return result_counts
