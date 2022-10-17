from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_trending_algorithm')

ONLINE_TRENDING = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_7_{market_id}_{retailer_scope}_online AS
WITH purchase_line_30 as (
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
    AND fpl.fact_time >= :begin_30_day_fact_time
    AND fpl.fact_time < :end_30_day_fact_time
    AND fpl.product_id is NOT NULL
WHERE s.start_time >= :begin_30_day_session_time
    AND s.start_time < :end_30_day_session_time
    AND s.account_id IN (:account_ids)
GROUP BY 1, 2, 3, 4),
purchase_line_7 as (
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
    AND fpl.fact_time >= :begin_7_day_fact_time
    AND fpl.fact_time < :end_7_day_fact_time
    AND fpl.product_id is NOT NULL
WHERE s.start_time >= :begin_7_day_session_time
    AND s.start_time < :end_7_day_session_time
    AND s.account_id IN (:account_ids)
GROUP BY 1, 2, 3, 4
HAVING sum(fpl.quantity) >= 5
)
SELECT pl30.account_id,
    pl30.product_id,
    pl30.country_code,
    pl30.region,
    pl7.subtotal/pl30.subtotal as subtotal
FROM purchase_line_30 as pl30 
JOIN purchase_line_7 as pl7
ON pl30.product_id = pl7.product_id
AND pl30.country_code = pl7.country_code
AND pl30.region = pl7.region
"""

OFFLINE_TRENDING = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_offline AS
WITH purchase_line_30 as (
SELECT
    p1.account_id,
    '' as country_code,
    '' as region,
    p1.product_id,
    SUM(p2.quantity) as subtotal
FROM scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
JOIN dio_purchase p2
    ON p1.customer_id = p2.customer_id
    AND p1.fact_time >= :begin_30_day_session_time
    AND p1.fact_time < :end_30_day_session_time
WHERE
    p1.account_id = :account_id
GROUP BY 1, 2, 3, 4),
purchase_line_7 as (
SELECT
    p1.account_id,
    '' as country_code,
    '' as region,
    p1.product_id,
    SUM(p2.quantity) as subtotal
FROM scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
JOIN dio_purchase p2
    ON p1.customer_id = p2.customer_id
    AND p1.fact_time >= :begin_7_day_session_time
    AND p1.fact_time < :end_7_day_session_time
WHERE 
    p1.account_id = :account_id
GROUP BY 1, 2, 3, 4)
SELECT pl30.account_id,
    pl30.country_code,
    pl30.region,
    pl30.product_id,
    pl7.subtotal/pl30.subtotal as subtotal
FROM purchase_line_30 as pl30 
JOIN purchase_line_7 as pl7
ON pl30.product_id = pl7.product_id
"""

ONLINE_OFFLINE_TRENDING = """
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
            sum(subtotal) as subtotal
        FROM scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_online
        GROUP BY 1, 2, 3, 4
        UNION ALL
        SELECT
            account_id,
            product_id,
            country_code,
            region,
            sum(subtotal) as subtotal
        FROM scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_offline
        GROUP BY 1, 2, 3, 4
    )
    GROUP BY 1, 2, 3, 4
"""

def precompute_trending_algorithm(recsets):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_trending_algorithm'), contextlib.closing(engine.connect()) as \
            warehouse_conn:
        warehouse_conn.execute("use warehouse {}".format(
            getattr(settings, 'RECS_QUERY_WH', os.environ.get('RECS_QUERY_WH', 'QUERY2_WH'))))
        for recset in recsets:
            if recset and recset.algorithm == 'trending':
                log.log_info('processing recset {}'.format(recset.id))

                result_counts.append(precompute_utils.process_noncollab_algorithm(warehouse_conn, recset,
                                                                                  ONLINE_TRENDING,
                                                                                  OFFLINE_TRENDING,
                                                                                  ONLINE_OFFLINE_TRENDING))
    log.log_info('ending precompute_trending_algorithm process')
    return result_counts
