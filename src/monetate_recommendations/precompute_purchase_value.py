from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_purchase_value_algorithm')

ONLINE_PURCHASE_VALUE = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback}_{market_id}_{retailer_scope}_online AS
/* Merchandiser: {algorithm}, account {account_id}, {lookback} day,_{market_id} market, {retailer_scope} retailer_scope facts */
WITH purchase_line_value AS (
  SELECT
      f.account_id,
      f.fact_time,
      f.mid_epoch,
      f.mid_ts,
      f.mid_rnd,
      f.product_id,
      f.quantity * f.currency_unit_price * ex.rate account_value
  FROM m_dedup_purchase_line f
  JOIN config_account a
      ON a.account_id = f.account_id
  JOIN exchange_rate ex
      ON ex.effective_date::date = f.fact_time::date
      AND ex.from_currency_code = f.currency
      AND ex.to_currency_code = a.currency
  WHERE f.product_id is NOT NULL
      AND f.fact_time >= :begin_fact_time
      AND f.fact_time < :end_fact_time
      AND a.account_id IN (:account_ids)
)
SELECT
    s.account_id,
    f.product_id,
    COALESCE(s.country_code, '') country_code,
    COALESCE(s.region, '') region,
    SUM(f.account_value) as subtotal
FROM m_session_first_geo s
JOIN purchase_line_value f
    ON f.account_id = s.account_id
    AND f.fact_time BETWEEN s.start_time and s.end_time
    AND f.mid_ts = s.mid_ts
    AND f.mid_rnd = s.mid_rnd
WHERE 
    s.start_time >= :begin_session_time
    AND s.start_time < :end_session_time
    AND s.account_id IN (:account_ids)
GROUP BY 1, 2, 3, 4;
"""

OFFLINE_PURCHASE_VALUE = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_offline AS
    SELECT
        p1.account_id,
        '' as country_code,
        '' as region,
        p1.product_id,
        SUM(p1.quantity * p1.currency_unit_price * ex.rate) as subtotal
    FROM 
        scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    JOIN config_account ca
        ON ca.account_id = p1.account_id
    JOIN exchange_rate ex
        ON ex.effective_date::date = p1.fact_time::date
        AND ex.from_currency_code = p1.currency
        AND ex.to_currency_code = ca.currency
    WHERE
        p1.fact_time >= :begin_fact_time
        AND p1.fact_time < :end_fact_time
        AND p1.account_id = :account_id
    GROUP BY 1, 2, 3, 4
"""

ONLINE_OFFLINE_PURCHASE_VALUE = """
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
        '' as country_code,
        '' as region,
        subtotal
    FROM scratch.{algorithm}_{account_id}_{lookback_days}_{market_id}_{retailer_scope}_offline
)
GROUP BY 1, 2, 3, 4
"""

def precompute_purchase_value_algorithm(recsets):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_purchase_value_algorithm'), contextlib.closing(engine.connect()) as \
            warehouse_conn:
        warehouse_conn.execute("use warehouse {}".format(
            getattr(settings, 'RECS_QUERY_WH', os.environ.get('RECS_QUERY_WH', 'QUERY2_WH'))))
        for recset in recsets:
            if recset and recset.algorithm == 'purchase_value':
                log.log_info('processing recset {}'.format(recset.id))
                result_counts.append(precompute_utils.process_noncollab_algorithm(warehouse_conn, recset,
                                                                                  ONLINE_PURCHASE_VALUE,
                                                                                  OFFLINE_PURCHASE_VALUE,
                                                                                  ONLINE_OFFLINE_PURCHASE_VALUE))
    log.log_info('ending precompute_purchase_value_algorithm process')
    return result_counts
