from django.conf import settings
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_trending_algorithm')

TRENDING_PRODUCTS = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_7 AS
WITH purchase_line as (
SELECT 
    pl30.account_id,
    pl30.mid_epoch,
    pl30.mid_ts,
    pl30.mid_rnd,
    pl30.product_id,
    pl7.quantity/pl30.quantity as subtotal
FROM m_dedup_purchase_line pl30, m_dedup_purchase_line pl7
WHERE pl30.account_id IN (:account_ids)
    AND pl30.fact_time >= :begin_trending_fact_time
    AND pl30.fact_time < :end_trending_fact_time
    AND pl7.fact_time >= :begin_fact_time
    AND pl7.fact_time < :end_fact_time
    AND pl30.product_id is NOT NULL
    AND pl30.product_id = pl7.product_id
GROUP BY 1,2,3,4,5
HAVING sum(pl7.quantity) > 5
  )
  
  SELECT 
    s.account_id,
    pl.product_id,
    COALESCE(s.country_code, '') country_code,
    COALESCE (s.region, '') region,
    pl.rank
FROM m_session_first_geo s
JOIN purchase_line pl
    ON pl.account_id = :account_id
    AND f.fact_time BETWEEN s.start_time and s.end_time
    AND f.mid_ts = s.mid_ts
    AND f.mid_rnd = s.mid_rnd
    WHERE s.start_time >= :begin_session_time
    AND s.start_time < :end_session_time
    AND s.account_id IN (:account_ids)
GROUP BY 1,2,3,4,5
    
    
"""

def precompute_trending_algorithm(recsets):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_LOAD_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_trending_algorithm'), contextlib.closing(engine.connect()) as \
            warehouse_conn:
        for recset in recsets:
            if recset and recset.algorithm == 'trending':
                log.log_info('processing recset {}'.format(recset.id))

                result_counts.append(precompute_utils.process_noncollab_algorithm(warehouse_conn, recset,
                                                                                  TRENDING_PRODUCTS))
    log.log_info('ending precompute_trending_algorithm process')
    return result_counts
