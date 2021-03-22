from django.conf import settings
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_view_algorithm')

MOSTVIEWED_LOOKBACK = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback}_{market_id}_{retailer_scope} AS
/* Recs metrics: {algorithm}, account {account_id}, {lookback} day,_{market_id} market, {retailer_scope} retailer_scope facts */
SELECT
  fpv.product_id,
  COALESCE(s.country_code, '') country_code,
  COALESCE(s.region, '') region,
  COUNT(*) subtotal
FROM m_session_first_geo s
JOIN fact_product_view fpv
  ON fpv.account_id = s.account_id
  AND fpv.fact_time BETWEEN s.start_time and s.end_time
  AND fpv.mid_ts = s.mid_ts
  AND fpv.mid_rnd = s.mid_rnd
  AND fpv.fact_time >= :begin_fact_time
  AND fpv.fact_time < :end_fact_time
WHERE s.account_id IN (:account_ids)
  AND s.start_time >= :begin_session_time
  AND s.start_time < :end_session_time
GROUP BY 1, 2, 3;
"""


def precompute_view_algorithm(recsets):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_LOAD_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_view_algorithm'), contextlib.closing(engine.connect()) as warehouse_conn:
        for recset in recsets:
            if recset and recset.algorithm in ['view', 'most_popular']:
                log.log_info('processing recset {}'.format(recset.id))
                result_counts.append(precompute_utils.process_noncollab_algorithm(warehouse_conn, recset,
                                                                                  MOSTVIEWED_LOOKBACK))
    log.log_info('ending precompute_view_algorithm process')
    return result_counts
