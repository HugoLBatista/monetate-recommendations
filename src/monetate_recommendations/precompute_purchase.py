from django.conf import settings
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_purchase_algorithm')

BESTSELLERS_LOOKBACK = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback} AS
/* Merchandiser: {algorithm}, account {account_id}, {lookback} day  */
SELECT
    s.account_id,
    fpl.product_id,
    COALESCE(s.country_code, '') country_code,
    COALESCE(s.region, '') region,
    SUM(fpl.quantity) as subtotal
FROM m_session_first_geo s
JOIN m_dedup_purchase_line fpl
    ON fpl.account_id = :account_id
    AND fpl.fact_time BETWEEN s.start_time and s.end_time
    AND fpl.mid_ts = s.mid_ts
    AND fpl.mid_rnd = s.mid_rnd
    AND fpl.fact_time >= :begin_fact_time
    AND fpl.fact_time < :end_fact_time
    AND fpl.product_id is NOT NULL
WHERE s.start_time >= :begin_session_time
    AND s.start_time < :end_session_time
    AND s.account_id = :account_id
GROUP BY 1, 2, 3, 4;
"""


def precompute_purchase_algorithm(recsets):
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_purchase_algorithm'), contextlib.closing(engine.connect()) as warehouse_conn:
        for recset in recsets:
            if recset and recset.algorithm == 'purchase':
                log.log_info('processing recset {}'.format(recset.id))
                precompute_utils.process_noncollab_algorithm(warehouse_conn, recset, BESTSELLERS_LOOKBACK)
    log.log_info('ending precompute_purchase_algorithm process')
