from django.conf import settings
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils
from monetate_recommendations import product_type_filter_expression
import monetate.dio.models as dio_models

log.configure_script_log('precompute_view_algorithm')

MOSTVIEWED_LOOKBACK = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback} AS
/* Recs metrics: {algorithm}, account {account_id}, {lookback} day  */
SELECT
  fpv.product_id,
  COALESCE(s.country_code, '') country_code,
  COALESCE(s.region, '') region,
  COUNT(*) subtotal
FROM m_session_first_geo s
JOIN fact_product_view fpv
  ON fpv.account_id = :account_id
  AND fpv.fact_time BETWEEN s.start_time and s.end_time
  AND fpv.mid_ts = s.mid_ts
  AND fpv.mid_rnd = s.mid_rnd
  AND fpv.fact_time >= :begin_fact_time
  AND fpv.fact_time < :end_fact_time
WHERE s.account_id = :account_id
  AND s.start_time >= :begin_session_time
  AND s.start_time < :end_session_time
GROUP BY 1, 2, 3;
"""


def precompute_view_algorithm(recsets):
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_view_algorithm'), contextlib.closing(engine.connect()) as warehouse_conn:
        for recset in recsets:
            if recset and recset.algorithm == 'view':
                log.log_info('processing recset {}'.format(recset.id))
                catalog_id = recset.product_catalog.id if recset.product_catalog else \
                    dio_models.DefaultAccountCatalog.objects.get(account=recset.account.id).schema.id
                product_type_filter = precompute_utils.parse_product_type_filter(recset.filter_json)
                dataset_hash = precompute_utils.get_filter_hash(recset.filter_json)
                filter_variables, filter_query = product_type_filter_expression.get_query_and_variables(
                    product_type_filter)
                precompute_utils.create_metric_table(warehouse_conn, recset.account.id, recset.lookback_days, text(
                    MOSTVIEWED_LOOKBACK.format(algorithm=recset.algorithm, account_id=recset.account.id,
                                               lookback=recset.lookback_days)))
                query = precompute_utils.SKU_RANKS_BY_REGION_FOR_ACCOUNT_ID.format(algorithm=recset.algorithm,
                                                                                   account_id=recset.account.id,
                                                                                   lookback=recset.lookback_days,
                                                                                   filter_query=filter_query)
                unload_path, manifest_path = precompute_utils.create_unload_target_paths(recset.id)
                warehouse_conn.execute(
                    text(precompute_utils.SNOWFLAKE_UNLOAD.format(query=query)),
                    target=unload_path,
                    dataset_hash=dataset_hash,
                    catalog_id=catalog_id,
                    **filter_variables
                )
                precompute_utils.unload_manifest(conn=warehouse_conn, source=unload_path, target=manifest_path)
    log.log_info('ending precompute_view_algorithm process')
