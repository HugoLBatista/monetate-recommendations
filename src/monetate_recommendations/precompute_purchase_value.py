from django.conf import settings
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils
from monetate_recommendations import product_type_filter_expression
import monetate.dio.models as dio_models

log.configure_script_log('precompute_purchase_value_algorithm')

TOPREVENUE_LOOKBACK = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{lookback} AS
/* Merchandiser: {algorithm}, account {account_id}, {lookback} day  */
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
      ON a.account_id = :account_id
  JOIN exchange_rate ex
      ON ex.effective_date::date = f.fact_time::date
      AND ex.from_currency_code = f.currency
      AND ex.to_currency_code = a.currency
  WHERE f.product_id is NOT NULL
      AND f.fact_time >= :begin_fact_time
      AND f.fact_time < :end_fact_time
)
SELECT
    s.account_id,
    f.product_id,
    COALESCE(s.country_code, '') country_code,
    COALESCE(s.region, '') region,
    SUM(f.account_value) as subtotal
FROM m_session_first_geo s
JOIN purchase_line_value f
    ON f.account_id = :account_id
    AND f.fact_time BETWEEN s.start_time and s.end_time
    AND f.mid_ts = s.mid_ts
    AND f.mid_rnd = s.mid_rnd
WHERE s.start_time >= :begin_session_time
    AND s.start_time < :end_session_time
GROUP BY 1, 2, 3, 4;
"""


def precompute_purchase_value_algorithm(recsets):
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_purchase_value_algorithm'), contextlib.closing(engine.connect()) as \
            warehouse_conn:
        for recset in recsets:
            if recset and recset.algorithm == 'purchase_value':
                log.log_info('processing recset {}'.format(recset.id))
                catalog_id = recset.product_catalog.id if recset.product_catalog else \
                    dio_models.DefaultAccountCatalog.objects.get(account=recset.account.id).schema.id
                product_type_filter = precompute_utils.parse_product_type_filter(recset.filter_json)
                dataset_hash = precompute_utils.get_filter_hash(recset.filter_json)
                filter_variables, filter_query = product_type_filter_expression.get_query_and_variables(
                    product_type_filter)
                precompute_utils.create_metric_table(warehouse_conn, recset.account.id, recset.lookback_days, text(
                    TOPREVENUE_LOOKBACK.format(algorithm=recset.algorithm, account_id=recset.account.id,
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
    log.log_info('ending precompute_purchase_value_algorithm process')
