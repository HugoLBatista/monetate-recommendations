from django.conf import settings
import os
import datetime
import json
import binascii
import bisect
from copy import deepcopy
from sqlalchemy.sql import text
from monetate.common import log
from monetate.common.row import get_single_value_query
from monetate.common.warehouse import sqlalchemy_warehouse
from monetate.common.sqlalchemy_session import CLUSTER_MAX
import monetate.retailer.models as retailer_models
import monetate.dio.models as dio_models
from monetate.recs.models import AccountRecommendationSetting
from monetate_recommendations import supported_prefilter_expression
from supported_prefilter_expression import SUPPORTED_PREFILTER_FIELDS, FILTER_MAP

DATA_JURISDICTION = 'recs_global'
SESSION_SHARDS = 8

GEO_TARGET_COLUMNS = {
    'country': ["country_code"],
    'region': ["country_code", "region"]
}

SNOWFLAKE_UNLOAD = """
COPY
INTO :target
FROM (
    SELECT object_construct(
        'shard_key', :shard_key,
        'document', object_construct(
            'pushdown_filter_hash', sha1(LOWER(CONCAT('product_type=', {dynamic_product_type} {geo_hash_sql}))),
            'data', (
                array_agg(object_construct(*))
                WITHIN GROUP (ORDER BY rank ASC)
            )
        ),
        'sent_time', :sent_time,
        'account', object_construct(
            'id', :account_id
        ),
        'schema', object_construct(
            'feed_type', 'RECSET_NONCOLLAB_RECS',
            'id', :recset_id
        )
    )
    FROM scratch.recset_{account_id}_{recset_id}_ranks
    {group_by}
)
FILE_FORMAT = (TYPE = JSON, compression='gzip')
SINGLE=TRUE
MAX_FILE_SIZE=1000000000
"""


DYNAMIC_FILTER_RANKS = """
SELECT filtered_scored_records.*, 
    TRIM(split_product_type.value::string, ' ') as split_product_type,
    ROW_NUMBER() OVER ({partition_by} ORDER BY score DESC, id) as rank
FROM filtered_scored_records,
LATERAL FLATTEN(input=>ARRAY_APPEND(SPLIT(product_type, ','), '')) split_product_type
"""


STATIC_FILTER_RANKS = """
SELECT filtered_scored_records.*,
    ROW_NUMBER() OVER ({partition_by} ORDER BY score DESC, id) as rank
FROM filtered_scored_records
"""

RESULT_COUNT = """
SELECT COUNT(*) 
FROM scratch.recset_{account_id}_{recset_id}_ranks
"""

# SKU ranking query used by view, purchase, and purchase_value algorithms
SKU_RANKS_BY_RECSET = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.recset_{account_id}_{recset_id}_ranks AS
WITH
pid_algo_raw AS (
    /* Aggregates per product_id for an account at appropriate geo_rollup level */
    SELECT
        product_id,
        SUM(subtotal) AS score
        {geo_columns}
    FROM scratch.{algorithm}_{account_id}_{lookback}_{market_id}_{retailer_scope}
    GROUP BY product_id
    {geo_columns}
),
pid_max_score AS (SELECT MAX(score) as max_score FROM pid_algo_raw),
pid_algo AS (
    SELECT product_id, ceil((score / max_score) * 1000, 2) AS score {geo_columns}
    FROM pid_algo_raw, pid_max_score
    GROUP BY product_id, max_score, score {geo_columns}
),
reduced_catalog AS (
    /*
        Reduce catalog to representative visually distinct items by (image link, color) per item group
        Limit to at most 50 representative items per item group for later post filtering.
    */
    SELECT
        item_group_id,
        id
    FROM (
        SELECT
            item_group_id,
            id,
            ROW_NUMBER() OVER (PARTITION by item_group_id ORDER BY id DESC) AS ordinal
        FROM (
            SELECT
                c.item_group_id,
                c.image_link,
                c.color,
                /* Flatten, trim extra spaces, and convert back to string for filtering */
                array_to_string(array_agg(TRIM(split_product_type.value::string, ' ')), ',') as product_type,
                MAX(c.id) AS id
            FROM product_catalog c
            JOIN config_dataset_data_expiration e
                ON c.dataset_id = e.dataset_id,
            LATERAL FLATTEN(input=>split(c.product_type, ',')) split_product_type
            WHERE c.dataset_id = :catalog_id
                AND c.retailer_id = :retailer_id
                AND c.update_time >= e.cutoff_time
                {early_filter}
            GROUP BY 1, 2, 3
        )
        {late_filter}
    )
    WHERE ordinal <= 50
),
sku_algo AS (
    /* Explode recommended product ids into recommended representative skus */
    SELECT
        c.id,
        pid_algo.score
        {geo_columns}
    FROM pid_algo
    JOIN reduced_catalog c
    ON c.item_group_id = pid_algo.product_id
),
filtered_scored_records AS (
    SELECT pc.*, sa.score
      {geo_columns}
    FROM product_catalog as pc
    JOIN sku_algo as sa
        ON pc.id = sa.id
    WHERE pc.retailer_id = :retailer_id
        AND pc.dataset_id = :catalog_id
), ranked_records AS (
    {rank_query}
)
SELECT *
FROM ranked_records
WHERE rank <= 1000
"""


def parse_supported_filters(filter_json):
    def _filter_product_type(f):
        return f['left']['field'] == 'product_type'

    def _filter_not_product_type(f):
        return f['left']['field'] != 'product_type'

    def _filter_dynamic(f):
        return f['right']['type'] != 'function'

    def _filter_supported_static_filter(f):
        return f['left']['field'].lower() in SUPPORTED_PREFILTER_FIELDS and f['right']['type'] != 'function'

    # a filter is supported if:
    #   1. it is in the list of supported prefilter fields AND
    #   2. the filter type (equal to, starts with, etc) is supported AND
    #   3. it is a product_type field OR it is a constant (not a function = constant)
    def _filter_supported_filters(f):
        return (
                f['left']['field'].lower() in SUPPORTED_PREFILTER_FIELDS
        ) and (
                f['type'] in FILTER_MAP
        ) and (
                f['right']['type'] != 'function' or f['left']['field'] == 'product_type'
        )

    filter_dict = json.loads(filter_json)
    supported_filters = filter(_filter_supported_filters, filter_dict['filters'])
    product_type_filters = filter(_filter_product_type, supported_filters)
    static_product_type_filters = filter(_filter_dynamic, product_type_filters)
    static_supported_filters = filter(_filter_supported_static_filter, supported_filters)
    static_supported_filters_non_product_type = filter(_filter_not_product_type, static_supported_filters)

    # when the expression is an "or" expression, we can only prefilter if all fields are supported for pre-filtering
    # AND there is not a mixture of filters on product_type plus other fields. product_type filtering is performed in
    # one are of the SQL while the filters on other fields are performed in a different area, so we can't 'or' across.
    has_product_and_non_product = product_type_filters and static_supported_filters_non_product_type
    has_unsupported_filters = len(supported_filters) != len(filter_dict['filters'])
    if filter_dict["type"] == "or" and (has_product_and_non_product or has_unsupported_filters):
        filter_dict['filters'] = []
        return filter_dict, filter_dict, False
    # 'or' filters can't be pushed down to the DB. Take for example:
    # products: [{product_type: a, brand: b}, {product_type: c, brand: d}] with filter on (product_type=a OR brand=d)
    # if we push down product_type=a, then we end up with only {product_type: a, brand: b} item when both actually match
    has_dynamic_product_type_filter = filter_dict['type'] == 'and' and \
                                      len(static_product_type_filters) != len(product_type_filters)
    # the early filter expression is on non-product_type static values
    early_filter_expr = deepcopy(filter_dict)
    early_filter_expr['filters'] = static_supported_filters_non_product_type
    # the late filter expression if for product_type static values
    late_filter_expr = deepcopy(filter_dict)
    late_filter_expr['filters'] = static_product_type_filters
    return early_filter_expr, late_filter_expr, has_dynamic_product_type_filter


def create_metric_table(conn, account_ids, lookback, algorithm, query):

    begin_fact_time = datetime.datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=lookback)
    end_fact_time = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    begin_session_time, end_session_time = sqlalchemy_warehouse.get_session_time_bounds(
        begin_fact_time, end_fact_time)

    if algorithm == 'trending':
        begin_30_day_fact_time = datetime.datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=30)
        end_30_day_fact_time = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        begin_30_day_session_time, end_30_day_session_time = sqlalchemy_warehouse.get_session_time_bounds(
            begin_30_day_fact_time, end_30_day_fact_time
        )
        conn.execute(query, account_ids=account_ids, begin_7_day_fact_time=begin_fact_time,
                     end_7_day_fact_time=end_fact_time, begin_7_day_session_time=begin_session_time,
                     end_7_day_session_time=end_session_time, begin_30_day_fact_time=begin_30_day_fact_time,
                     end_30_day_fact_time=end_30_day_fact_time, begin_30_day_session_time=begin_30_day_session_time,
                     end_30_day_session_time=end_30_day_session_time)

    else:
        conn.execute(query, account_ids=account_ids, begin_fact_time=begin_fact_time, end_fact_time=end_fact_time,
                     begin_session_time=begin_session_time, end_session_time=end_session_time)


def get_shard_key(account_id):
    return binascii.crc32(str(account_id)) % CLUSTER_MAX


def get_shard_range(shard_key):
    """
    See: monetate-io/fileimport/file_importer.py ShardRange
    """
    min_shard = 0
    max_shard = CLUSTER_MAX
    n_shards = SESSION_SHARDS
    shard_size = float(max_shard - min_shard) / n_shards
    shard_boundaries = tuple(int(round(min_shard + i * shard_size)) for i in range(n_shards)) + (max_shard,)
    shard_key = int(shard_key)
    if not min_shard <= shard_key < max_shard:
        raise ValueError('shard_key {} not within [{}, {}]'.format(shard_key, min_shard, max_shard))
    hi_idx = bisect.bisect_right(shard_boundaries, shard_key)
    return shard_boundaries[hi_idx - 1], shard_boundaries[hi_idx]


def get_recset_account_ids(recset):
    """
    Return only accounts that have the precompute feature flag.
    """
    precompute_feature = retailer_models.ACCOUNT_FEATURES.ENABLE_NONCOLLAB_RECS_PRECOMPUTE
    account_ids = []
    if recset.account and recset.account.has_feature(precompute_feature):
        account_ids.append(recset.account.id)
    elif recset.is_retailer_tenanted:
        accounts = retailer_models.Account.objects.filter(retailer_id=recset.retailer.id,
                                                          archived=False,
                                                          accountfeature__feature_flag__name=precompute_feature)
        account_ids = [account.id for account in accounts if len(dio_models.DefaultAccountCatalog.objects.filter(
            account=account.id))]
    return account_ids


def get_unload_sql(geo_target, has_dynamic_filter):
    """
    gets the SQL snippets for geo partitioning of precompute non-contextual models as well as sql snippets for
    dynamic product type filters. If a geo_target or dynamic filter is not specified, then all snippets will simply
    be an empty string.
     example return:
    {
        'geo_columns': ",country_code,region",
        'geo_hash_sql': ",'/country_code=',IFNULL(country_code,''),'/region=',IFNULL(region,'')",
        'dynamic_product_type': "split_product_type",
        'group_by': "GROUP BY country_code,region,split_product_type",
        'rank_query': '''
            SELECT filtered_scored_records.*,
                TRIM(split_product_type.value::string, ' ') as split_product_type,
                ROW_NUMBER() OVER (PARTITION BY country_code,region,split_product_type ORDER BY score DESC, id) as rank
            FROM filtered_scored_records,
            LATERAL FLATTEN(input=>ARRAY_APPEND(SPLIT(product_type, ','), '')) split_product_type
        '''
    }
    geo_hash_sql becomes one part of the push-down filter hash. each part of the filter is separated by a '/', which is
    the reason for the prepended slash before country_code and region.
    """
    geo_cols = GEO_TARGET_COLUMNS.get(geo_target, None)
    geo_str = ",".join(geo_cols) if geo_cols else ""
    dynamic_filter_delimiter = "," if geo_cols else ""
    dynamic_filter_str = dynamic_filter_delimiter + "split_product_type" if has_dynamic_filter else ""
    partition_by = "PARTITION BY " + geo_str + dynamic_filter_str if geo_cols or has_dynamic_filter else ""
    rank_query = DYNAMIC_FILTER_RANKS if has_dynamic_filter else STATIC_FILTER_RANKS
    return {
        'geo_columns': "," + ",".join(geo_cols) if geo_cols else "",
        'geo_hash_sql': "".join([",'/{}=',IFNULL({},'')".format(col, col) for col in geo_cols]) if geo_cols else "",
        'dynamic_product_type': "split_product_type" if has_dynamic_filter else "''",
        'group_by': "GROUP BY " + geo_str + dynamic_filter_str if geo_cols or has_dynamic_filter else "",
        'rank_query': rank_query.format(partition_by=partition_by)
    }


def create_unload_target_path(account_id, recset_id):
    stage = getattr(settings, 'SNOWFLAKE_DATAIO_STAGE', '@dataio_stage_v1')
    shard_key = get_shard_key(account_id)
    vshard_lower, vshard_upper = get_shard_range(shard_key)
    dt = datetime.datetime.utcnow()
    interval_duration = 1
    round_minute = dt.minute - dt.minute % interval_duration
    bucket_time = dt.replace(minute=round_minute, second=0, microsecond=0)
    path = '{data_jurisdiction}/{bucket_time:%Y/%m/%d}/{data_jurisdiction}-{bucket_time:%Y%m%dT%H%M%S.000Z}_PT' \
           '{interval_duration}M-{vshard_lower}-{vshard_upper}-precompute_{account_id}_{recset_id}.json.gz'\
        .format(data_jurisdiction=DATA_JURISDICTION,
                bucket_time=bucket_time,
                interval_duration=interval_duration,
                vshard_lower=vshard_lower,
                vshard_upper=vshard_upper,
                account_id=account_id,
                recset_id=recset_id)

    return os.path.join(stage, path), bucket_time


def get_account_ids_for_market_driven_recsets(recset, account_id):
    if recset.retailer_market_scope is True:
        account_ids = [account.id for account in recset.retailer.account_set.all()]
        log.log_info('Retailer scoped recset, using {} accounts'.format(len(account_ids)))
        return account_ids
    if recset.market is not None:
        account_ids = [account.id for account in recset.market.accounts.all()]
        log.log_info('Market scoped recset, using {} accounts'.format(len(account_ids)))
        return account_ids
    else:
        log.log_info('Account scoped recset for account {}'.format(account_id))
        return [account_id]


def process_noncollab_algorithm(conn, recset, metric_table_query):
    """
    Example JSON shape unloaded to s3:
    {
        "account":
        {
            "id": 1
        },
        "document":
        {
            "data": [...records],
            "pushdown_filter_hash": "d27aaa6c61c21f9dc9e99ddceb7a7ac1ba1d6ad3"
        },
        "schema":
        {
            "feed_type": "RECSET_NONCOLLAB_RECS",
            'id': 1001
        },
        "sent_time": "2020-08-19 16:35:00",
        "shard_key": 847799
    }
    """
    result_counts = []

    for account_id in get_recset_account_ids(recset):
        log.log_info('Querying results for recset {}, account {}'.format(recset.id, account_id))
        recommendation_settings = AccountRecommendationSetting.objects.filter(account_id=account_id)
        if len(recommendation_settings) is 1:
            global_filter_json = recommendation_settings[0].filter_json
        else:
            log.log_debug("Account has no recommendation settings, using default of empty filter_json")
            global_filter_json = u'{"type": "or", "filters": []}'
        early_filter_exp, late_filter_exp, has_dynamic_filter = parse_supported_filters(recset.filter_json)
        global_early_filter_exp, global_late_filter_exp, global_has_dynamic_filter = \
            parse_supported_filters(global_filter_json)
        has_dynamic_filter = has_dynamic_filter or global_has_dynamic_filter
        early_filter_sql, late_filter_sql, filter_variables = supported_prefilter_expression.get_query_and_variables(
            early_filter_exp, late_filter_exp, global_early_filter_exp, global_late_filter_exp)
        catalog_id = recset.product_catalog.id if recset.product_catalog else \
            dio_models.DefaultAccountCatalog.objects.get(account=account_id).schema.id
        account_ids = get_account_ids_for_market_driven_recsets(recset, account_id)
        create_metric_table(conn, account_ids, recset.lookback_days, recset.algorithm,
                            text(metric_table_query.format(algorithm=recset.algorithm,
                                                           account_id=account_id,
                                                           lookback=recset.lookback_days,
                                                           market_id=recset.market.id if recset.market else None,
                                                           retailer_scope=recset.retailer_market_scope,
                                                           )))
        unload_path, send_time = create_unload_target_path(account_id, recset.id)
        unload_sql = get_unload_sql(recset.geo_target, has_dynamic_filter)

        conn.execute(text(SKU_RANKS_BY_RECSET.format(algorithm=recset.algorithm,
                                                     recset_id=recset.id,
                                                     account_id=account_id,
                                                     lookback=recset.lookback_days,
                                                     early_filter=early_filter_sql,
                                                     late_filter=late_filter_sql,
                                                     market_id=recset.market.id if recset.market else None,
                                                     retailer_scope=recset.retailer_market_scope,
                                                     **unload_sql)),
                     retailer_id=recset.retailer.id,
                     catalog_id=catalog_id,
                     **filter_variables)
        result_counts.append(get_single_value_query(conn.execute(text(RESULT_COUNT.format(recset_id=recset.id,
                                                                                          account_id=account_id,
                                                                                          **unload_sql))), 0))
        conn.execute(text(SNOWFLAKE_UNLOAD.format(recset_id=recset.id, account_id=account_id, **unload_sql)),
                     shard_key=get_shard_key(account_id),
                     account_id=account_id,
                     recset_id=recset.id,
                     sent_time=send_time,
                     target=unload_path)
    return result_counts
