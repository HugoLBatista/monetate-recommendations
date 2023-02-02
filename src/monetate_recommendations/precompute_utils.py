from django.conf import settings
from django.db.models import Q
import os
import datetime
import json
import binascii
import bisect
import six
from copy import deepcopy
from sqlalchemy.sql import text
from monetate.common import log, job_timing
from monetate.common.row import get_single_value_query
from monetate.common.warehouse import sqlalchemy_warehouse
from monetate.common.sqlalchemy_session import CLUSTER_MAX
import monetate.retailer.models as retailer_models
import monetate.dio.models as dio_models
from monetate.recs.models import RecommendationSet, RecommendationSetDataset, AccountRecommendationSetting
from monetate_recommendations import supported_prefilter_expression
from monetate_recommendations import supported_prefilter_expression_v2 as filters
from monetate_recommendations import supported_prefilter_expression_v3 as new_filters
from .precompute_constants import UNSUPPORTED_PREFILTER_FIELDS, SUPPORTED_DATA_TYPES, SUPPORTED_PREFILTER_FIELDS, DATA_TYPE_TO_SNOWFLAKE_TYPE
from .supported_prefilter_expression_v3 import FILTER_MAP
from monetate_recommendations import precompute_purchase_associated_pids
from .precompute_purchase_associated_pids import get_dataset_ids_for_pos, GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID

DATA_JURISDICTION = 'recs_global'
DATA_JURISDICTION_PID_PID = 'recs_global_pid_pid'
SESSION_SHARDS = 8
MIN_PURCHASE_THRESHOLD = 3
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
                array_agg(object_construct('id', id, 'normalized_score', score, 'rank', rank))
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

SNOWFLAKE_UNLOAD_2 = """
COPY
INTO :target
FROM (
    SELECT object_construct(
        'shard_key', :shard_key,
        'document', object_construct(
            'pushdown_filter_hash', sha1(LOWER(TO_JSON(object_construct({pushdown_filter_str})))),
            'lookup_key', '',
            'pushdown_filter_json', LOWER(TO_JSON(object_construct({pushdown_filter_str}))),
            'data', (
                array_agg(object_construct('id', id, 'normalized_score', score, 'rank', rank))
                WITHIN GROUP (ORDER BY rank ASC)
            )
        ),
        'sent_time', :sent_time,
        'account', object_construct(
            'id', :account_id
        ),
        'schema', object_construct(
            'feed_type', 'RECSET_RECS',
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

SNOWFLAKE_UNLOAD_COLLAB = """
COPY 
INTO :target
FROM (
    SELECT object_construct(
    'shard_key', :shard_key,
    'document', object_construct(
            'lookup_key', lookup_key,
            'data', (
                array_agg(object_construct('id', id,  'normalized_score', normalized_score,'rank', rank))
                WITHIN GROUP (ORDER BY rank ASC)
            )
        ),
        'sent_time', :sent_time,
        'account', object_construct(
            'id', :account_id
        ),
        'schema', object_construct(
            'feed_type', 'RECSET_COLLAB_RECS',
            'id', :recset_id
        )
    )
    FROM scratch.recset_{account_id}_{recset_id}_ranks
    GROUP BY lookup_key
)
FILE_FORMAT = (TYPE = JSON, compression='gzip')
SINGLE=TRUE
MAX_FILE_SIZE=1000000000
"""
SNOWFLAKE_UNLOAD_PID_PID = """
COPY 
INTO :target
FROM (
    SELECT object_construct(
    'document', object_construct(
            'lookup_key', lookup_key,
            'data', (
                array_agg(object_construct('product_id', product_id,  'normalized_score', normalized_score,'score', score))
                WITHIN GROUP (ORDER BY ordinal ASC)
            )
        ),
        'sent_time', :sent_time,
        'schema', object_construct(
            'feed_type', 'RECSET_COLLAB_RECS_PID',
            'account_id', :account_id,
            'market_id', :market_id,
            'retailer_id', :retailer_id,
            'algorithm', :algorithm,
            'lookback_days', :lookback_days
        )
    )
    FROM scratch.pid_ranks_{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}
    WHERE ordinal <= 10000
    GROUP BY lookup_key
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
LATERAL FLATTEN(input=>ARRAY_APPEND(parse_csv_string_udf(product_type), '')) split_product_type
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
    FROM scratch.{algorithm}_{metric_table_account_id}_{lookback}_{market_id}_{retailer_scope}_{purchase_data_source}
    GROUP BY product_id
    {geo_columns}
),
pid_max_score AS (SELECT MAX(score) as max_score FROM pid_algo_raw),
pid_algo AS (
    SELECT product_id, ceil((score / max_score) * 1000, 2) AS score {geo_columns}
    FROM pid_algo_raw, pid_max_score
    GROUP BY product_id, max_score, score {geo_columns}
),
latest_catalog AS (
 SELECT pc.* FROM product_catalog as pc
    JOIN config_dataset_data_expiration e
     ON pc.dataset_id = e.dataset_id
 WHERE pc.retailer_id=:retailer_id AND pc.dataset_id = :catalog_id
    AND pc.update_time >= e.cutoff_time
),
filtered_catalog AS (
SELECT *
FROM latest_catalog as lc
{early_filter}
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
            FROM filtered_catalog as c,
            LATERAL FLATTEN(input=>split(c.product_type, ',')) split_product_type
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
    SELECT pc.id, pc.product_type, sa.score
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

# account_id , market_id and retailer_id create a unique key only one variable will have a value and rest will be None
# example  6814_None_None
PID_RANKS_BY_COLLAB_RECSET = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.pid_ranks_{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days} 
AS WITH 
score_scaling AS (
        SELECT
        max(score) AS max_raw_score,
        min(score) AS min_raw_score,
        /* avoid division by 0 in edge case of all identical scores */
        greatest(max_raw_score - min_raw_score, 1) AS raw_score_range,
        0.01 as min_target,
        1000 as max_target,
        max_target - min_target as target_range 
        FROM scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_{purchase_data_source}
)
    SELECT
        account_id, 
        pid1 as lookup_key, 
        pid2 as product_id, 
        score,
        ordinal,
        round((score - score_scaling.min_raw_score) / (score_scaling.raw_score_range) /* map raw score to range [0, 1] */
            * (score_scaling.target_range) /* scale into range [0, max_target] */
            + score_scaling.min_target, /* shift up by min_target */
            2) /* round to 2 places (arbitrary; per requirements for search usage) */
            AS normalized_score
        FROM (
            SELECT
                account_id, pid1, pid2, score,
                ROW_NUMBER() OVER (PARTITION by account_id, pid1 ORDER BY score DESC, pid2 DESC) AS ordinal
            FROM scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_{purchase_data_source}
        )
JOIN score_scaling"""

SKU_RANKS_BY_COLLAB_RECSET = """
CREATE TEMPORARY TABLE scratch.recset_{account_id}_{recset_id}_ranks AS
WITH
    latest_catalog as (
    SELECT pc.* FROM product_catalog as pc
    JOIN config_dataset_data_expiration e
    ON pc.dataset_id = e.dataset_id
    WHERE pc.retailer_id=:retailer_id AND pc.dataset_id = :catalog_dataset_id
    AND pc.update_time >= e.cutoff_time
    ),
    filtered_catalog as (
    SELECT *
    FROM latest_catalog as lc
    {static_filter}
    ),
     context_items_attributes AS (
      SELECT distinct context.item_group_id {context_attributes}
      FROM scratch.pid_ranks_{algorithm}_{pid_rank_account_id}_{market_id}_{retailer_id}_{lookback_days} as pid_algo
      JOIN latest_catalog context
      ON pid_algo.lookup_key = context.item_group_id
    ),
    
    recommendation_item_attributes AS (
        SELECT recommendation.item_group_id, max(recommendation.id) as id, recommendation.color, recommendation.image_link {recommendation_attributes}
        FROM scratch.pid_ranks_{algorithm}_{pid_rank_account_id}_{market_id}_{retailer_id}_{lookback_days} as pid_algo
        JOIN filtered_catalog recommendation
        ON pid_algo.lookup_key = recommendation.item_group_id
        {recommendation_attributes_group_by}
    ),
    sku_algo AS (
        /* Explode recommended product ids into recommended representative skus */
        SELECT
            pid_algo.lookup_key,
            max(recommendation.id) as id,
            pid_algo.score,
            pid_algo.normalized_score
         FROM scratch.pid_ranks_{algorithm}_{pid_rank_account_id}_{market_id}_{retailer_id}_{lookback_days} as pid_algo
         JOIN recommendation_item_attributes recommendation
            ON pid_algo.product_id = recommendation.item_group_id 
            {dynamic_filter}
        GROUP BY 1,3,4,recommendation.color,recommendation.image_link
    )
    SELECT
        lookup_key,
        id,
        ordinal AS rank,
        normalized_score
    FROM (
        SELECT
            lookup_key,
            id,
            ROW_NUMBER() OVER (PARTITION by lookup_key ORDER BY score DESC, id DESC, lookup_key DESC) AS ordinal,
            normalized_score
        FROM sku_algo
    )
    WHERE rank <= 50
"""


def parse_supported_filters(filter_dict, catalog_fields, algo_type):

    # a filter is supported if:
    #   1. it is in the list of supported prefilter fields AND
    #   2. the filter type (equal to, starts with, etc) is supported AND
    #   3. the datatype is a supported data type
    def _filter_supported_filters(f):
        catalog_field = next((catalog_field for catalog_field in catalog_fields if
                              catalog_field["name"].lower() == f['left']['field'].lower()), None)
        return (
                f['left']['field'].lower() not in UNSUPPORTED_PREFILTER_FIELDS
        ) and (
                f['type'] in FILTER_MAP
        ) and (
            catalog_field and catalog_field["data_type"].lower() in SUPPORTED_DATA_TYPES
        )

    # a filter is supported if:
    #   1. true from _filter_supporterd_filters
    #   2. is a static or a product_type
    def _filter_non_collab_supported_filters(f):
        return _filter_supported_filters(f) and (
                f['right']['type'] != 'function' or f['left']['field'] == 'product_type')

    # a filter is supported if:
    #   1. true from _filter_supporterd_filters
    #   2. is a static or dynamic function items_from_base_recommendation_on
    def _filter_collab_supported_filters(f):
        return _filter_supported_filters(f) and (
                f['right']['type'] != 'function' or f['right']['value'] == 'items_from_base_recommendation_on')

    if algo_type == 'collaborative':
        supported_filters = list(filter(_filter_collab_supported_filters, filter_dict['filters']))
    else:
        supported_filters = list(filter(_filter_non_collab_supported_filters, filter_dict['filters']))

    return supported_filters


def parse_non_collab_filters(filter_json, catalog_fields):
    filter_dict = json.loads(filter_json)
    supported_filters = parse_supported_filters(filter_dict, catalog_fields, 'non_collaborative')
    static_supported_filters = [f for f in supported_filters if f['right']['type'] != 'function']
    product_type_filters = [f for f in supported_filters if f['left']['field'] == 'product_type']
    static_product_type_filters = [f for f in product_type_filters if f['right']['type'] != 'function']
    static_supported_filters_non_product_type = [f for f in static_supported_filters
                                                 if f['left']['field'] != 'product_type']


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


def parse_collab_filters(filter_json, catalog_fields):

    filter_dict = json.loads(filter_json)
    static_supported_filters = deepcopy(filter_dict)
    dynamic_supporterd_filter = deepcopy(filter_dict)
    supported_filters = parse_supported_filters(filter_dict, catalog_fields, 'collaborative')

    static_supported_filters['filters'] = [f for f in supported_filters if f['right']['type'] != 'function']
    dynamic_supporterd_filter['filters'] = [f for f in supported_filters if f['right']['type'] == 'function']

    return static_supported_filters, dynamic_supporterd_filter


def collab_dynamic_filter_query(recset_dynamic_filter, global_dynamic_filter, catalog_fields):
    if recset_dynamic_filter['filters'] or global_dynamic_filter['filters']:
        dynamic_filter_sql, dynamic_filter_variables = new_filters.get_query_and_variables_collab(recset_dynamic_filter,
                                                                                              global_dynamic_filter,
                                                                                              catalog_fields)
        # the query is used when we have dynamic filters in a rec strategy for collab algo
        # this query is used in the SKU_RANKS_BY_COLLAB_RECSET query in place of the {dynamic_filter} variable
        dynamic_filter_query = """
        JOIN context_items_attributes context
        ON pid_algo.lookup_key = context.item_group_id
        WHERE {}""".format(dynamic_filter_sql)
        return dynamic_filter_query
    return ''

def get_item_attributes_from_filtered_catalog(recset_dynamic_filter, global_dynamic_filter, catalog_fields):
    context_attributes = ""
    recommendation_attributes = ""
    recommendation_attributes_group_by = "GROUP BY recommendation.item_group_id, recommendation.color, recommendation.image_link"
    dynamic_filters = recset_dynamic_filter['filters'] + global_dynamic_filter['filters']
    if dynamic_filters:
        has_custom_attributes = False
        for each in dynamic_filters:
            attribute = each["left"]["field"]
            field = "{}".format(attribute)
            if attribute not in SUPPORTED_PREFILTER_FIELDS:
                has_custom_attributes = True
                snowflake_type = [field["data_type"] for field in catalog_fields if field["name"] == attribute][0]
                field = "custom:{}::{} as {}".format(attribute, snowflake_type, attribute.lower())

            context_attributes += ", context.{}".format(field)
            recommendation_attributes += ", recommendation.{}".format(field)
            recommendation_attributes_group_by += ", {}".format(attribute.lower())

        if has_custom_attributes:
            context_attributes += ", context.custom"
            recommendation_attributes += ", recommendation.custom"
            recommendation_attributes_group_by += ", recommendation.custom"
    return context_attributes, recommendation_attributes, recommendation_attributes_group_by

def get_static_and_dynamic_filter(recset_filter, global_filter, catalog_fields):

    recset_static_filter, recset_dynamic_filter = parse_collab_filters(recset_filter, catalog_fields)
    global_static_filter, global_dynamic_filter = parse_collab_filters(global_filter, catalog_fields)
    dynamic_filter_sql = collab_dynamic_filter_query(recset_dynamic_filter,global_dynamic_filter, catalog_fields)
    static_filter_sql, static_filter_variables = new_filters.get_query_and_variables_collab(recset_static_filter,
                                           global_static_filter, catalog_fields)
    context_attributes, recommendation_attributes, recommendation_attributes_group_by = get_item_attributes_from_filtered_catalog(recset_dynamic_filter, global_dynamic_filter, catalog_fields)
    return ('WHERE ' + static_filter_sql), static_filter_variables, dynamic_filter_sql, context_attributes, recommendation_attributes, recommendation_attributes_group_by


def get_fact_time(lookback):
    begin_fact_time = datetime.datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=lookback)
    end_fact_time = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    return begin_fact_time, end_fact_time


def create_metric_table(conn, account_ids, algorithm, query, begin_fact_time, end_fact_time,
                        begin_session_time, end_session_time, begin_30_day_fact_time, end_30_day_fact_time,
                        begin_30_day_session_time, end_30_day_session_time):

    if algorithm == 'trending':
        conn.execute(query, account_ids=account_ids, begin_7_day_fact_time=begin_fact_time,
                     end_7_day_fact_time=end_fact_time, begin_7_day_session_time=begin_session_time,
                     end_7_day_session_time=end_session_time, begin_30_day_fact_time=begin_30_day_fact_time,
                     end_30_day_fact_time=end_30_day_fact_time, begin_30_day_session_time=begin_30_day_session_time,
                     end_30_day_session_time=end_30_day_session_time)

    else:
        conn.execute(query, account_ids=account_ids, begin_fact_time=begin_fact_time, end_fact_time=end_fact_time,
                     begin_session_time=begin_session_time, end_session_time=end_session_time)


def get_shard_key(account_id):
    return binascii.crc32(six.ensure_binary(str(account_id))) % CLUSTER_MAX


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

def get_pushdown_filter_json(unload_sql_params, geo_target):
    pushdown_filter_json = {'product_type':unload_sql_params['dynamic_product_type']}
    geo_cols = GEO_TARGET_COLUMNS.get(geo_target, [])

    for col in geo_cols:
        pushdown_filter_json["_"+col] = "IFNULL({},'')".format(col)

    return pushdown_filter_json

def get_pushdown_filter_str(pushdown_filter_json):
    # Sort the json by keys to avoid creating different hash for the same json.
    keys = sorted(pushdown_filter_json.keys())
    pushdown_filter_str = ""
    for key in keys:
        pushdown_filter_str += "'{}',{},".format(key, pushdown_filter_json[key])
    return pushdown_filter_str[:-1]

def get_path_info(key_id):
    stage = getattr(settings, 'SNOWFLAKE_DATAIO_STAGE', '@dataio_stage_v1')
    shard_key = get_shard_key(key_id)
    vshard_lower, vshard_upper = get_shard_range(shard_key)
    dt = datetime.datetime.utcnow()
    interval_duration = 1
    round_minute = dt.minute - dt.minute % interval_duration
    bucket_time = dt.replace(minute=round_minute, second=0, microsecond=0)
    return stage, vshard_lower, vshard_upper, interval_duration, bucket_time


def create_unload_target_path(account_id, recset_id):
    stage, vshard_lower, vshard_upper, interval_duration, bucket_time = get_path_info(account_id)

    path = '{data_jurisdiction}/{bucket_time:%Y/%m/%d}/{data_jurisdiction}-{bucket_time:%Y%m%dT%H%M%S.000Z}_PT' \
           '{interval_duration}M-{vshard_lower}-{vshard_upper}-precompute_{account_id}_{recset_id}.json.gz'\
        .format(data_jurisdiction=DATA_JURISDICTION,
                bucket_time=bucket_time,
                interval_duration=interval_duration,
                vshard_lower=vshard_lower,
                vshard_upper=vshard_upper,
                account_id=account_id,
                recset_id=recset_id)
    new_path = '{data_jurisdiction}/{bucket_time:%Y/%m/%d}/{data_jurisdiction}-{bucket_time:%Y%m%dT%H%M%S.000Z}_PT' \
               '{interval_duration}M-{vshard_lower}-{vshard_upper}-precompute_{account_id}_{recset_id}_new.json.gz'\
            .format(data_jurisdiction=DATA_JURISDICTION,
                bucket_time=bucket_time,
                interval_duration=interval_duration,
                vshard_lower=vshard_lower,
                vshard_upper=vshard_upper,
                account_id=account_id,
                recset_id=recset_id)

    return os.path.join(stage, path), os.path.join(stage, new_path), bucket_time


def unload_target_pid_path(account_id, market_id, retailer_id, algorithm, lookback_days):
    key_id = str(account_id) + str(market_id) + str(retailer_id)
    recset_group_id = '{account_id}_{market_id}_{market_id}'.format(account_id=account_id, market_id=market_id,
                                                                    retailer_id=retailer_id)
    stage, vshard_lower, vshard_upper, interval_duration, bucket_time = get_path_info(key_id)

    path = '{data_jurisdiction}/{bucket_time:%Y/%m/%d}/{data_jurisdiction}-{bucket_time:%Y%m%dT%H%M%S.000Z}_PT' \
           '{interval_duration}M-{vshard_lower}-{vshard_upper}-precompute_' \
           '{recset_group_id}_{algorithm}_{lookback_days}.json.gz' \
        .format(data_jurisdiction=DATA_JURISDICTION_PID_PID,
                bucket_time=bucket_time,
                interval_duration=interval_duration,
                vshard_lower=vshard_lower,
                vshard_upper=vshard_upper,
                recset_group_id=recset_group_id,
                algorithm=algorithm,
                lookback_days=lookback_days)

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


def get_account_ids_for_processing(queue_entry):
    if queue_entry.market:
        account_ids = [account.id for account in queue_entry.market.accounts.all()]
        log.log_info('Market scoped recset, using {} accounts'.format(len(account_ids)))
        return account_ids
    if queue_entry.retailer:
        account_ids = [account.id for account in queue_entry.retailer.account_set.all()]
        log.log_info('Retailer scoped recset, using {} accounts'.format(len(account_ids)))
        return account_ids
    else:
        log.log_info('Account scoped recset for account {}'.format(queue_entry.account))
        return [queue_entry.account.id]


def get_recset_ids(recset_group):
    if recset_group.account:
        # recset_group -> queue item
        # get the recsets that the recset_group referes too with the combination of algo and lookback
        # for that account
        # account, algo, and lookback -> Retailer level recset (not market) and account level recset (not market)
        retailer_recset_ids = RecommendationSetDataset.objects.filter(
            account_id=recset_group.account, recommendation_set_id__algorithm=recset_group.algorithm,
            recommendation_set_id__lookback_days=recset_group.lookback_days, recommendation_set_id__market_id=None,
            recommendation_set_id__retailer_market_scope=None,
            recommendation_set_id__purchase_data_source=recset_group.purchase_data_source,
            recommendation_set_id__archived=False)
        retailer_recsets = RecommendationSet.objects.filter(
                id__in=[retailer_recset_id.recommendation_set_id for retailer_recset_id in retailer_recset_ids]
            )

        account_recsets = RecommendationSet.objects.filter(
            (Q(account=recset_group.account, algorithm=recset_group.algorithm,
               lookback_days=recset_group.lookback_days, market_id=None, retailer_market_scope=None,
               purchase_data_source=recset_group.purchase_data_source, archived=False)))
        return retailer_recsets | account_recsets

    elif recset_group.market:
        recsets = RecommendationSet.objects.filter(market_id=recset_group.market,
                                                   algorithm=recset_group.algorithm,
                                                   lookback_days=recset_group.lookback_days,
                                                   purchase_data_source=recset_group.purchase_data_source,
                                                   archived=False)
        return recsets
    # retailer market - processing "All accounts for this retailer" (global or account level recset)
    elif recset_group.retailer:
        recsets = RecommendationSet.objects.filter(retailer_market_scope=1,
                                                   retailer_id=recset_group.retailer,
                                                   algorithm=recset_group.algorithm,
                                                   lookback_days=recset_group.lookback_days,
                                                   purchase_data_source=recset_group.purchase_data_source,
                                                   archived=False)
        return recsets

    return []


def get_algo_filter_dict(algorithm):

    # sane default
    algo_dict = {
        "filters":[]
    }

    # only one algo filter should be active at a time
    # currently, there is only one algo-level filter
    # TODO: if more algo-level filters are required in the future, we should simplify/clean this up.
    # TODO: maybe something similar to the funcmap
    if algorithm == "bought_together":
        algo_dict["filters"] = [
            {"left":
                {
                    "type":"field",
                    "field":"product_type"
                },
                "type":"not startswith",
                "right":
                {
                    "type":"function",
                    "value":"items_from_base_recommendation_on"
                }
             }
        ]

    return algo_dict

def process_noncollab_algorithm(conn, recset, metric_table_query, offline_query=None, online_offline_query=None):
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
        try:
            catalog_id = recset.product_catalog.id if recset.product_catalog else \
                dio_models.DefaultAccountCatalog.objects.get(account=account_id).schema.id
            catalog_fields = dio_models.Schema.objects.get(id=catalog_id).active_field_set.values("name", "data_type")
        except:
            log.log_info("Skipping account id {}, no catalog set".format(account_id))
            continue
        early_filter_exp, late_filter_exp, has_dynamic_filter = parse_non_collab_filters(recset.filter_json,
                                                                                         catalog_fields)
        global_early_filter_exp, global_late_filter_exp, global_has_dynamic_filter = \
            parse_non_collab_filters(global_filter_json, catalog_fields)
        has_dynamic_filter = has_dynamic_filter or global_has_dynamic_filter
        early_filter_sql, late_filter_sql, filter_variables = new_filters.get_query_and_variables_non_collab(
            early_filter_exp, late_filter_exp, global_early_filter_exp, global_late_filter_exp, catalog_fields)
        account_ids = get_account_ids_for_market_driven_recsets(recset, account_id)
        account = None if recset.is_market_or_retailer_driven_ds else account_id
        market = recset.market.id if recset.market else None
        retailer = recset.retailer.id if recset.retailer else None
        begin_fact_time, end_fact_time = get_fact_time(recset.lookback_days)
        begin_session_time, end_session_time = sqlalchemy_warehouse.get_session_time_bounds(
            begin_fact_time, end_fact_time)

        begin_30_day_fact_time, end_30_day_fact_time = get_fact_time(30)
        begin_30_day_session_time, end_30_day_session_time = None, None
        if recset.algorithm == "trending":
            begin_30_day_session_time, end_30_day_session_time = sqlalchemy_warehouse.get_session_time_bounds(
                begin_30_day_fact_time, end_30_day_fact_time
            )
        account_ids_dataset_ids = precompute_purchase_associated_pids.get_dataset_ids_for_pos(account_ids)
        create_helper_query_for_non_collab_algorithm(recset, account, market, retailer,
                                                       begin_fact_time, account_ids_dataset_ids, conn)

        if recset.purchase_data_source in ["online", "online_offline"]:
            # online_query
            create_metric_table(conn, account_ids, recset.algorithm,
                                text(metric_table_query.format(algorithm=recset.algorithm, account_id=account,
                                                               lookback=recset.lookback_days,
                                                               market_id=market,
                                                               retailer_scope=recset.retailer_market_scope)),
                                begin_fact_time, end_fact_time, begin_session_time, end_session_time,
                                begin_30_day_fact_time, end_30_day_fact_time,
                                begin_30_day_session_time, end_30_day_session_time)

        if recset.purchase_data_source == "online_offline":
            if recset.algorithm in ["purchase", "trending", "purchase_value"]:
                # offline_query
                conn.execute(text(
                    offline_query.format(
                        algorithm=recset.algorithm, account_id=account, market_id=market, retailer_id=retailer,
                        retailer_scope=recset.retailer_market_scope, lookback_days=recset.lookback_days)),
                    begin_fact_time=begin_fact_time, end_fact_time=end_fact_time, account_id=account,
                    begin_7_day_session_time=begin_session_time, end_7_day_session_time=end_session_time,
                    begin_30_day_session_time=begin_30_day_session_time, end_30_day_session_time=end_30_day_session_time)
                # online_offline (union all + sum query)
                conn.execute(text(
                    online_offline_query.format(
                        algorithm=recset.algorithm, account_id=account, market_id=market,  retailer_id=retailer,
                        retailer_scope=recset.retailer_market_scope, lookback_days=recset.lookback_days,
                        purchase_data_source=recset.purchase_data_source)))

        if recset.purchase_data_source == "offline":
            if recset.algorithm in ["purchase", "trending", "purchase_value"]:
                # offline_query
                conn.execute(text(
                    offline_query.format(
                        algorithm=recset.algorithm, account_id=account, market_id=market, retailer_id=retailer,
                        retailer_scope=recset.retailer_market_scope, lookback_days=recset.lookback_days)),
                    begin_fact_time=begin_fact_time, end_fact_time=end_fact_time, account_id=account,
                    begin_7_day_session_time=begin_session_time, end_7_day_session_time=end_session_time,
                    begin_30_day_session_time=begin_30_day_session_time, end_30_day_session_time=end_30_day_session_time)

        unload_path, new_unload_path, send_time = create_unload_target_path(account_id, recset.id)
        unload_sql = get_unload_sql(recset.geo_target, has_dynamic_filter)
        pushdown_filter_json = get_pushdown_filter_json(unload_sql, recset.geo_target)
        pushdown_filter_str = get_pushdown_filter_str(pushdown_filter_json)

        conn.execute(text(SKU_RANKS_BY_RECSET.format(algorithm=recset.algorithm,
                                                     recset_id=recset.id,
                                                     account_id=account_id,
                                                     metric_table_account_id=None if recset.is_market_or_retailer_driven_ds else account_id,
                                                     lookback=recset.lookback_days,
                                                     early_filter=early_filter_sql,
                                                     late_filter=late_filter_sql,
                                                     market_id=recset.market.id if recset.market else None,
                                                     retailer_scope=recset.retailer_market_scope,
                                                     purchase_data_source=recset.purchase_data_source,
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

        # Unload to new path only if feature flag is enabled.
        precompute_feature = retailer_models.ACCOUNT_FEATURES.UNIFIED_PRECOMPUTE
        account_obj = retailer_models.Account.objects.get(id=account_id)
        if account_obj.has_feature(precompute_feature):
            conn.execute(text(SNOWFLAKE_UNLOAD_2.format(recset_id=recset.id, account_id=account_id,
            pushdown_filter_str=pushdown_filter_str,
            **unload_sql)),
                        shard_key=get_shard_key(account_id),
                        account_id=account_id,
                        recset_id=recset.id,
                        sent_time=send_time,
                        target=new_unload_path)
    return result_counts

# TODO: function name here, only running offline query if certain conditions are met
def create_helper_query_for_non_collab_algorithm(recset, account, market, retailer,
                                                 begin_fact_time, account_ids_dataset_ids, conn):
    if recset.algorithm in ["purchase", "trending", "purchase_value"]:
        # GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID is required in case of both online_offline and offline
        lookback_days = recset.lookback_days
        if recset.purchase_data_source in ["online_offline", "offline"]:
            if not account_ids_dataset_ids:
                raise ValueError('Account/s {} has/have no offline purchase datasets'.format(account))
            conn.execute(text(
                precompute_purchase_associated_pids.GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID.format(
                    account_id=account, market_id=market,
                    retailer_id=retailer, lookback_days=lookback_days)), begin_fact_time=begin_fact_time,
                aids_dids=account_ids_dataset_ids)

def get_account_ids_for_catalog_join_and_output(recset, queue_account):
    """
    For a given recset, return the account/s to process
    """
    precompute_feature = retailer_models.ACCOUNT_FEATURES.ENABLE_COLLAB_RECS_PRECOMPUTE_MODELING
    # if the recset is retailer & non-market, return the account included in the queue entry
    # for such recsets, we will have enqueued each account as a separate queue item
    if recset.is_retailer_tenanted and not recset.is_market_or_retailer_driven_ds:
        return [queue_account]
    # for market recsets defined across all accounts, return all accounts
    elif recset.is_retailer_tenanted:
        return [account for account in
                retailer_models.Account.objects.filter(retailer_id=recset.retailer_id,
                                                       archived=False,
                                                       accountfeature__feature_flag__name=precompute_feature)]
    # for account level recset, return the account directly from the recset
    else:
        if recset.account.has_feature(precompute_feature):
            return [recset.account]
    return []


def process_collab_recsets(conn, queue_entry, account, market, retailer):
    result_counts = []
    recsets = get_recset_ids(queue_entry)
    for recset in recsets:
        account_ids = get_account_ids_for_catalog_join_and_output(recset, queue_entry.account)
        for account_id in account_ids:
            log.log_info("Processing recset id {}, account id {} for queue entry {}"
                         .format(recset.id, account_id, queue_entry.id))
            recommendation_settings = AccountRecommendationSetting.objects.filter(account_id=account_id)
            global_filter_json = recommendation_settings[0].filter_json if recommendation_settings else u'{"type":"or","filters":[]}'

            try:
                catalog_id = recset.product_catalog.id if recset.product_catalog else \
                    dio_models.DefaultAccountCatalog.objects.get(account=account_id).schema.id
                catalog_fields = dio_models.Schema.objects.get(id=catalog_id).active_field_set.values("name", "data_type")
            except dio_models.DefaultAccountCatalog.DoesNotExist:
                log.log_info("Skipping {} with account id {}, no catalog set found".format(account_id, account_id.id))
                continue

            # get algo_filter json
            algo_filter_dict = get_algo_filter_dict(recset.algorithm)

            # add algo_filter to the recset filters
            recset_filter_dict = json.loads(recset.filter_json)
            recset_filter_dict['filters'].extend(algo_filter_dict['filters'])
            final_filter_json = json.dumps(recset_filter_dict)

            # pass the algorithm into get_static_and_dynamic_filter and return algo_filter_sql
            # along with the other filter sql
            static_filter_sql, static_filter_variables, dynamic_filter_sql, context_attributes, recommendation_attributes, recommendation_attributes_group_by = get_static_and_dynamic_filter(
                final_filter_json, global_filter_json, catalog_fields)
            # this query explodes the pid to sku to create a pid-sku relation
            conn.execute(text(SKU_RANKS_BY_COLLAB_RECSET.format(algorithm=recset.algorithm, recset_id=recset.id,
                                                                account_id=account_id.id,
                                                                pid_rank_account_id=account,
                                                                lookback_days=recset.lookback_days,
                                                                dynamic_filter=dynamic_filter_sql,
                                                                market_id=market,
                                                                retailer_id=retailer,
                                                                static_filter=static_filter_sql,
                                                                context_attributes=context_attributes,
                                                                recommendation_attributes=recommendation_attributes,
                                                                recommendation_attributes_group_by=recommendation_attributes_group_by
                                                                )),
                         retailer_id=recset.retailer.id,
                         catalog_dataset_id=catalog_id,
                         **static_filter_variables)

            unload_path, new_unload_path, send_time = create_unload_target_path(account_id.id, recset.id)
            result_counts.append(get_single_value_query(conn.execute(text(
                RESULT_COUNT.format(recset_id=recset.id,account_id=account_id.id,))), 0))
            # this query write the pid-sku relation to s3
            conn.execute(text(SNOWFLAKE_UNLOAD_COLLAB.format(recset_id=recset.id, account_id=account_id.id)),
                         shard_key=get_shard_key(account_id.id),
                         account_id=account_id.id,
                         recset_id=recset.id,
                         sent_time=send_time,
                         target=unload_path)
            log.log_info("Finished processing recset id {}, number of rows {} and file path {}".format(
                recset.id, result_counts[-1], unload_path))

    return result_counts
