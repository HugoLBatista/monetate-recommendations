from django.conf import settings
from django.db.models import Q
import contextlib
import os
import datetime
import json
import binascii
import bisect
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
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
from supported_prefilter_expression import SUPPORTED_PREFILTER_FIELDS, FILTER_MAP

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
        FROM scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}
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
            FROM  scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}
        )
JOIN score_scaling"""


SKU_RANKS_BY_COLLAB_RECSET = """
CREATE TEMPORARY TABLE scratch.recset_{account_id}_{recset_id}_ranks AS
WITH 
    non_expired_catalog_items as (
    SELECT pc.* FROM product_catalog as pc
    JOIN config_dataset_data_expiration e
    ON pc.dataset_id = e.dataset_id 
    WHERE pc.retailer_id=:retailer_id AND pc.dataset_id = :catalog_dataset_id
    AND pc.update_time >= e.cutoff_time
    ),
    sku_algo AS (
        /* Explode recommended product ids into recommended representative skus */
        SELECT
            pid_algo.lookup_key,
            max(recommendation.id) as id,
            pid_algo.score,
            pid_algo.normalized_score
         FROM scratch.pid_ranks_{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days} as pid_algo
            JOIN non_expired_catalog_items context
            ON pid_algo.lookup_key = context.item_group_id
          JOIN non_expired_catalog_items recommendation
            ON pid_algo.product_id = recommendation.item_group_id
            {filter}
            GROUP BY 1,3,4
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
            ROW_NUMBER() OVER (PARTITION by lookup_key ORDER BY score DESC, id DESC) AS ordinal,
            normalized_score
        FROM sku_algo
    )
    WHERE rank <= 1000
    
"""
# account_id , market_id and retailer_id create a unique key only one variable will have a value and rest will be None
# example  6814_None_None
GET_LAST_PURCHASE_PER_MID_AND_PID = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.last_purchase_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
SELECT account_id, mid_epoch, mid_ts, mid_rnd, product_id, max(fact_time) as fact_time
FROM m_dedup_purchase_line
WHERE account_id in (:account_ids)
    AND fact_time >= :begin_fact_time
GROUP BY 1, 2, 3, 4, 5
"""
# account_id , market_id and retailer_id create a unique key only one variable will have a value and rest will be None
# example  6814_None_None
GET_LAST_VIEW_PER_MID_AND_PID = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.last_view_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
WITH device_earliest_product_view AS (
    SELECT fact_product_view.account_id, mid_epoch, mid_ts, mid_rnd, product_id, min(fact_time) fact_time
    FROM fact_product_view
    /* ignore views whose pids are not in catalog so invalid pids coming in on view facts do not get into the results */
    JOIN config_account ON (fact_product_view.account_id = config_account.account_id)
    JOIN product_catalog ON (fact_product_view.product_id = product_catalog.item_group_id
        AND config_account.retailer_id = product_catalog.retailer_id)
    WHERE fact_product_view.account_id in (:account_ids) AND
        fact_time >= :begin_fact_time
    GROUP BY 1, 2, 3, 4, 5
),
filtered_devices AS (
    /* Exclude devices viewing more than 1000 distinct products per month */
    SELECT account_id, mid_epoch, mid_ts, mid_rnd, count(*)
    FROM device_earliest_product_view
    GROUP BY 1, 2, 3, 4
    HAVING count(*) < (:lookback / 30.0 * 1000)
)
SELECT p.account_id, p.mid_epoch, p.mid_ts, p.mid_rnd, p.product_id, p.fact_time
FROM device_earliest_product_view p
JOIN filtered_devices fd
    ON fd.account_id = p.account_id
    AND fd.mid_epoch = p.mid_epoch
    AND fd.mid_ts = p.mid_ts
    AND fd.mid_rnd = p.mid_rnd
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


def get_fact_time(lookback):
    begin_fact_time = datetime.datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=lookback)
    end_fact_time = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    return begin_fact_time, end_fact_time


def create_helper_query(conn, accounts, lookback, algorithm, query):
    begin_fact_time, end_fact_time = get_fact_time(lookback)
    if algorithm == 'purchase_also_purchase':
        conn.execute(query, account_ids=accounts, begin_fact_time=begin_fact_time, end_fact_time=end_fact_time)
    elif algorithm == 'view_also_view':
        conn.execute(query, account_ids=accounts, begin_fact_time=begin_fact_time, end_fact_time=end_fact_time,
                     lookback=lookback)


def create_metric_table(conn, account_ids, lookback, algorithm, query):

    begin_fact_time, end_fact_time = get_fact_time(lookback)
    begin_session_time, end_session_time = sqlalchemy_warehouse.get_session_time_bounds(
        begin_fact_time, end_fact_time)

    if algorithm == 'trending':

        begin_30_day_fact_time, end_30_day_fact_time = get_fact_time(30)

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

    return os.path.join(stage, path), bucket_time


def unload_target_pid_path(account_id, market_id, retailer_id):
    key_id = str(account_id) + str(market_id) + str(retailer_id)
    recset_group_id = '{account_id}_{market_id}_{market_id}'.format(account_id=account_id, market_id=market_id,
                                                                    retailer_id=retailer_id)
    stage, vshard_lower, vshard_upper, interval_duration, bucket_time = get_path_info(key_id)

    path = '{data_jurisdiction}/{bucket_time:%Y/%m/%d}/{data_jurisdiction}-{bucket_time:%Y%m%dT%H%M%S.000Z}_PT' \
           '{interval_duration}M-{vshard_lower}-{vshard_upper}-precompute_{recset_group_id}.json.gz' \
        .format(data_jurisdiction=DATA_JURISDICTION_PID_PID,
                bucket_time=bucket_time,
                interval_duration=interval_duration,
                vshard_lower=vshard_lower,
                vshard_upper=vshard_upper,
                recset_group_id=recset_group_id)

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


def get_recset_group_account_ids(recommendation):
    if recommendation.market:
        account_ids = [account.id for account in recommendation.market.accounts.all()]
        log.log_info('Market scoped recset, using {} accounts'.format(len(account_ids)))
        return account_ids
    if recommendation.retailer:
        account_ids = [account.id for account in recommendation.retailer.account_set.all()]
        log.log_info('Retailer scoped recset, using {} accounts'.format(len(account_ids)))
        return account_ids
    else:
        log.log_info('Account scoped recset for account {}'.format(recommendation.account))
        return [recommendation.account.id]


def get_recset_ids(recset_group):

    if recset_group.account:
        # recset_group -> queue item
        # get the recsets that the recset_group referes too with the combination of algo and lookback
        # for that account
        # account, algo, and lookback -> Retailer level recset (not market) and account level recset (not market)
        retailer_recsets = RecommendationSetDataset.objects.filter(
            account_id=recset_group.account, recommendation_set_id__algorithm=recset_group.algorithm,
            recommendation_set_id__lookback_days=recset_group.lookback_days,
            recommendation_set_id__archived=False)

        recsets = RecommendationSet.objects.filter(
            (Q(id=retailer_recsets) |
             Q(account=recset_group.account, algorithm=recset_group.algorithm,
               lookback_days=recset_group.lookback_days, archived=False)))
        recsets = [recs for recs in recsets]
        return recsets

    elif recset_group.market:
        recsets = RecommendationSet.objects.filter(market_id=recset_group.market,
                                                   algorithm=recset_group.algorithm,
                                                   lookback_days=recset_group.lookback_days,
                                                   archived=False)
        return recsets
    elif recset_group.retailer:
        recsets = RecommendationSet.objects.filter(retailer_id=recset_group.retailer,
                                                   algorithm=recset_group.algorithm,
                                                   lookback_days=recset_group.lookback_days,
                                                   archived=False)
        return recsets

    return []


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
        early_filter_exp, late_filter_exp, has_dynamic_filter = parse_supported_filters(recset.filter_json)
        early_filter_sql, late_filter_sql, filter_variables = supported_prefilter_expression.get_query_and_variables(
            early_filter_exp, late_filter_exp)
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


def initialize_process_collab_algorithm(recsets_group, algorithm, algorithm_query, algo_helper_query):
    result_counts = []
    # Disable pooling so temp tables do not persist on connections returned to pool
    engine = create_engine(settings.SNOWFLAKE_QUERY_DSN, poolclass=NullPool)
    with job_timing.job_timer('precompute_{}_algorithm'.format(algorithm)),\
            contextlib.closing(engine.connect()) as warehouse_conn:
        warehouse_conn.execute("use warehouse {}".format(
            getattr(settings, 'RECS_COLLAB_QUERY_WH', os.environ.get('RECS_COLLAB_QUERY_WH', 'QUERY8_WH'))))
        for recset_group in recsets_group:
            if recset_group and recset_group.algorithm == algorithm:
                log.log_info('processing recset {}'.format(recset_group.id))
                result_counts.append(
                    process_collab_algorithm(warehouse_conn, recset_group, algorithm_query, algo_helper_query))
    log.log_info('ending precompute_{}_algorithm process'.format(algorithm))
    return result_counts

def process_collab_algorithm(conn, recset_group, metric_table_query, helper_query):
    result_counts = []
    account = recset_group.account.id if recset_group.account else None
    market = recset_group.market.id if recset_group.market else None
    retailer = recset_group.retailer.id if recset_group.retailer else None
    algorithm = recset_group.algorithm
    lookback_days = recset_group.lookback_days
    log.log_info('Querying results for recommendation {}'.format(recset_group.id))
    account_ids = get_recset_group_account_ids(recset_group)

    create_helper_query(conn, account_ids, lookback_days, algorithm,
                        text(helper_query.format(account_id=account, market_id=market,
                                                 retailer_id=retailer, lookback_days=lookback_days)))
    if algorithm == 'purchase_also_purchase' and \
            recset_group.account.has_feature(retailer_models.ACCOUNT_FEATURES.MIN_THRESHOLD_FOR_PAP_FBT):
        conn.execute(text(metric_table_query.format(algorithm=algorithm, account_id=account, market_id=market,
                                                    retailer_id=retailer, lookback_days=lookback_days)),
                     minimum_count=MIN_PURCHASE_THRESHOLD)

    # runs view_also_view query or purchase_also_purchase if no threshold feature flag
    else:
        conn.execute(text(metric_table_query.format(algorithm=algorithm, account_id=account, market_id=market,
                                                    retailer_id=retailer, lookback_days=lookback_days)),
                     minimum_count=1)

    conn.execute(text(PID_RANKS_BY_COLLAB_RECSET.format(algorithm=algorithm, account_id=account,
                                                        lookback_days=lookback_days,  market_id=market,
                                                        retailer_id=retailer,
                                                        )))
    unload_pid_path, send_time = unload_target_pid_path(account, market, retailer)
    conn.execute(text(SNOWFLAKE_UNLOAD_PID_PID.format(algorithm=algorithm, account_id=account,
                                                      lookback_days=lookback_days, market_id=market,
                                                      retailer_id=retailer)),
                 account_id=account, market_id=market,
                 retailer_id=retailer,
                 sent_time=send_time,
                 target=unload_pid_path,
                 algorithm=algorithm,
                 lookback_days=lookback_days)

    recsets = get_recset_ids(recset_group)
    for recset in recsets:
        account_ids = RecommendationSetDataset.objects.filter(recommendation_set=recset).values_list('account_id') \
            if recset.is_retailer_tenanted else [recset.account]
        for account_id in account_ids:
            recommendation_settings = AccountRecommendationSetting.objects.filter(account_id=account_id)
            global_filter_json = recommendation_settings[0].filter_json if recommendation_settings else u'{"type":"or","filters":[]}'
            filter_sql, filter_variables = filters.get_query_and_variables_collab(recset.filter_json, global_filter_json)
            catalog_id = recset.product_catalog.id if recset.product_catalog else \
                dio_models.DefaultAccountCatalog.objects.get(account=account_id).schema.id
            conn.execute(text(SKU_RANKS_BY_COLLAB_RECSET.format(algorithm=recset.algorithm, recset_id=recset.id,
                                                                account_id=account_id.id,
                                                                lookback_days=recset.lookback_days,
                                                                filter=filter_sql,
                                                                market_id=market,
                                                                retailer_id=retailer,
                                                                )),
                         retailer_id=recset.retailer.id,
                         catalog_dataset_id=catalog_id,
                         **filter_variables)

            unload_path, send_time = create_unload_target_path(account_id.id, recset.id)
            result_counts.append(get_single_value_query(conn.execute(text(RESULT_COUNT.format(recset_id=recset.id,
                                                                                              account_id=account_id.id,
                                                                                              ))), 0))
            conn.execute(text(SNOWFLAKE_UNLOAD_COLLAB.format(recset_id=recset.id, account_id=account_id.id)),
                         shard_key=get_shard_key(account_id.id),
                         account_id=account_id.id,
                         recset_id=recset.id,
                         sent_time=send_time,
                         target=unload_path)

    return result_counts




