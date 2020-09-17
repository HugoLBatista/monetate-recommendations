from django.conf import settings
import os
import datetime
import json
import binascii
import bisect
from sqlalchemy.sql import text
from monetate.common.warehouse import sqlalchemy_warehouse
from monetate.common.sqlalchemy_session import CLUSTER_MAX
import monetate.retailer.models as retailer_models
import monetate.dio.models as dio_models
from monetate_recommendations import product_type_filter_expression
from monetate_recommendations import constants

DATA_JURISDICTION = 'recs_global'
SESSION_SHARDS = 8


SNOWFLAKE_UNLOAD = """
COPY
INTO :target
FROM (
    WITH filtered_scored_records AS (
        {query}
    ),
    ranked_records AS (
        {rank_query}
    )
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
    FROM ranked_records
    WHERE rank <= 1000
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


# SKU ranking query used by view, purchase, and purchase_value algorithms
SKU_RANKS_BY_REGION_FOR_ACCOUNT_ID = """
WITH
pid_algo AS (
    /* Aggregates per product_id for an account at appropriate geo_rollup level */
    SELECT
        product_id,
        SUM(subtotal) AS score
        {geo_columns}
    FROM scratch.{algorithm}_{account_id}_{lookback}
    GROUP BY product_id
    {geo_columns}
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
            GROUP BY 1, 2, 3
        )
        {filter_query}
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
)
SELECT pc.*, sa.score
  {geo_columns}
FROM product_catalog as pc
JOIN sku_algo as sa
    ON pc.id = sa.id
WHERE pc.retailer_id = :retailer_id
    AND pc.dataset_id = :catalog_id
"""


def parse_product_type_filter(filter_json):
    def _filter_product_type(f):
        return f['left']['field'] == 'product_type'

    def _filter_dynamic(f):
        return f['right']['type'] != 'function'

    filter_dict = json.loads(filter_json)
    product_type_filters = filter(_filter_product_type, filter_dict['filters'])
    static_product_type_filters = filter(_filter_dynamic, product_type_filters)

    filter_dict['filters'] = static_product_type_filters
    has_dynamic_filter = filter_dict['type'] == 'and' and \
                                      len(static_product_type_filters) != len(product_type_filters)

    return filter_dict, has_dynamic_filter


def create_metric_table(conn, account_id, lookback, query):
    begin_fact_time = datetime.datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=lookback)
    end_fact_time = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    begin_session_time, end_session_time = sqlalchemy_warehouse.get_session_time_bounds(
        begin_fact_time, end_fact_time)
    conn.execute(query, account_id=account_id, begin_fact_time=begin_fact_time, end_fact_time=end_fact_time,
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


def get_retailer_strategy_accounts(retailer_id):
    accounts = retailer_models.Account.objects.filter(retailer_id=retailer_id, archived=False)
    return [account.id for account in accounts if len(dio_models.DefaultAccountCatalog.objects.filter(
        account=account.id))]


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
    geo_cols = constants.GEO_TARGET_COLUMNS.get(geo_target, None)
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
           '{interval_duration}M-{vshard_lower}-{vshard_upper}-precompute_{recset_id}.json.gz'\
        .format(data_jurisdiction=DATA_JURISDICTION,
                bucket_time=bucket_time,
                interval_duration=interval_duration,
                vshard_lower=vshard_lower,
                vshard_upper=vshard_upper,
                recset_id=recset_id)

    return os.path.join(stage, path), bucket_time


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
    account_ids = [recset.account.id] if recset.account else get_retailer_strategy_accounts(recset.retailer.id)
    for account_id in account_ids:
        product_type_filter, has_dynamic_filter = parse_product_type_filter(recset.filter_json)
        filter_variables, filter_query = product_type_filter_expression.get_query_and_variables(
            product_type_filter)
        catalog_id = recset.product_catalog.id if recset.product_catalog else \
            dio_models.DefaultAccountCatalog.objects.get(account=account_id).schema.id
        create_metric_table(conn, account_id, recset.lookback_days,
                            text(metric_table_query.format(algorithm=recset.algorithm,
                                                           account_id=account_id,
                                                           lookback=recset.lookback_days)))
        unload_path, send_time = create_unload_target_path(account_id, recset.id)
        unload_sql = get_unload_sql(recset.geo_target, has_dynamic_filter)
        product_rank_query = SKU_RANKS_BY_REGION_FOR_ACCOUNT_ID.format(algorithm=recset.algorithm,
                                                                       account_id=account_id,
                                                                       lookback=recset.lookback_days,
                                                                       filter_query=filter_query,
                                                                       **unload_sql)
        conn.execute(text(SNOWFLAKE_UNLOAD.format(query=product_rank_query, **unload_sql)),
                     shard_key=get_shard_key(account_id),
                     account_id=account_id,
                     retailer_id=recset.retailer.id,
                     recset_id=recset.id,
                     sent_time=send_time,
                     target=unload_path,
                     filter_json=recset.filter_json,
                     catalog_id=catalog_id,
                     **filter_variables)
