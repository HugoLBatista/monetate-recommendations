from django.conf import settings
import os
import datetime
import hashlib
import json
import binascii
import bisect
from sqlalchemy.sql import text
from monetate.common.warehouse import sqlalchemy_warehouse
from monetate.common.sqlalchemy_session import CLUSTER_MAX
import monetate.dio.models as dio_models
from monetate_recommendations import product_type_filter_expression

DATA_JURISDICTION = 'recs_global'
SESSION_SHARDS = 8

SNOWFLAKE_UNLOAD = """
COPY
INTO :target
FROM (
    WITH ranked_records AS (
        {query}
    )
    SELECT object_construct(
        'shard_key', :shard_key,
        'document', object_construct(
            'pushdown_filter_hash', :filter_hash,
            'data', (
                SELECT array_agg(object_construct(*)) 
                WITHIN GROUP (ORDER BY RANK ASC) 
                FROM ranked_records
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
)
FILE_FORMAT = (TYPE = JSON, compression='gzip')
SINGLE=TRUE
MAX_FILE_SIZE=1000000000
"""

# SKU ranking query used by view, purchase, and purchase_value algorithms
SKU_RANKS_BY_REGION_FOR_ACCOUNT_ID = """
WITH
pid_algo AS (
    /* Aggregates per product_id for an account at appropriate geo_rollup level */
    SELECT
        product_id,
        SUM(subtotal) AS score
    FROM scratch.{algorithm}_{account_id}_{lookback}
    GROUP BY product_id
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
                TRIM(split_product_type.value::string, ' ') as product_type,
                MAX(c.id) AS id
            FROM product_catalog c
            JOIN config_dataset_data_expiration e
                ON c.dataset_id = e.dataset_id,
                LATERAL FLATTEN(input=>split(c.product_type, ',')) split_product_type
            WHERE c.dataset_id = :catalog_id
                AND c.retailer_id = :retailer_id
                AND c.update_time >= e.cutoff_time
            GROUP BY 1, 2, 3, 4
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
    FROM pid_algo
    JOIN reduced_catalog c
    ON c.item_group_id = pid_algo.product_id
),
ranked_ids AS (
    /* Convert scores into ordinals, limit to top 1000 per lookup key for later post filtering */
    SELECT
        id,
        ordinal
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (ORDER BY score DESC, id) AS ordinal
        FROM sku_algo
    )
    WHERE ordinal <= 1000
)
SELECT pc.*, ri.ordinal AS rank
FROM product_catalog as pc
JOIN ranked_ids as ri
    ON pc.id = ri.id
WHERE pc.retailer_id = :retailer_id
    AND pc.dataset_id = :catalog_id
"""


def parse_product_type_filter(filter_json):
    # TODO: In the future this will need to support dyanmic filters
    def _filter_product_type(f):
        is_product_type = f['left']['field'] == 'product_type'
        is_dynamic = f['right']['type'] == 'function'
        return is_product_type and not is_dynamic

    filter_dict = json.loads(filter_json)
    filter_dict['filters'] = filter(_filter_product_type, filter_dict['filters'])
    return filter_dict


def get_filter_hash(filter_json):
    # TODO: In the future this will need to support dyanmic filters
    return hashlib.sha1(filter_json).hexdigest()


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
    product_type_filter = parse_product_type_filter(recset.filter_json)
    filter_variables, filter_query = product_type_filter_expression.get_query_and_variables(
        product_type_filter)
    catalog_id = recset.product_catalog.id if recset.product_catalog else \
        dio_models.DefaultAccountCatalog.objects.get(account=recset.account.id).schema.id
    filter_hash = get_filter_hash(recset.filter_json)
    create_metric_table(conn, recset.account.id, recset.lookback_days,
                        text(metric_table_query.format(algorithm=recset.algorithm,
                                                       account_id=recset.account.id,
                                                       lookback=recset.lookback_days)))
    unload_path, send_time = create_unload_target_path(recset.account.id, recset.id)
    product_rank_query = SKU_RANKS_BY_REGION_FOR_ACCOUNT_ID.format(algorithm=recset.algorithm,
                                                                   account_id=recset.account.id,
                                                                   lookback=recset.lookback_days,
                                                                   filter_query=filter_query)
    conn.execute(text(SNOWFLAKE_UNLOAD.format(query=product_rank_query)),
                 shard_key=get_shard_key(recset.account.id),
                 account_id=recset.account.id,
                 retailer_id=recset.retailer.id,
                 recset_id=recset.id,
                 sent_time=send_time,
                 target=unload_path,
                 filter_hash=filter_hash,
                 catalog_id=catalog_id,
                 **filter_variables)
