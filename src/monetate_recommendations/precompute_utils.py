from django.conf import settings
import os
import datetime
import hashlib
import json
from collections import OrderedDict
from monetate.common.warehouse import sqlalchemy_warehouse
from monetate.common.warehouse.sqlalchemy_snowflake import normalized_indent_text

SNOWFLAKE_UNLOAD = '''
COPY
INTO :target
FROM (
{query}
)
SINGLE=TRUE
MAX_FILE_SIZE=1000000000
'''

# SKU ranking query used by view, purchase, and purchase_value algorithms
SKU_RANKS_BY_REGION_FOR_ACCOUNT_ID = """
WITH
pid_algo AS (
    /* Aggregates per product_id for an account at appropriate geo_rollup level */
    SELECT
        :dataset_hash AS lookup_key,
        product_id,
        SUM(subtotal) AS score
    FROM scratch.{algorithm}_{account_id}_{lookback}
    GROUP BY 1, 2
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
                MAX(c.id) AS id
            FROM product_catalog c
            JOIN config_dataset_data_expiration e
                ON c.dataset_id = e.dataset_id
            WHERE c.dataset_id = :catalog_id
                AND c.update_time >= e.cutoff_time
                {filter_query}
            GROUP BY 1, 2, 3
        )
    )
    WHERE ordinal <= 50
),
sku_algo AS (
    /* Explode recommended product ids into recommended representative skus */
    SELECT
        pid_algo.lookup_key,
        c.id,
        pid_algo.score
    FROM pid_algo
    JOIN reduced_catalog c
        ON c.item_group_id = pid_algo.product_id
)
/* Convert scores into ordinals, limit to top 1000 per lookup ley for later post filtering */
SELECT
    lookup_key,
    id,
    ordinal
FROM (
    SELECT
        lookup_key,
        id,
        ROW_NUMBER() OVER (PARTITION by lookup_key ORDER BY score DESC, id) AS ordinal
    FROM sku_algo
)
WHERE ordinal <= 1000
"""


def parse_product_type_filter(filter_json):
    def _filter_product_type(f):
        is_product_type = f['left']['field'] == 'product_type'
        is_dynamic = f['right']['type'] == 'function'
        return is_product_type and not is_dynamic

    filter_dict = json.loads(filter_json)
    filter_dict['filters'] = filter(_filter_product_type, filter_dict['filters'])
    return filter_dict


def get_filter_hash(filter_json):
    return hashlib.sha1(str(filter_json)).hexdigest()


def create_metric_table(conn, account_id, lookback, query):
    begin_fact_time = datetime.datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=lookback)
    end_fact_time = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    begin_session_time, end_session_time = sqlalchemy_warehouse.get_session_time_bounds(
        begin_fact_time, end_fact_time)
    conn.execute(query, account_id=account_id, begin_fact_time=begin_fact_time, end_fact_time=end_fact_time,
                 begin_session_time=begin_session_time, end_session_time=end_session_time)


def create_unload_target_paths(recset_id):
    """
    Format suffix for target path for Snowflake unload.
    Not inlined so unit tests can get a replicable path.
    :param schema_id: recommendation dataset id for unload
    :param dt: unload datetime
    :return: path suffixes (i.e. the target without the @ stage part) with no leading slash for
    """
    # If you're looking for these in s3 and don't see them: dio s3_manager moves files out of the /direct/ dir as it
    # sees them into the retailer-* directories.
    stage = getattr(settings, 'RECO_DIO_SNOWFLAKE_STAGE', '@reco_dio_stage_v1')
    datetime_str = '{dt:%y/%m/%d/%H/%M/%S}'.format(dt=datetime.datetime.now())
    unload_suffix, manifest_suffix = 'unloaded_data.csv.gz', 'manifest'
    target_path = os.path.join(stage, repr(recset_id).encode('utf-8'), 'full', datetime_str, 'csv')
    return os.path.join(target_path, unload_suffix), os.path.join(target_path, manifest_suffix)


def unload_manifest(conn, source, target):
    """
    The DIO S3 manager expects redshift style manifest files.
    """
    conn.execute(normalized_indent_text('''
        LIST :source
        '''), source=source)

    conn.execute(normalized_indent_text('''
        COPY /* unload manifest */
        INTO :target
        FROM (
            SELECT
                object_construct(
                    'entries', array_construct(
                        object_construct(
                            'url', list."name",
                            'meta', object_construct('content_length', list."size")
                        )
                    )
                )
            FROM table(result_scan(last_query_id())) AS list
        )
        FILE_FORMAT = (TYPE = JSON, COMPRESSION = NONE)
        SINGLE = TRUE
        '''), target=target)
