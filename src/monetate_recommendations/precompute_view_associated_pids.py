from sqlalchemy.sql import text
import precompute_utils
from monetate.common import log, job_timing
import monetate.retailer.models as retailer_models


GET_EARLIEST_VIEW_PER_MID_AND_PID = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.earliest_view_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
WITH device_earliest_product_view AS (
    SELECT account_id, mid_epoch, mid_ts, mid_rnd, product_id, min(fact_time) fact_time
    FROM fact_product_view
    WHERE account_id in (:account_ids)
        AND fact_time >= :begin_fact_time
        /* exclude empty string to prevent empty lookup keys, filter out common invalid values to reduce join size */
        AND product_id NOT IN ('', 'null', 'NULL')
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

# account_id , market_id and retailer_id create a unique key only one variable will have a value and rest will be None
# example  6814_None_None
VIEW_ALSO_VIEW = """
CREATE TEMPORARY TABLE scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_{purchase_data_source}
AS
WITH
half_scores AS (
    /* Filter out pairs existing seen only on one device. */
    SELECT
        p1.account_id account_id,
        p1.product_id pid1,
        p2.product_id pid2,
        count(*) score 
    FROM scratch.earliest_view_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    JOIN scratch.earliest_view_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p2
        ON p1.account_id = p2.account_id
        AND p1.mid_epoch = p2.mid_epoch
        AND p1.mid_ts = p2.mid_ts
        AND p1.mid_rnd = p2.mid_rnd
        AND p1.product_id < p2.product_id  /* lower triangle */
    GROUP BY 1, 2, 3
    HAVING count(*) > 1
)
    /* create symmetric pairs */
    /* verify if performance benefit on snowflake */
    SELECT
        account_id,
        pid1,
        pid2,
        score
    FROM half_scores
    UNION ALL
    SELECT
        account_id,
        pid2 AS pid1,
        pid1 AS pid2,
        score
    FROM half_scores

"""

QUERY_DISPATCH = {
    'view_also_view': VIEW_ALSO_VIEW
}


def process_view_collab_algorithm(conn, queue_entry):
    result_counts = []
    # since the queue table currently has accounts that do not have the precompute collab feature flag
    # we don't want to process these queue entries
    if queue_entry.account and \
            not queue_entry.account.has_feature(retailer_models.ACCOUNT_FEATURES.ENABLE_COLLAB_RECS_PRECOMPUTE_MODELING):
        log.log_info("skipping results for recset group with id {} - does not have collab feature flag"
                     .format(queue_entry.id))
        return result_counts
    account = queue_entry.account.id if queue_entry.account else None
    market = queue_entry.market.id if queue_entry.market else None
    retailer = queue_entry.retailer.id if queue_entry.retailer else None
    algorithm = queue_entry.algorithm
    lookback_days = queue_entry.lookback_days
    log.log_info('Processing queue entry {}'.format(queue_entry.id))
    log.log_info("Processing algorithm {}, lookback {}".format(algorithm, lookback_days))
    account_ids = precompute_utils.get_account_ids_for_processing(queue_entry)
    # this query creates a temp table with all the purchases or views in given lookback period
    begin_fact_time, end_fact_time = precompute_utils.get_fact_time(lookback_days)
    query = text(GET_EARLIEST_VIEW_PER_MID_AND_PID.format(account_id=account, market_id=market,
                                                          retailer_id=retailer, lookback_days=lookback_days))
    conn.execute(query, account_ids=account_ids, begin_fact_time=begin_fact_time,
                 end_fact_time=end_fact_time, lookback=lookback_days)

    conn.execute(text(QUERY_DISPATCH[algorithm].format(algorithm=algorithm, account_id=account, market_id=market,
                                                       retailer_id=retailer, lookback_days=lookback_days,
                                                       purchase_data_source="online")))

    # normalize score
    conn.execute(text(precompute_utils.PID_RANKS_BY_COLLAB_RECSET.format(algorithm=algorithm, account_id=account,
                                                                         lookback_days=lookback_days, market_id=market,
                                                                         retailer_id=retailer,
                                                                         purchase_data_source="online")))

    result_counts = precompute_utils.process_collab_recsets(conn, queue_entry, account, market, retailer)

    log.log_info('Completed processing queue entry {}'.format(queue_entry.id))

    return result_counts
