from sqlalchemy.sql import text
import precompute_utils
from monetate.common import log, job_timing
import monetate.retailer.models as retailer_models
from monetate.recs.models import AccountRecommendationSetting

MIN_PURCHASE_THRESHOLD = 3

# account_id , market_id and retailer_id create a unique key only one variable will have a value and rest will be None
# example  6814_None_None
GET_ONLINE_LAST_PURCHASE_PER_MID_AND_PID = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.last_purchase_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
SELECT account_id, mid_epoch, mid_ts, mid_rnd, product_id, max(fact_time) as fact_time
FROM m_dedup_purchase_line
WHERE account_id in (:account_ids)
    AND fact_time >= :begin_fact_time
    /* exclude empty string to prevent empty lookup keys, filter out common invalid values to reduce join size */
    AND product_id NOT IN ('', 'null', 'NULL')
GROUP BY 1, 2, 3, 4, 5
"""


GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
SELECT a.account_id, a.dataset_id, p.customer_id, p.product_id, max(p.time) as fact_time
FROM (
    SELECT t.account_id, t.dataset_id, d.cutoff_time
    FROM (VALUES (:aids_dids)) t(account_id, dataset_id)
    JOIN config_dataset_data_expiration d
    ON t.dataset_id = d.dataset_id
) a
JOIN dio_purchase p
ON p.dataset_id = a.dataset_id
    AND p.update_time >= a.cutoff_time
    /* exclude empty string to prevent empty lookup keys, filter out common invalid values to reduce join size */
    AND product_id NOT IN ('', 'null', 'NULL')
GROUP BY 1, 2, 3, 4
HAVING fact_time >= :begin_fact_time
"""


ONLINE_OFFLINE_PAP_QUERY = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_{purchase_data_source} AS
SELECT
    account_id,
    pid1,
    pid2,
    sum(score) score
FROM (
    SELECT
        account_id,
        pid1,
        pid2,
        count(*) score
    FROM scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_online
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT
        account_id,
        pid1,
        pid2,
        count(*) score
    FROM scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_offline
    GROUP BY 1, 2, 3
   )
GROUP BY 1, 2, 3
"""


ONLINE_PAP_QUERY = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_online AS
    SELECT
        p1.account_id account_id,
        p1.product_id pid1,
        p2.product_id pid2,
        count(*) score
    FROM scratch.last_purchase_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    JOIN scratch.last_purchase_per_mid_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p2
        ON p1.account_id = p2.account_id
        AND p1.mid_epoch = p2.mid_epoch
        AND p1.mid_ts = p2.mid_ts
        AND p1.mid_rnd = p2.mid_rnd
        AND p1.product_id != p2.product_id
    GROUP BY 1, 2, 3
    HAVING count(*) >= :minimum_count
"""


OFFLINE_PAP_QUERY = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_offline AS
    SELECT
        p1.account_id account_id,
        p1.product_id pid1,
        p2.product_id pid2,
        count(*) score
    FROM scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p1
    JOIN scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} p2
        ON p1.dataset_id = p2.dataset_id
        AND p1.customer_id = p2.customer_id
        AND p1.product_id != p2.product_id
    GROUP BY 1, 2, 3
    HAVING count(*) >= :minimum_count
"""

# TODO: Refactor online_offline to call online and offline query
PAP_QUERY_DISPATCH = {
    "online_offline": ONLINE_OFFLINE_PAP_QUERY,
    "online": ONLINE_PAP_QUERY,
    "offline": OFFLINE_PAP_QUERY,
}

# TODO: Refactor online_offline to call online and offline helper queries
SOURCE_DATA_DISPATCH = {
    'online': GET_ONLINE_LAST_PURCHASE_PER_MID_AND_PID,
    'offline': GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID,
    # 'online_offline': GET_ONLINE_LAST_PURCHASE_PER_MID_AND_PID + GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID
}


def get_dataset_ids_for_pos(account_ids):
    # [(account_id, dataset_id), ...]
    account_ids_dataset_ids = list(AccountRecommendationSetting.objects.filter(account_id__in=account_ids,
                                              pos_dataset_id__isnull=False).values_list("account_id", "pos_dataset_id"))
    # converts list of tuples into list
    # [account_id, dataset_id]
    flattened_aids_dids = [item for tup_list in account_ids_dataset_ids for item in tup_list]
    return flattened_aids_dids


def run_pap_main_and_helper_queries(account, account_ids, market, retailer, lookback_days, algorithm,
                                    purchase_data_source, begin_fact_time, account_ids_dataset_ids, min_count, conn):
    if purchase_data_source == "online_offline":
        # run all queries
        # execute both online and offline helper queries
        conn.execute(text(GET_ONLINE_LAST_PURCHASE_PER_MID_AND_PID.format(account_id=account, market_id=market,
                                                                          retailer_id=retailer,
                                                                          lookback_days=lookback_days)),
                     account_ids=account_ids,
                     begin_fact_time=begin_fact_time)
        conn.execute(text(GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID.format(account_id=account, market_id=market,
                                                                           retailer_id=retailer,
                                                                           lookback_days=lookback_days)),
                     account_ids=account_ids,
                     begin_fact_time=begin_fact_time, aids_dids=account_ids_dataset_ids)
        # execute both online and offline pap queries and then aggregate results into final union query
        conn.execute(text(PAP_QUERY_DISPATCH["online"].
                          format(algorithm=algorithm, account_id=account, market_id=market, retailer_id=retailer,
                                 lookback_days=lookback_days)), minimum_count=min_count)
        conn.execute(text(PAP_QUERY_DISPATCH["offline"].
                          format(algorithm=algorithm, account_id=account, market_id=market, retailer_id=retailer,
                                 lookback_days=lookback_days)), minimum_count=min_count)
        conn.execute(text(PAP_QUERY_DISPATCH[purchase_data_source].
                          format(algorithm=algorithm, account_id=account, market_id=market, retailer_id=retailer,
                                 lookback_days=lookback_days, purchase_data_source=purchase_data_source)),
                     minimum_count=min_count)
    # offline only or online only
    else:
        conn.execute(text(SOURCE_DATA_DISPATCH[purchase_data_source].
                          format(account_id=account, market_id=market, retailer_id=retailer, lookback_days=lookback_days)),
                     begin_fact_time=begin_fact_time, account_ids=account_ids, aids_dids=account_ids_dataset_ids)
        conn.execute(text(PAP_QUERY_DISPATCH[purchase_data_source].format(algorithm=algorithm, account_id=account,
                     market_id=market, retailer_id=retailer, lookback_days=lookback_days)), minimum_count=min_count)


def process_purchase_collab_algorithm(conn, queue_entry):
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
    purchase_data_source = queue_entry.purchase_data_source
    log.log_info('Processing queue entry {}'.format(queue_entry.id))
    log.log_info("Processing algorithm {}, lookback {}".format(algorithm, lookback_days))
    # get account ids based on whether the queue entry corresponds to a markets/retailer/account level recset
    account_ids = precompute_utils.get_account_ids_for_processing(queue_entry)
    begin_fact_time, end_fact_time = precompute_utils.get_fact_time(lookback_days)
    min_count = MIN_PURCHASE_THRESHOLD \
        if queue_entry.account \
           and queue_entry.account.has_feature(retailer_models.ACCOUNT_FEATURES.MIN_THRESHOLD_FOR_PAP_FBT) else 1
    # get account ids, dataset ids for pos [(account_id, dataset_id)...]
    account_ids_dataset_ids = get_dataset_ids_for_pos(account_ids)
    if not account_ids_dataset_ids:
        log.log_info("Account/s {} has/have no pos dataset ids".format(account_ids))
    # TODO: Do we need to fail when list above is empty??
    run_pap_main_and_helper_queries(account, account_ids, market, retailer, lookback_days, algorithm,
                                    purchase_data_source, begin_fact_time, account_ids_dataset_ids, min_count, conn)
    # normalize score
    conn.execute(text(precompute_utils.PID_RANKS_BY_COLLAB_RECSET.format(algorithm=algorithm, account_id=account,
                                                 lookback_days=lookback_days, market_id=market, retailer_id=retailer,
                                                 purchase_data_source=queue_entry.purchase_data_source)))
    result_counts = precompute_utils.process_collab_recsets(conn, queue_entry, account, market, retailer)

    log.log_info('Completed processing queue entry {}'.format(queue_entry.id))
    return result_counts
