"""
Utilities for querying offline point of sale purchase data.
"""

from monetate.recs.models import AccountRecommendationSetting


GET_OFFLINE_PURCHASE_PER_CUSTOMER_AND_PID = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.offline_purchase_per_customer_and_pid_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
SELECT a.account_id, a.dataset_id, p.customer_id, p.product_id, p.quantity, p.currency, p.currency_unit_price, max(p.time) as fact_time
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
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING fact_time >= :begin_fact_time
"""


def get_dataset_ids_for_pos(account_ids):
    # [(account_id, dataset_id), ...]
    account_ids_dataset_ids = list(AccountRecommendationSetting.objects.filter(account_id__in=account_ids,
                                              pos_dataset_id__isnull=False).values_list("account_id", "pos_dataset_id"))
    # converts list of tuples into list
    # [account_id, dataset_id]
    flattened_aids_dids = [item for tup_list in account_ids_dataset_ids for item in tup_list]
    return flattened_aids_dids
