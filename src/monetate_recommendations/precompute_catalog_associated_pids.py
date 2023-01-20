import json
from sqlalchemy.sql import text
import precompute_utils
from monetate_monitoring import log
import monetate.retailer.models as retailer_models
from monetate.recs.models import RecommendationSet, RecommendationSetDataset, AccountRecommendationSetting
from monetate_recommendations import supported_weights_expression
import monetate.dio.models as dio_models


# Retrieving the products only if they are available in stock for the given retailer
GET_RETAILER_PRODUCT_CATALOG = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.retailer_product_catalog_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
SELECT *
FROM product_catalog
WHERE retailer_id=:retailer_id AND dataset_id=:dataset_id AND lower(availability)=lower(:availability)
"""


SIMILAR_PRODUCTS_V2 = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days}_{purchase_data_source}
AS
WITH truth_table as (SELECT pc1.item_group_id as pid1, pc2.item_group_id as pid2,
                      {weights}
                     FROM scratch.retailer_product_catalog_{account_id}_{market_id}_{retailer_id}_{lookback_days} pc1
                     JOIN scratch.retailer_product_catalog_{account_id}_{market_id}_{retailer_id}_{lookback_days} pc2 ON
                     (pc1.retailer_id = pc2.retailer_id AND
                     pc1.item_group_id != pc2.item_group_id))
SELECT {account_id} as account_id, pid1, pid2, {selected_attributes} AS Score FROM truth_table
"""

QUERY_DISPATCH = {
    'similar_products_v2': SIMILAR_PRODUCTS_V2
}

def get_similar_products_weights(account, market, retailer, lookback_days):
    recommendation_settings = AccountRecommendationSetting.objects.filter(account_id=account)
    weights_json = json.loads(recommendation_settings[0].similar_product_weights_json) if recommendation_settings else None
    if weights_json is None:
        raise ValueError('Account {} has no weights JSON for similar products execution, using None'.format(account))
    weights_json = weights_json["enabled_catalog_attributes"]
    catalog_id = dio_models.DefaultAccountCatalog.objects.get(account=account).schema.id
    weights_sql, selected_attributes = supported_weights_expression.get_weights_query(weights_json, catalog_id, \
                                                                                      account, market, retailer,
                                                                                      lookback_days)
    return weights_sql, selected_attributes


def process_catalog_collab_algorithm(conn, queue_entry):
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

    if account is None:
        raise ValueError('Account has no Account ID for similar products execution, using None')
    retailer_id = queue_entry.account.retailer_id
    dataset_id = dio_models.DefaultAccountCatalog.objects.get(account=queue_entry.account.id).schema.id
    # TODO: availability is Adidas specific, need to change in the future to support all clients
    availability = "In Stock"
    conn.execute(text(GET_RETAILER_PRODUCT_CATALOG.format(account_id=account, market_id=market,
                                                          retailer_id=retailer, lookback_days=lookback_days)),
                 retailer_id=retailer_id, dataset_id=dataset_id, availability=availability)

    weights_sql, selected_attributes = get_similar_products_weights(account, market, retailer, lookback_days)
    conn.execute(text(QUERY_DISPATCH[algorithm].format(algorithm=algorithm, account_id=account, market_id=market,
                                                       retailer_id=retailer, lookback_days=lookback_days,
                                                       weights=weights_sql, selected_attributes=selected_attributes,
                                                       purchase_data_source="online")))

    # normalize score
    conn.execute(text(precompute_utils.PID_RANKS_BY_COLLAB_RECSET.format(algorithm=algorithm, account_id=account,
                                                                         lookback_days=lookback_days, market_id=market,
                                                                         retailer_id=retailer,
                                                                         purchase_data_source="online")))

    result_counts = precompute_utils.process_collab_recsets(conn, queue_entry, account, market, retailer)

    log.log_info('Completed processing queue entry {}'.format(queue_entry.id))

    return result_counts
