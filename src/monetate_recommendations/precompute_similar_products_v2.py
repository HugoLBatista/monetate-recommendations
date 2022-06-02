from django.conf import settings
import contextlib
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from monetate.common import job_timing, log
from monetate_recommendations import precompute_utils

log.configure_script_log('precompute_PAP_algorithm')
#TODO: update the join on product catalog, we are multiplying our counts with the skus
SIMILAR_PRODUCTS_V2 = """
CREATE TEMPORARY TABLE IF NOT EXISTS scratch.{algorithm}_{account_id}_{market_id}_{retailer_id}_{lookback_days} AS
WITH truth_table as (SELECT pc1.item_group_id as pid1, pc2.item_group_id as pid2,
                      {weights}
                     FROM scratch.retailer_product_catalog_{account_id}_{market_id}_{retailer_id}_{lookback_days} pc1
                     JOIN scratch.retailer_product_catalog_{account_id}_{market_id}_{retailer_id}_{lookback_days} pc2 ON
                     (pc1.retailer_id = pc2.retailer_id AND
                     pc1.item_group_id != pc2.item_group_id))
SELECT {account_id} as account_id, pid1, pid2, {selected_attributes} AS Score FROM truth_table
"""


def precompute_similar_products_v2(recsets_group):

    return precompute_utils.initialize_process_collab_algorithm(recsets_group, 'similar_products_v2',
                                                                SIMILAR_PRODUCTS_V2,
                                                                precompute_utils.GET_RETAILER_PRODUCT_CATALOG)