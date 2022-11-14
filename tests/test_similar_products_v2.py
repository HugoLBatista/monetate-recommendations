import hashlib
import json
import mock
from datetime import datetime, timedelta
from django.db.models import Q
from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCaseWithData
from monetate.retailer.cache import invalidation_context
import monetate.recs.models as recs_models
import monetate.dio.models as dio_models

class SimilarProductsV2TestCase(RecsTestCaseWithData):
    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(SimilarProductsV2TestCase, cls).setUpClass()

        # initializing similar_products_v2 recsets
        # account level 30 day lookback
        recs1 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False, 'purchase_data_source': "online"}
        # filters with 30 day lookback
        recs2 = {'filter_json': json.dumps({"type": "and", "filters": [
            {"type": "startswith", "left": {"type": "field", "field": "id"},
             "right": {"type": "value", "value": ["SKU-00001"]}}
        ]}), 'lookback': 30, 'global_recset': False, 'market': False, 'retailer_market_scope': False,
                 'purchase_data_source': "online"}
        # account level 7 day lookback
        recs3 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 7, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False, 'purchase_data_source': "online"}
        # account level 2 day lookback
        recs4 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 2, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False, 'purchase_data_source': "online"}

        recsets_to_create = [recs1, recs2, recs3, recs4]
        with invalidation_context():
            for recset in recsets_to_create:
                rec = recs_models.RecommendationSet.objects.create(
                    algorithm='similar_products_v2',
                    account=None if recset['global_recset'] else cls.account,
                    lookback_days=recset['lookback'],
                    filter_json=recset['filter_json'],
                    retailer=cls.account.retailer,
                    base_recommendation_on="none",
                    geo_target="none",
                    name="test",
                    order="algorithm",
                    version=1,
                    product_catalog=dio_models.Schema.objects.get(id=cls.product_catalog_id),
                    retailer_market_scope=cls._setup_retailer_market(recset['retailer_market_scope'], recset['market']),
                    market=cls._setup_market(recset['market']),
                    purchase_data_source=recset['purchase_data_source']
                )
                # for global recset which is not market we need to create a row in recommendation_set_dataset table
                if rec.is_retailer_tenanted and not rec.is_market_or_retailer_driven_ds:
                    recset_dataset = dio_models.Schema.objects.create(retailer=cls.account.retailer,
                                                                      name='similar_products_v2')
                    recs_models.RecommendationSetDataset.objects.create(
                        recommendation_set_id=rec.id,
                        dataset_id=recset_dataset.id,
                        account_id=cls.account.id
                    )
                # add to queue if not already in queue
                recs_models.PrecomputeQueue.objects.get_or_create(
                    account=cls.set_account(rec, cls.account) if rec.is_retailer_tenanted
                    else cls.set_account(rec),
                    market=rec.market,
                    retailer=rec.retailer if rec.retailer_market_scope else None,
                    algorithm=rec.algorithm,
                    lookback_days=rec.lookback_days,
                    purchase_data_source="online"
                )

    def test_default_attribute_weights_enabled_similar_products_v2(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='similar_products_v2',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='similar_products_v2',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        similar_product_weights_json=json.dumps({"enabled_catalog_attributes":
                                                         [{"catalog_attribute": "color", "weight": 0.5},
                                                          {"catalog_attribute": "product_type", "weight": 0.1},
                                                          ]})

        recs1_expected_result = [
            ('TP-00004', [('SKU-00003', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00001', [('SKU-00002', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00004', 4), ('SKU-00003', 5)]),
            ('TP-00003', [('SKU-00004', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00001', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00004', 4), ('SKU-00003', 5)]),
        ]
        recs2_expected_result = [
            ('TP-00002', [('SKU-00001', 1)]),
            ('TP-00005', [('SKU-00001', 1)]),
            ('TP-00003', [('SKU-00001', 1)]),
            ('TP-00004', [('SKU-00001', 1)]),
        ]
        expected_results_arr = [recs1_expected_result,recs2_expected_result]
        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('similar_products_v2', 30, recsets,
                                   expected_results, account=self.account,
                                   similar_product_weights_json=similar_product_weights_json)

    def test_custom_attribute_weights_enabled_similar_products_v2(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='similar_products_v2',
              account=self.account,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='similar_products_v2',
              account=None,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        similar_product_weights_json=json.dumps({"enabled_catalog_attributes":
                                                         [{"catalog_attribute": "product_category", "weight": 0.5}]})

        recs3_expected_result = [
            ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
        ]
        expected_results_arr = [recs3_expected_result]
        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('similar_products_v2', 7, recsets,
                                   expected_results, account=self.account,
                                   similar_product_weights_json=similar_product_weights_json)

    def test_default_attribute_weights_not_enabled_similar_products_v2(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='similar_products_v2',
              account=self.account,
              lookback_days=2,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='similar_products_v2',
              account=None,
              lookback_days=2,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        similar_product_weights_json=json.dumps({"enabled_catalog_attributes":
                                                         [{"catalog_attribute": "color"},
                                                          {"catalog_attribute": "product_type"},
                                                          ]})

        recs4_expected_result = [
            ('TP-00004', [('SKU-00003', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00001', [('SKU-00002', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00004', 4), ('SKU-00003', 5)]),
            ('TP-00003', [('SKU-00004', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00001', 1), ('SKU-00006', 2), ('SKU-00005', 3), ('SKU-00004', 4), ('SKU-00003', 5)]),
        ]
        expected_results_arr = [recs4_expected_result]
        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('similar_products_v2', 2, recsets,
                                   expected_results, account=self.account,
                                   similar_product_weights_json=similar_product_weights_json)
