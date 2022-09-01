import hashlib
import json
from datetime import datetime, timedelta
from django.db.models import Q
from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCaseWithData
from monetate.retailer.cache import invalidation_context
import monetate.recs.models as recs_models
import monetate.dio.models as dio_models


class PurchaseAlsoPurchaseTestCase(RecsTestCaseWithData):
    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(PurchaseAlsoPurchaseTestCase, cls).setUpClass()

        # initializing purchase_also_purchase recsets
        # account level 30 day lookback
        recs1 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False}
        # global level 30 day lookback
        recs2 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': True,
                 'market': False, 'retailer_market_scope': False}
        # filters with 30 day lookback
        recs3 = {'filter_json': json.dumps({"type": "and", "filters": [
            {"type": "startswith", "left": {"type": "field", "field": "id"},
             "right": {"type": "value", "value": ["SKU-00001"]}}
        ]}), 'lookback': 30, 'global_recset': False, 'market': False, 'retailer_market_scope': False}
        # account level 7 day lookback
        recs4 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 7, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False}
        # market 30 day lookback
        recs5 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': True, 'retailer_market_scope': False}
        # retailer_market 30 day lookback
        recs6 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': False, 'retailer_market_scope': True}

        recsets_to_create = [recs1, recs2, recs3, recs4, recs5, recs6]

        with invalidation_context():
            for recset in recsets_to_create:
                rec = recs_models.RecommendationSet.objects.create(
                    algorithm='purchase_also_purchase',
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
                    purchase_data_source='online'
                )
                # for global recset which is not market we need to create a row in recommendation_set_dataset table
                if rec.is_retailer_tenanted and not rec.is_market_or_retailer_driven_ds:
                    recset_dataset = dio_models.Schema.objects.create(retailer=cls.account.retailer,
                                                                      name='purchase_also_purchase')
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
                    purchase_data_source='online'
                )


    def test_30_day_purchase_also_purchase_account_level(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #     ('TP-00001', [('TP-00004', 3), ('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2)]),
        #     ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        #     ('TP-00003', [('TP-00004', 3), ('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        #     ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00002', [('TP-00004', 3), ('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        # ]
        recs1_expected_result = [
            ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
        ]
        recs2_expected_result = [
            ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
        ]
        recs3_expected_result = [
            ('TP-00002', [('SKU-00001', 1)]),
            ('TP-00005', [('SKU-00001', 1)]),
            ('TP-00003', [('SKU-00001', 1)]),
            ('TP-00004', [('SKU-00001', 1)]),
        ]

        expected_results_arr = [recs1_expected_result, recs2_expected_result, recs3_expected_result]
        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('purchase_also_purchase', 30, recsets,
                                   expected_results, account=self.account)

    def test_30_day_purchase_also_purchase_account_level_market(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=30,
              market=self.market,
              retailer_market_scope=False,
              purchase_data_source="online") |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=30,
              market=self.market,
              retailer_market_scope=False,
              purchase_data_source="online")
        )
        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #     ('TP-00001', [('TP-00004', 3), ('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2)]),
        #     ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        #     ('TP-00003', [('TP-00004', 3), ('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        #     ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00002', [('TP-00004', 3), ('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        # ]
        recs5_expected_result = [
            ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
        ]
        expected_results_arr = [recs5_expected_result]

        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('purchase_also_purchase', 30, recsets,
                                   expected_results, market=self.market)

    def test_30_day_purchase_also_purchase_account_level_retailer_market(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=True,
              purchase_data_source="online") |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=True,
              purchase_data_source="online")
        )
        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #     ('TP-00001', [('TP-00004', 3), ('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2)]),
        #     ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        #     ('TP-00003', [('TP-00004', 3), ('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        #     ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00002', [('TP-00004', 3), ('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2)]),
        # ]
        recs5_expected_result =[
            ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
        ]
        expected_results_arr = [recs5_expected_result]
        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('purchase_also_purchase', 30, recsets,
                                   expected_results, account=None, market=None, retailer=self.retailer_id)

    def test_7_day_purchase_also_purchase_account_level(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #     ('TP-00001', [('TP-00004', 1), ('TP-00003', 1), ('TP-00002', 1)]),
        #     ('TP-00004', [('TP-00005', 2), ('TP-00003', 1), ('TP-00002', 1), ('TP-00001', 1)]),
        #     ('TP-00003', [('TP-00002', 2), ('TP-00004', 1), ('TP-00001', 1)]),
        #     ('TP-00005', [('TP-00004', 2)]),
        #     ('TP-00002', [('TP-00003', 2), ('TP-00004', 1), ('TP-00001', 1)]),
        # ]
        expected_results = {}
        for r in recsets:
            expected_results[r.id] = [
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3)]),
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),
                ('TP-00005', [('SKU-00004', 1)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),
            ]
        self._run_collab_recs_test('purchase_also_purchase', 7, recsets, expected_results,
                                   account=self.account)

        def test_7_day_purchase_also_purchase_account_level_online_offline_pos(self):
            recsets = recs_models.RecommendationSet.objects.filter(
                Q(algorithm='purchase_also_purchase',
                  account=self.account,
                  lookback_days=30,
                  market=None,
                  retailer_market_scope=None,
                  purchase_data_source="online_offline") |
                Q(algorithm='purchase_also_purchase',
                  account=None,
                  lookback_days=30,
                  market=None,
                  retailer_market_scope=None,
                  purchase_data_source="online_offline")
            )

            expected_results = {}
            for r in recsets:
                expected_results[r.id] = [
                    ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3)]),
                    ('TP-00004',
                     [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
                    ('TP-00003', [('SKU-00002', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),
                    ('TP-00005', [('SKU-00004', 1)]),
                    ('TP-00002', [('SKU-00003', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),
                ]
            self._run_collab_recs_test('purchase_also_purchase', 7, recsets, expected_results,
                                       account=self.account)

