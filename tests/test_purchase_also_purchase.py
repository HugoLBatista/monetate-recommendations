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


class PurchaseAlsoPurchase(RecsTestCaseWithData):
    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(PurchaseAlsoPurchase, cls).setUpClass()

        # initializing purchase_also_purchase recsets
        # account level 30 day lookback
        recs1 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False}
        # global level 30 day lookback
        recs2 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': True,
                 'market': False, 'retailer_market_scope': False}
        # filters with 30 day lookback
        recs3 = {'filter_json': json.dumps({"type": "and", "filters": [
            {"type": "in", "left": {"type": "field", "field": "id"}, "right": {"type": "value", "value": "SKU-00001"}}
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
                )
                # add to queue if not already in queue
                recs_models.PrecomputeQueue.objects.get_or_create(
                    account=cls.set_account(rec, cls.account) if rec.is_retailer_tenanted
                    else cls.set_account(rec),
                    market=rec.market,
                    retailer=rec.retailer if rec.retailer_market_scope else None,
                    algorithm=rec.algorithm,
                    lookback_days=rec.lookback_days,
                )

    def set_account(cls, recset, account=None):
        # anytime a recset has a market, account_id should be None
        if recset.is_market_or_retailer_driven_ds:
            return None
        # if not market and not retailer level, return account_id from RecommendationSet table
        elif not recset.is_retailer_tenanted:
            return recset.account
        # if not market but retailer level, return the account_id of current account
        return account

    def test_30_day_purchase_also_purchase_account_level(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=None) |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=None)
        )
        print([r.id for r in recsets])
        pid_pid_expected_results = [
            ('TP-00001', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2)]),
            ('TP-00004', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
            ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        ]
        recs1_expected_result = [
            ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00001', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00003', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
            ('TP-00005', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
            ('TP-00002', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
        ]
        recs2_expected_result = [
            ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00001', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
            ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
        ]
        recs3_expected_result = [
            ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00001', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
            ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
        ]
        expected_results_arr = [recs1_expected_result, recs2_expected_result, recs3_expected_result]
        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('purchase_also_purchase', 30, recsets, pid_pid_expected_results,
                                   expected_results, account=self.account)

    def test_30_day_purchase_also_purchase_account_level_market(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=30,
              market=True,
              retailer_market_scope=0) |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=30,
              market=True,
              retailer_market_scope=0)
        )
        pid_pid_expected_results = [
            ('TP-00001', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2)]),
            ('TP-00004', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
            ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        ]
        recs5_expected_result = [
            ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00001', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
            ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
        ]
        expected_results_arr = [recs5_expected_result]

        expected_results = {}
        for r in recsets:
            expected_results[r.id] = [
                ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
                ('TP-00001', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
                ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
                ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
                ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ]
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('purchase_also_purchase', 30, recsets, pid_pid_expected_results,
                                   expected_results, market=self.market)

    def test_30_day_purchase_also_purchase_account_level_retailer_market(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=1) |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=1)
        )
        pid_pid_expected_results = [
            ('TP-00001', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2)]),
            ('TP-00004', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
            ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        ]
        recs5_expected_result = [
            ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00001', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
            ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
            ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
        ]
        expected_results_arr = [recs5_expected_result]
        expected_results = {}
        for r in recsets:
            expected_results[r.id] = [
                ('TP-00004', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
                ('TP-00001', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2), ('TP-00006')]),
                ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
                ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
                ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2), ('TP-00006')]),
            ]
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('purchase_also_purchase', 30, recsets, pid_pid_expected_results,
                                   expected_results, account=None, market=None, retailer=1)

    def test_7_day_purchase_also_purchase_account_level(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='purchase_also_purchase',
              account=self.account,
              lookback_days=7,
              market=None,
              retailer_market_scope=None) |
            Q(algorithm='purchase_also_purchase',
              account=None,
              lookback_days=7,
              market=None,
              retailer_market_scope=None)
        )
        # print([r.id for r in recsets])
        pid_pid_expected_results = [
            ('TP-00005', [('TP-00004', 2)]),
            ('TP-00004', [('TP-00005', 2)]),
        ]
        expected_results = {}
        for r in recsets:
            expected_results[r.id] = [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2)]),
                ('TP-00005', [('TP-00004', 1)]),
            ]
        self._run_collab_recs_test('purchase_also_purchase', 7, recsets, pid_pid_expected_results, expected_results,
                                   account=self.account)

    """def test_30_day_purchase_also_purchase_account_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=False
        )

    def test_30_day_purchase_also_purchase_account_level_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=True
        )

    def test_30_day_purchase_also_purchase_account_level_retailer_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=False,
            retailer_market_scope=True
        )

    def test_30_day_purchase_also_purchase_global_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            global_recset=True
        )

    def test_30_day_purchase_also_purchase_global_level_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=True,
            global_recset=True
        )

    def test_30_day_purchase_also_purchase_global_level_retauler_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=False,
            global_recset=True
        )

    def test_7_purchase_also_purchase_day_account_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=7,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3)]),
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),
                ('TP-00005', [('SKU-00004', 1)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),

            ],
            market=False
        )

    def test_7_purchase_also_purchase_global_level(self):
        pass

    def test_30_day_purchase_also_purchasew_recset_level_filters(self):
        pass"""


