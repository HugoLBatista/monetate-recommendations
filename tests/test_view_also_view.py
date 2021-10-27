import hashlib
import json
from datetime import datetime, timedelta

from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCaseWithData
from monetate.retailer.cache import invalidation_context
import monetate.recs.models as recs_models
import monetate.dio.models as dio_models

class ViewAlsoViewTestCase(RecsTestCaseWithData):
    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(ViewAlsoViewTestCase, cls).setUpClass()

        # initializing view_also_view recsets
        # account level 30 day lookback
        recs1 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False}
        # global level 30 day lookback
        recs2 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': True,
                 'market': False, 'retailer_market_scope': False}
        # filters with 30 day lookback
        recs3 = {'filter_json': json.dumps({"type":"and","filters":[
            {"type":"in","left":{"type":"field","field":"id"},"right":{"type":"value","value":"SKU-00001"}}
        ]}),'lookback': 30, 'global_recset': False, 'market': False, 'retailer_market_scope': False}
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
                    algorithm='view_also_view',
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

    def test_30_day_view_also_view_account_level(self):

        self._run_collab_recs_test('view_also_view', 30, account=self.account)

    def test_7_day_view_also_view_account_level(self):
        pass

    """def test_30_day_view_also_view_account_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4),('SKU-00001', 5)]),
                ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),

            ],
            market=False
        )

    def test_30_day_view_also_view_account_level_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4),('SKU-00001', 5)]),
                ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),

            ],
            market=True,
        )

    def test_30_day_view_also_view_account_level_retailer_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4),('SKU-00001', 5)]),
                ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),

            ],
            market=False,
            retailer_market_scope=True
        )

    def test_30_day_view_also_view_global_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr=[
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
                ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),

            ],
            global_recset=True
        )

    def test_30_day_view_also_view_global_level_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr=[
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
                ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),

            ],
            market=True,
            global_recset=True
        )

    def test_30_day_view_also_view_global_level_retauler_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr=[
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
                ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),

            ],
            market=False,
            retailer_market_scope=True
        )

    def test_7_view_also_view_day_account_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=7,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2)]),
                ('TP-00005', [('SKU-00004', 1)]),

            ],
            market=False,
        )

    def test_7_day_global_level(self):
        pass

    def test_30_day_view_also_view_recset_level_filters(self):
        filter_json = json.dumps({"type":"and","filters":[{"type":"in","left":{"type":"field","field":"id"},"right":{"type":"value","value":"SKU-00001"}}]})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4),('SKU-00001', 5)]),
                ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),

            ],
        )
"""

