import hashlib
import json
from datetime import datetime, timedelta
from django.db.models import Q
from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCaseWithData
from monetate_caching.cache import invalidation_context
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
                 'market': False, 'retailer_market_scope': False, 'purchase_data_source': 'online'}
        # global level 30 day lookback
        recs2 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': True,
                 'market': False, 'retailer_market_scope': False, 'purchase_data_source': 'online'}
        # filters with 30 day lookback
        recs3 = {'filter_json': json.dumps({"type":"and","filters":[
            {"type":"startswith","left":{"type":"field","field":"id"},"right":{"type":"value","value": ["SKU-00001"]}}
        ]}), 'lookback': 30, 'global_recset': False, 'market': False, 'retailer_market_scope': False,
                 'purchase_data_source': 'online'}
        # account level 7 day lookback
        recs4 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 7, 'global_recset': False,
                 'market': False, 'retailer_market_scope': False, 'purchase_data_source': 'online'}
        # market 30 day lookback
        recs5 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': True, 'retailer_market_scope': False, 'purchase_data_source': 'online'}
        # retailer_market 30 day lookback
        recs6 = {'filter_json': json.dumps({"type": "and", "filters": []}), 'lookback': 30, 'global_recset': False,
                 'market': False, 'retailer_market_scope': True, 'purchase_data_source': 'online'}

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
                    purchase_data_source=recset['purchase_data_source']
                )
                # for global recset which is not market we need to create a row in recommendation_set_dataset table
                if rec.is_retailer_tenanted and not rec.is_market_or_retailer_driven_ds:
                    recset_dataset = dio_models.Schema.objects.create(retailer=cls.account.retailer,
                                                                      name='view_also_view')
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

    def test_30_day_view_also_view_account_level(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='view_also_view',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='view_also_view',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #     ('TP-00001', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        #     ('TP-00004', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        #     ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        # ]
        recs1_expected_result = [
            ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004',5)]),
        ]
        recs2_expected_result = [
            ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004',5)]),
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
        self._run_collab_recs_test('view_also_view', 30, recsets,
                                   expected_results, account=self.account) 

    def test_30_day_view_also_view_account_level_market(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='view_also_view',
              account=self.account,
              lookback_days=30,
              market=self.market,
              retailer_market_scope=False,
              purchase_data_source="online") |
            Q(algorithm='view_also_view',
              account=None,
              lookback_days=30,
              market=self.market,
              retailer_market_scope=False,
              purchase_data_source="online")
        )
        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #     ('TP-00001', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        #     ('TP-00004', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        #     ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        # ]
        recs5_expected_result = [
            ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004',5)]),
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
        self._run_collab_recs_test('view_also_view', 30, recsets,
                                   expected_results, market=self.market)

    def test_30_day_view_also_view_account_level_retailer_market(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='view_also_view',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=True,
              purchase_data_source="online") |
            Q(algorithm='view_also_view',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=True,
              purchase_data_source="online")
        )
        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #     ('TP-00001', [('TP-00003', 3), ('TP-00002', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        #     ('TP-00004', [('TP-00005', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00003', [('TP-00002', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        #     ('TP-00005', [('TP-00004', 2), ('TP-00003', 2), ('TP-00002', 2), ('TP-00001', 2)]),
        #     ('TP-00002', [('TP-00003', 3), ('TP-00001', 3), ('TP-00005', 2), ('TP-00004', 2)]),
        # ]
        recs5_expected_result =[
            ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
            ('TP-00001', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00003', [('SKU-00002', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004', 5)]),
            ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
            ('TP-00002', [('SKU-00003', 1), ('SKU-00001', 2), ('SKU-00006', 3), ('SKU-00005', 4), ('SKU-00004',5)]),
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
        self._run_collab_recs_test('view_also_view', 30, recsets,
                                   expected_results, account=None, market=None, retailer=self.retailer_id)

    def test_7_day_view_also_view_account_level(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='view_also_view',
              account=self.account,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='view_also_view',
              account=None,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        # commenting out as we are currently not using pid_pid output
        # pid_pid_expected_results = [
        #         ('TP-00005', [('TP-00004', 2)]),
        #         ('TP-00004', [('TP-00005', 2)]),
        # ]
        expected_results = {}
        for r in recsets:
            expected_results[r.id] = [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2)]),
                ('TP-00005', [('SKU-00004', 1)]),
            ]
        self._run_collab_recs_test('view_also_view', 7, recsets, expected_results, account=self.account)

class ViewAlsoViewFiltersTestCase(RecsTestCaseWithData):
    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(ViewAlsoViewFiltersTestCase, cls).setUpClass()

        # initializing view_also_view recsets
        # filters with 30 day lookback
        recs1 = {'filter_json': json.dumps({"type":"and","filters":[{"left":{"type":"field","field":"color"},
        "type":"in","right":{"type":"function","value":"items_from_base_recommendation_on"}}]}),
        'lookback': 30, 'global_recset': False, 'market': False, 'retailer_market_scope': False,
                 'purchase_data_source': 'online'}

        # filters with 7 day lookback
        recs2 = {'filter_json': json.dumps({"type":"and","filters":[{"left":{"type":"field","field":"brand"},"type":"not in",
        "right":{"type":"function","value":"items_from_base_recommendation_on"}},{"left":{"type":"field","field":"brand"},
        "type":"not in","right":{"type":"function","value":"items_from_base_recommendation_on"}}]}),
        'lookback': 7, 'global_recset': False, 'market': False, 'retailer_market_scope': False,
                 'purchase_data_source': 'online'}

        recsets_to_create = [recs1, recs2]
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
                    purchase_data_source=recset['purchase_data_source']
                )
                # for global recset which is not market we need to create a row in recommendation_set_dataset table
                if rec.is_retailer_tenanted and not rec.is_market_or_retailer_driven_ds:
                    recset_dataset = dio_models.Schema.objects.create(retailer=cls.account.retailer,
                                                                      name='view_also_view')
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
    
    def test_30_day_view_also_view_account_level_with_default_attribute_added_to_collab_sku_ranks_query(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='view_also_view',
              account=self.account,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='view_also_view',
              account=None,
              lookback_days=30,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        recs1_expected_result = [
            ('TP-00001', [('SKU-00002', 1)]),
            ('TP-00002', [('SKU-00001', 1)]),
            ('TP-00003', [('SKU-00004', 1)]),
            ('TP-00004', [('SKU-00003', 1)]),
        ]

        expected_results_arr = [recs1_expected_result]
        expected_results = {}
        for index, r in enumerate(recsets):
            expected_results[r.id] = expected_results_arr[index]
        self._run_collab_recs_test('view_also_view', 30, recsets,
                                   expected_results, account=self.account) 

    def test_7_day_view_also_view_account_level_with_multiple_filters_on_same_attribute(self):
        recsets = recs_models.RecommendationSet.objects.filter(
            Q(algorithm='view_also_view',
              account=self.account,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online") |
            Q(algorithm='view_also_view',
              account=None,
              lookback_days=7,
              market=None,
              retailer_market_scope=None,
              purchase_data_source="online")
        )

        expected_results = {}
        for r in recsets:
            expected_results[r.id] = [
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2)]),
                ('TP-00005', [('SKU-00004', 1)]),
            ]
        self._run_collab_recs_test('view_also_view', 7, recsets, expected_results, account=self.account)