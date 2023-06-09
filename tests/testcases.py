import json
import mock
import monetate.dio.models as dio_models
import monetate.recs.models as recs_models
import monetate.retailer.models as retailer_models
import monetate.test.testcases
import monetate_s3.s3_filereader2 as s3_filereader2
import os
import random
from datetime import datetime, timedelta
from django.utils import timezone
from monetate.common.warehouse.sqlalchemy_snowflake import get_stage_s3_uri_prefix
from monetate.market.models import Market, MarketAccount
from monetate.test.testcases import SnowflakeTestCase
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from monetate_caching.cache import invalidation_context

from monetate_recommendations import precompute_utils
from monetate_recommendations.precompute_algo_map import FUNC_MAP
from monetate_recommendations.precompute_collab_algo_map import initialize_collab_algorithm
from monetate_recommendations.precompute_utils import get_account_ids_for_market_driven_recsets
from . import warehouse_utils
from .patch import patch_invalidations

# Duct tape fix for running SnowflakeTestCase in monetate_recommendations.
# TODO: Update monetate.test.testcases to check relative paths for both source or package.
root_dir = os.path.dirname(monetate.__file__)
monetate.test.testcases.snowflake_schema_path = os.path.join(root_dir, 'snowflake', 'tables', 'public')
monetate.test.testcases.snowflake_functions_schema_path = os.path.join(root_dir, 'snowflake', 'functions')

catalog_fields = [{'name': 'id', 'data_type': 'STRING'},
                  {'name': 'title', 'data_type': 'STRING'},
                  {'name': 'description', 'data_type': 'STRING'},
                  {'name': 'link', 'data_type': 'STRING'},
                  {'name': 'image_link', 'data_type': 'STRING'},
                  {'name': 'additional_image_link', 'data_type': 'STRING'},
                  {'name': 'mobile_link', 'data_type': 'STRING'},
                  {'name': 'availability', 'data_type': 'STRING'},
                  {'name': 'availability_date', 'data_type': 'DATETIME'},
                  {'name': 'expiration_date', 'data_type': 'DATETIME'},
                  {'name': 'price', 'data_type': 'NUMBER'},
                  {'name': 'sale_price', 'data_type': 'NUMBER'},
                  {'name': 'sale_price_effective_date_begin', 'data_type': 'DATETIME'},
                  {'name': 'sale_price_effective_date_end', 'data_type': 'DATETIME'},
                  {'name': 'loyalty_points', 'data_type': 'STRING'},
                  {'name': 'product_type', 'data_type': 'STRING'},
                  {'name': 'brand', 'data_type': 'STRING'},
                  {'name': 'mpn', 'data_type': 'STRING'},
                  {'name': 'condition', 'data_type': 'STRING'},
                  {'name': 'adult', 'data_type': 'BOOLEAN'},
                  {'name': 'is_bundle', 'data_type': 'BOOLEAN'},
                  {'name': 'color', 'data_type': 'STRING'},
                  {'name': 'product_category', 'data_type': 'STRING'}]


class SimpleQSMock(object):
    def __init__(self, cf):
        self.cf = cf

    def values(self, ignoreA, ignoreB):
        return self.cf

simpleQSMock = SimpleQSMock(catalog_fields)

class RecsTestCase(SnowflakeTestCase):
    fixtures = []
    conn = None  # Calm sonar complaints about missing class member (it's set in superclass)
    tables_used = [
        'config_account',
        'config_dataset_data_expiration',
        'exchange_rate',
        'fact_product_view',
        'm_dedup_purchase_line',
        'm_session_first_geo',
        'product_catalog',
        'dio_purchase'
    ]

    @classmethod
    def setUpClass(cls):
        """Accounts and product catalog setup common to the algo tests."""
        super(RecsTestCase, cls).setUpClass()
        cls.account = warehouse_utils.create_account(session_cutover_time=warehouse_utils.LONG_AGO)
        # create feature flag
        # Note: normal fixtures for the features not included in monetate-tenant package
        with invalidation_context():
            feature_category = retailer_models.AccountFeatureCategory.objects.get_or_create(
                name='test_precompute',
                label='Test Precompute'
            )[0]
            retailer_models.AccountFeatureFlag.objects.get_or_create(
                name=retailer_models.ACCOUNT_FEATURES.ENABLE_NONCOLLAB_RECS_PRECOMPUTE,
                category=feature_category,
                status='dev',
                description=''
            )
            retailer_models.AccountFeatureFlag.objects.get_or_create(
                name=retailer_models.ACCOUNT_FEATURES.ENABLE_COLLAB_RECS_PRECOMPUTE_MODELING,
                category=feature_category,
                status='dev',
                description=''
            )
            retailer_models.AccountFeatureFlag.objects.get_or_create(
                name=retailer_models.ACCOUNT_FEATURES.UNIFIED_PRECOMPUTE,
                category=feature_category,
                status='dev',
                description=''
            )
            cls.account.add_feature(retailer_models.ACCOUNT_FEATURES.ENABLE_NONCOLLAB_RECS_PRECOMPUTE)
            cls.account.add_feature(retailer_models.ACCOUNT_FEATURES.ENABLE_COLLAB_RECS_PRECOMPUTE_MODELING)
            cls.account.add_feature(retailer_models.ACCOUNT_FEATURES.UNIFIED_PRECOMPUTE)
        cls.account_id = cls.account.id
        cls.retailer_id = cls.account.retailer.id
        cls.product_catalog_id = warehouse_utils.create_default_catalog_schema(cls.account).schema_id
        now = datetime.utcnow().replace(microsecond=0)
        update_time = (now - timedelta(seconds=1)).isoformat() + "Z"
        cls.conn.execute(
            """
            INSERT INTO config_account
            (account_id, name, instance, domain, timezone, currency, archived, session_cutover_time,
            retailer_id, retailer_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (cls.account.id, cls.account.name, 'p', 'example.com', 'EST', 'USD', 0, None,
             cls.account.retailer.id, cls.account.retailer.name)
        )
        cls.conn.execute(
            """
            INSERT INTO product_catalog
                (retailer_id, dataset_id, id, description, image_link, item_group_id, link, price, product_type,
                 title, update_time, brand, is_bundle, color, availability, custom)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00001', 'test', 'http://monetate.com/SKU-00001.jpg',
             'TP-00001', 'http://monetate.com/1', 1.99, 'Clothing > Shirt', 'T-Shirt', update_time, "ab", False,
             'black', 'In Stock', None),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00002', 'test', 'http://monetate.com/SKU-00002.jpg',
             'TP-00002', 'http://monetate.com/2', 2.99, 'Clothing > Pants, test', 'Jean Pants', update_time, "bc",
             True, 'black', 'In Stock', None),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00003', 'test', 'http://monetate.com/SKU-00003.jpg',
             'TP-00003', 'http://monetate.com/3', 3.99, 'Clothing > Pants', 'Jean Pants', update_time, "cd", True,
             'red', 'In Stock', None),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00004', 'test', 'http://monetate.com/SKU-00004.jpg',
             'TP-00004', 'http://monetate.com/4', 4.99, 'test ,    Clothing > Jeans', 'Jean Pants', update_time, "de",
             False, 'red', 'In Stock', None),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00005', 'test', 'http://monetate.com/SKU-00005.jpg',
             'TP-00005', 'http://monetate.com/5', 5.99, 'Clothing > Jeans', 'Jean Pants', update_time, "ef", False,
             'blue', 'In Stock', None),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00006', 'test', 'http://monetate.com/SKU-00006.jpg',
             'TP-00005', 'http://monetate.com/5', 6.99, 'test,Clothing > Jeans', 'Jean Pants', update_time, "fg", True,
             'white', 'In Stock', None),
        )
        cls.conn.execute(
            """
            UPDATE product_catalog
            SET custom = parse_json('{"product_category":"Daily Wear"}')
            WHERE id in ('SKU-00001','SKU-00002','SKU-00003','SKU-00004')
            """
        )
        cls.conn.execute(
            """
            UPDATE product_catalog
            SET custom = parse_json('{"product_category":"Daily_Wear"}')
            WHERE id in ('SKU-00005','SKU-00006')
            """
        )
        cutoff_time = now - timedelta(minutes=10)
        cls.conn.execute(
            """
            INSERT INTO config_dataset_data_expiration
                (id, dataset_id, cutoff_time, availability_time)
            VALUES
                (%s, %s, %s, %s)
            """,
            (20, cls.product_catalog_id, cutoff_time - timedelta(days=365), cutoff_time + timedelta(minutes=30)),
            # for pos join
            (21, 2, cutoff_time - timedelta(days=365), cutoff_time + timedelta(minutes=30)),
            (21, 5, cutoff_time - timedelta(days=365), cutoff_time + timedelta(minutes=30)),
            (22, 6, cutoff_time - timedelta(days=365), cutoff_time + timedelta(minutes=30)),
            (23, 7, cutoff_time - timedelta(days=365), cutoff_time + timedelta(minutes=30)),
            (24, 8, cutoff_time - timedelta(days=365), cutoff_time + timedelta(minutes=30)),
            (25, 9, cutoff_time - timedelta(days=365), cutoff_time + timedelta(minutes=30))
        )
        # make_row_list((cls.conn.execute("select dataset_id from config_dataset_data_expiration")))

    @patch_invalidations
    def _run_recs_test(self, algorithm, lookback, filter_json, expected_result=None, expected_result_arr=None,
                       geo_target="none", pushdown_filter_hashes=None, retailer_market_scope=None, market=None,
                       purchase_data_source="online", pushdown_filter_json_arr=None):
        # Insert row into config to mock out a lookback setting
        old_rec_setting = recs_models.AccountRecommendationSetting.objects.filter(account=self.account)
        if old_rec_setting:
            old_rec_setting[0].delete()
        recs_models.AccountRecommendationSetting.objects.create(
            account=self.account,
            lookback=lookback,
            filter_json='{"type": "or", "filters": []}',
        )

        with invalidation_context():
            recset = recs_models.RecommendationSet.objects.create(
                algorithm=algorithm,
                account=self.account,
                lookback_days=lookback,
                filter_json=filter_json,
                retailer=self.account.retailer,
                base_recommendation_on="none",
                geo_target=geo_target,
                name="test",
                order="algorithm",
                version=1,
                product_catalog=dio_models.Schema.objects.get(id=self.product_catalog_id),
                retailer_market_scope=self._setup_retailer_market(retailer_market_scope, market),
                market=self._setup_market(market),
                purchase_data_source=purchase_data_source
            )

        if retailer_market_scope is True or market is True:
            self.assertEqual([self.account_id],  get_account_ids_for_market_driven_recsets(recset, -1))

        # A run_id is added to path as part of the setup in SnowflakeTestCase to update stages
        unload_path, new_unload_path, sent_time = precompute_utils.create_unload_target_path(self.account.id, recset.id)

        s3_url = get_stage_s3_uri_prefix(self.conn, unload_path)

        new_s3_url = get_stage_s3_uri_prefix(self.conn, new_unload_path)

        with mock.patch('monetate.common.job_timing.record_job_timing'), \
            mock.patch('contextlib.closing', return_value=self.conn), \
            mock.patch('monetate.dio.models.Schema.active_field_set', simpleQSMock), \
            mock.patch('sqlalchemy.engine.Connection.close'), \
            mock.patch('monetate_recommendations.precompute_utils.create_unload_target_path',
                       autospec=True) as mock_suffix, \
            mock.patch('monetate_recommendations.offline.get_dataset_ids_for_pos') \
                as mock_pos_datasets, \
            mock.patch('monetate_recommendations.precompute_utils.unload_target_pid_path',
                       autospec=True) as mock_pid_suffix:
            mock_pos_datasets.return_value = [self.account_id, self.account_id]
            mock_suffix.return_value = unload_path, new_unload_path, sent_time
            FUNC_MAP[algorithm]([recset])
        expected_results = expected_result_arr or [expected_result]

        # TODO; Update these tests when old snowflake unload path is removed.
        actual_results = [json.loads(line.strip()) for line in s3_filereader2.read_s3_gz(s3_url)]
        actual_results_2 = [json.loads(line.strip()) for line in s3_filereader2.read_s3_gz(new_s3_url)]

        self._run_assertions(expected_results, actual_results, pushdown_filter_hashes, recset, pushdown_filter_json_arr,unload_type="old")
        self._run_assertions(expected_results, actual_results_2, pushdown_filter_hashes, recset, pushdown_filter_json_arr, unload_type="new")

    def _run_assertions(self, expected_results, actual_results, pushdown_filter_hashes, recset, pushdown_filter_json_arr, unload_type):
        self.assertEqual(len(actual_results), len(expected_results))
        for result_line in range(0, len(expected_results)):
            expected_result = expected_results[result_line]
            expected_feed_type = 'RECSET_NONCOLLAB_RECS' if unload_type == "old" else "RECSET_RECS"
            # lookup actual_result by pushdown_filter_hash if included otherwise assume results are in order
            if unload_type == "old":
                actual_result = [result for result in actual_results if
                                result['document']['pushdown_filter_hash'] == pushdown_filter_hashes[result_line]][0] \
                    if pushdown_filter_hashes else actual_results[result_line]
            else:
                actual_result = [result for result in actual_results if
                                result['document']['pushdown_filter_json'] == pushdown_filter_json_arr[result_line]][0] \
                    if pushdown_filter_json_arr else actual_results[result_line]

            # equal number product records vs expected
            self.assertEqual(len(actual_result['document']['data']), len(expected_result))
            self.assertEqual(actual_result['account']['id'], recset.account.id)
            self.assertEqual(actual_result['schema']['feed_type'], expected_feed_type)
            self.assertEqual(actual_result['schema']['id'], recset.id)

            # records match expected
            for i, item in enumerate(expected_result):
                self.assertEqual(item[0], actual_result['document']['data'][i]['id'])
                self.assertEqual(item[1], actual_result['document']['data'][i]['rank'])

    @classmethod
    def _setup_market(cls, setup):
        if setup is True:
            cls.market, created = Market.objects.get_or_create(
                name="Market from test",
                retailer=cls.account.retailer
            )
            if created:
                MarketAccount.objects.create(
                    account=cls.account,
                    market=cls.market
                )
            return cls.market

    @classmethod
    def _setup_retailer_market(cls, retailer_market, market):
        if retailer_market:
            return True
        elif market:
            return False
        else:
            return None

    @classmethod
    def set_account(cls, recset, account=None):
        # anytime a recset has a market, account_id should be None
        if recset.is_market_or_retailer_driven_ds:
            return None
        # if not market and not retailer level, return account_id from RecommendationSet table
        elif not recset.is_retailer_tenanted:
            return recset.account
        # if not market but retailer level, return the account_id of current account
        return account

class RecsTestCaseWithData(RecsTestCase):

    @classmethod
    def setUpClass(cls):
        super(RecsTestCaseWithData, cls).setUpClass()
        factgen = WarehouseFactsTestGenerator()
        mid0 = factgen.make_monetate_id(cls.account_id)
        mid1 = factgen.make_monetate_id(cls.account_id)
        mid2 = factgen.make_monetate_id(cls.account_id)

        qty = 8
        within_2_day = datetime.now() - timedelta(days=1)
        # slightly varying fact times are necessary for subsequent_purchase testing.
        # timedelta is a seeded random amount, so it should be consistent across runs
        random.seed(1234)
        within_2_day_1 = within_2_day + timedelta(minutes=random.randint(0,60))
        within_2_day_2 = within_2_day + timedelta(minutes=random.randint(0,60))
        within_2_day_3 = within_2_day + timedelta(minutes=random.randint(0,60))
        within_2_day_4 = within_2_day + timedelta(minutes=random.randint(0,60))
        within_2_day_5 = within_2_day + timedelta(minutes=random.randint(0,60))
        within_7_day = datetime.now() - timedelta(days=5)
        within_30_day = datetime.now() - timedelta(days=29)

        # offline customers
        customer0 = "customer0"
        customer1 = "customer1"
        customer2 = "customer2"

        v = [
            (mid0, within_7_day, 'TP-00003'),
            (mid0, within_7_day, 'TP-00004'),
            (mid0, within_7_day, 'TP-00005'),
            (mid0, within_30_day, 'TP-00001'),
            (mid0, within_30_day, 'TP-00002'),
            (mid0, within_30_day, 'TP-00003'),
            (mid0, within_30_day, 'TP-00004'),
            (mid0, within_30_day, 'TP-00005'),

            (mid1, within_7_day, 'TP-00002'),
            (mid1, within_7_day, 'TP-00003'),
            (mid1, within_30_day, 'TP-00001'),
            (mid1, within_30_day, 'TP-00002'),


            (mid2, within_7_day, 'TP-00004'),
            (mid2, within_7_day, 'TP-00005'),
            (mid2, within_30_day, 'TP-00001'),
            (mid2, within_30_day, 'TP-00002'),
            (mid2, within_30_day, 'TP-00003'),

        ]
        cls.conn.execute(
            """
            INSERT INTO fact_product_view
            (fact_date, account_id, fact_time, mid_epoch, mid_ts, mid_rnd, product_id, qty_in_stock)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(e[1].date(), e[0][0], e[1], e[0][1], e[0][3], e[0][2], e[2], qty) for e in v]
        )

        p = [
            (mid0, within_7_day, 'TP-00001', 'SKU-00005', 'purch_1', ),
            (mid0, within_7_day, 'TP-00002', 'SKU-00002', 'purch_2', ),
            (mid0, within_7_day, 'TP-00003', 'SKU-00004', 'purch_3', ),
            (mid0, within_7_day, 'TP-00004', 'SKU-00001', 'purch_4', ),

            (mid0, within_30_day, 'TP-00001', 'SKU-00005', 'purch_1', ),
            (mid0, within_30_day, 'TP-00002', 'SKU-00002', 'purch_2', ),
            (mid0, within_30_day, 'TP-00003', 'SKU-00004', 'purch_3', ),
            (mid0, within_30_day, 'TP-00004', 'SKU-00001', 'purch_4', ),

            (mid1, within_7_day, 'TP-00002', 'SKU-00005', 'purch_2', ),
            (mid1, within_7_day, 'TP-00003', 'SKU-00004', 'purch_3', ),
            (mid1, within_30_day, 'TP-00001', 'SKU-00005', 'purch_1', ),
            (mid1, within_30_day, 'TP-00002', 'SKU-00002', 'purch_2', ),
            (mid1, within_30_day, 'TP-00003', 'SKU-00004', 'purch_3', ),
            (mid1, within_30_day, 'TP-00004', 'SKU-00001', 'purch_4', ),

            (mid2, within_7_day, 'TP-00004', 'SKU-00001', 'purch_4', ),
            (mid2, within_7_day, 'TP-00005', 'SKU-00002', 'purch_5', ),
            (mid2, within_30_day, 'TP-00001', 'SKU-00005', 'purch_1', ),
            (mid2, within_30_day, 'TP-00002', 'SKU-00002', 'purch_2', ),
            (mid2, within_30_day, 'TP-00003', 'SKU-00004', 'purch_3', ),
            (mid2, within_30_day, 'TP-00004', 'SKU-00001', 'purch_4', ),

            (mid2, within_2_day_1, 'TP-00001', 'SKU-00005', 'purch_1',),
            (mid2, within_2_day_2, 'TP-00004', 'SKU-00001', 'purch_4',),
            (mid2, within_2_day_3, 'TP-00005', 'SKU-00002', 'purch_5',),
            (mid2, within_2_day_4, 'TP-00001', 'SKU-00005', 'purch_1',),
            (mid2, within_2_day_5, 'TP-00002', 'SKU-00002', 'purch_2',),
        ]
        cls.conn.execute(
            """
            INSERT INTO m_dedup_purchase_line
            (account_id, fact_time, mid_epoch, mid_ts, mid_rnd, purchase_id, line, product_id, sku, quantity, currency,
             currency_unit_price, usd_unit_price, usd_unit_cost)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, *[(e[0][0], e[1], e[0][1], e[0][3], e[0][2], e[4], 1, e[2], e[3], 1, 'USD', 3.0, 3.0, 3.0) for e in p]
        )

        offline_purchases = [
            (cls.retailer_id, 2, customer0, within_7_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, 2, customer0, within_7_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, 2, customer0, within_7_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, 2, customer0, within_7_day, 'purch_4', 'TP-00004', 'SKU-00001'),

            (cls.retailer_id, 2, customer0, within_30_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, 2, customer0, within_30_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, 2, customer0, within_30_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, 2, customer0, within_30_day, 'purch_4', 'TP-00004', 'SKU-00001'),

            (cls.retailer_id, 2, customer1, within_7_day, 'purch_2', 'TP-00002', 'SKU-00005'),
            (cls.retailer_id, 2, customer1, within_7_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, 2, customer1, within_30_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, 2, customer1, within_30_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, 2, customer1, within_30_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, 2, customer1, within_30_day, 'purch_4', 'TP-00004', 'SKU-00001'),

            (cls.retailer_id, 2, customer2, within_7_day, 'purch_4', 'TP-00004', 'SKU-00001'),
            (cls.retailer_id, 2, customer2, within_7_day, 'purch_5', 'TP-00005', 'SKU-00002'),
            (cls.retailer_id, 2, customer2, within_30_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, 2, customer2, within_30_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, 2, customer2, within_30_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, 2, customer2, within_30_day, 'purch_4', 'TP-00004', 'SKU-00001'),

            (cls.retailer_id, 2, customer2, within_2_day_1, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, 2, customer2, within_2_day_2, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, 2, customer2, within_2_day_3, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, 2, customer2, within_2_day_4, 'purch_4', 'TP-00004', 'SKU-00001'),
        ]

        cls.conn.execute(
            """
            INSERT INTO dio_purchase
            (retailer_id, dataset_id, customer_id, time, purchase_id, line, product_id, sku, currency,
            currency_unit_price, quantity, store_id, update_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(e[0], e[1], e[2], e[3], e[4], 1, e[5], e[6], 'USD', 3.0, 1, 2, e[3]) for e in offline_purchases]
        )

    @patch_invalidations
    def _run_collab_recs_test(self, algorithm, lookback, recsets, expected_results,
                              account=None, market=None, retailer=None,
                              similar_product_weights_json=None, purchase_data_source="online"):

        recset_group = recs_models.PrecomputeQueue.objects.get(
                account=account,
                market=market,
                retailer=retailer,
                algorithm=algorithm,
                lookback_days=lookback,
                purchase_data_source=purchase_data_source
            )

        # Insert row into config to mock out the similar_product_weights_json setting
        old_rec_setting = recs_models.AccountRecommendationSetting.objects.filter(account=self.account)
        if old_rec_setting:
            old_rec_setting[0].delete()
        recs_models.AccountRecommendationSetting.objects.create(
            account=self.account,
            lookback=lookback,
            filter_json='{"type": "or", "filters": []}',
            similar_product_weights_json=similar_product_weights_json,
        )
        unload_pid_path, pid_send_time = precompute_utils.unload_target_pid_path(recset_group.account,
                                                                                 recset_group.market,
                                                                                 recset_group.retailer,
                                                                                 algorithm, lookback)

        s3_url_pid_pid = get_stage_s3_uri_prefix(self.conn, unload_pid_path)
        unload_result = []
        s3_urls = []
        for recset in recsets:
            unload_path, new_unload_path, sent_time = precompute_utils.create_unload_target_path(self.account.id, recset.id)
            unload_result.append((unload_path, new_unload_path, sent_time))
            stage_s3_uris = (get_stage_s3_uri_prefix(self.conn, unload_path), get_stage_s3_uri_prefix(self.conn, new_unload_path))
            s3_urls.append(stage_s3_uris)
        with mock.patch('monetate.common.job_timing.record_job_timing'), \
            mock.patch('monetate_recommendations.offline.get_dataset_ids_for_pos') as mock_pos_datasets, \
            mock.patch('contextlib.closing', return_value=self.conn), \
            mock.patch('monetate.dio.models.Schema.active_field_set', simpleQSMock), \
            mock.patch('sqlalchemy.engine.Connection.close'), \
            mock.patch('monetate_recommendations.precompute_utils.create_unload_target_path',
                       autospec=True) as mock_suffix, \
            mock.patch('monetate_recommendations.precompute_utils.unload_target_pid_path',
                       autospec=True) as mock_pid_suffix:
            mock_pos_datasets.return_value = [1, 2]
            mock_pid_suffix.return_value = unload_pid_path, pid_send_time
            mock_suffix.side_effect = [(unload_path, new_unload_path, sent_time) for unload_path, new_unload_path, sent_time in unload_result]
            initialize_collab_algorithm([recset_group], algorithm)

        # test pid - pid (recset group)
        # commenting out as we are currently not using pid_pid output
        # actual_results_pid = [json.loads(line.strip()) for line in s3_filereader2.read_s3_gz(s3_url_pid_pid)]
        # self.assertEqual(len(actual_results_pid), len(pid_pid_expected_results))
        # for result_line in range(0, len(pid_pid_expected_results)):
        #     expected_result = pid_pid_expected_results[result_line]
        #     actual_result = actual_results_pid[result_line]
        #     # same lookup key
        #     self.assertEqual(actual_result['document']['lookup_key'], expected_result[0])
        #     # equal number product records vs expected
        #     self.assertEqual(len(actual_result['document']['data']), len(expected_result[1]))
        #     if recset_group.account_id:
        #         self.assertEqual(actual_result['schema']['account_id'], recset_group.account_id)
        #     if market:
        #         self.assertEqual(actual_result['schema']['market_id'], recset_group.market_id)
        #     if retailer:
        #         self.assertEqual(actual_result['schema']['retailer_id'], recset_group.retailer_id)
        #     self.assertEqual(actual_result['schema']['feed_type'], 'RECSET_COLLAB_RECS_PID')

            # records match expected
            # for i, item in enumerate(expected_result[1]):
            #     self.assertEqual(item[0], actual_result['document']['data'][i]['product_id'])
            #     self.assertEqual(item[1], actual_result['document']['data'][i]['score'])

        # test pid-sku (per recset)
        for index, recset in enumerate(recsets):
            expected_result_arr = expected_results[recset.id]
            actual_results = [json.loads(line.strip()) for line in s3_filereader2.read_s3_gz(s3_urls[index][0])]
            actual_results_2 = [json.loads(line.strip()) for line in s3_filereader2.read_s3_gz(s3_urls[index][1])]
            self.assertEqual(len(expected_result_arr), len(actual_results), "expected: {}\n actual: {}\n".format(expected_result_arr, actual_results))
            for i, item in enumerate(expected_result_arr):
                actual_result = actual_results[i]
                actual_result_2 = actual_results_2[i]
                if recset.account:
                    self.assertEqual(actual_result['account']['id'], recset.account.id)
                self.assertEqual(actual_result['schema']['feed_type'], 'RECSET_COLLAB_RECS', "\nExpected context item {}\n"
                                           "Expected recs is: {} \n"
                                            "Actual rec context item is {} \n"
                                            "Actual recs are: {}".format(item[0], item[1],
                                                                 actual_result['document']['lookup_key'],
                                                                 actual_result['document']['data']))

                # New unload path
                self.assertEqual(actual_result_2['schema']['feed_type'], 'RECSET_RECS', "\nExpected context item {}\n"
                                           "Expected recs is: {} \n"
                                            "Actual rec context item is {} \n"
                                            "Actual recs are: {}".format(item[0], item[1],
                                                                 actual_result_2['document']['lookup_key'],
                                                                 actual_result_2['document']['data']))

                self.assertEqual(len(actual_result['document']['data']), len(item[1]), "\nExpected context item {}\n"
                                            "Expected recs is: {} \n"
                                            "Actual rec context item is {} \n"
                                            "Actual recs are: {}".format(item[0], item[1],
                                                                 actual_result['document']['lookup_key'],
                                                                 actual_result['document']['data']))
                # New unload path
                self.assertEqual(len(actual_result_2['document']['data']), len(item[1]), "\nExpected context item {}\n"
                            "Expected recs is: {} \n"
                            "Actual rec context item is {} \n"
                            "Actual recs are: {}".format(item[0], item[1],
                                                    actual_result_2['document']['lookup_key'],
                                                    actual_result_2['document']['data']))

                self.assertEqual(actual_result['document']['lookup_key'], item[0], "\nExpected context item {}\n"
                                            "Expected recs is: {} \n"
                                            "Actual rec context item is {} \n"
                                            "Actual recs are: {}".format(item[0], item[1],
                                                                 actual_result['document']['lookup_key'],
                                                                 actual_result['document']['data']))

                # New unload path
                self.assertEqual(actual_result_2['document']['lookup_key'], item[0], "\nExpected context item {}\n"
                                            "Expected recs is: {} \n"
                                            "Actual rec context item is {} \n"
                                            "Actual recs are: {}".format(item[0], item[1],
                                                                 actual_result_2['document']['lookup_key'],
                                                                 actual_result_2['document']['data']))

                data = actual_result['document']['data']
                results = (actual_result['document']['data'], actual_result_2['document']['data'])
                for data in results:
                    for i, row in enumerate(item[1]):
                        self.assertEqual(row[0], data[i]['id'], "\nExpected context item {}\n"
                                                                "Actual rec context item is {} \n"
                                                                "Actual recs returned is: {}".format(item[0],
                                                                    actual_result['document']['lookup_key'], data))
                        self.assertEqual(row[1], data[i]['rank'], "\nTest failed on context item {}\n"
                                                "Actual recs returned is: {}".format(item[0], data))


