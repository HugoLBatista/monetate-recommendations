from datetime import datetime, timedelta
import mock
import json
import os

from django.utils import timezone
import monetate.common.s3_filereader2 as s3_filereader2
from monetate.common.warehouse.sqlalchemy_snowflake import get_stage_s3_uri_prefix
import monetate.recs.models as recs_models
from monetate.test.testcases import SnowflakeTestCase
import monetate.test.warehouse_utils as warehouse_utils
from monetate_recommendations import precompute_utils
from monetate_recommendations.precompute_algo_map import FUNC_MAP
from monetate_recommendations.precompute_collab_algo_map import FUNC_MAP as COLLAB_FUNC_MAP
import monetate.dio.models as dio_models
from monetate.retailer.cache import invalidation_context
import monetate.retailer.models as retailer_models
from monetate.market.models import Market, MarketAccount
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator

# Duct tape fix for running test in monetate_recommendations. Normally this would run as part of the
# SnowflakeTestCase setup, but the snowflake_schema_path is not the same when ran from monetate_recommendations.
# This path will allow the tables_used variable to successfully create the necessary tables for the test.
from monetate.test import testcases

from monetate_recommendations.precompute_utils import get_account_ids_for_market_driven_recsets
from tests import patch_invalidations

testcases.snowflake_schema_path = os.path.join(os.path.abspath(os.path.join(os.getcwd(), '..', '..')), 'ec2-user',
                                               'monetate-server', 'snowflake', 'tables', 'public')
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
                  {'name': 'is_bundle', 'data_type': 'BOOLEAN'}]


class SimpleQSMock(object):
    def __init__(self, cf):
        self.cf = cf

    def values(self, ignoreA, ignoreB):
        return self.cf


simpleQSMock = SimpleQSMock(catalog_fields)

class RecsTestCase(SnowflakeTestCase):
    conn = None  # Calm sonar complaints about missing class member (it's set in superclass)
    tables_used = [
        'config_account',
        'config_dataset_data_expiration',
        'exchange_rate',
        'fact_product_view',
        'm_dedup_purchase_line',
        'm_session_first_geo',
        'product_catalog',
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
            cls.account.add_feature(retailer_models.ACCOUNT_FEATURES.ENABLE_NONCOLLAB_RECS_PRECOMPUTE)
            cls.account.add_feature(retailer_models.ACCOUNT_FEATURES.ENABLE_COLLAB_RECS_PRECOMPUTE_MODELING)
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
                 title, update_time, brand, is_bundle)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00001', 'test', 'http://monetate.com/SKU-00001.jpg',
             'TP-00001', 'http://monetate.com/1', 1.99, 'Clothing > Pants', 'Jean Pants', update_time, "ab", False),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00002', 'test', 'http://monetate.com/SKU-00002.jpg',
             'TP-00002', 'http://monetate.com/2', 2.99, 'Clothing > Pants, test', 'Jean Pants', update_time, "bc",
             True),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00003', 'test', 'http://monetate.com/SKU-00003.jpg',
             'TP-00003', 'http://monetate.com/3', 3.99, 'Clothing > Pants', 'Jean Pants', update_time, "cd", True),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00004', 'test', 'http://monetate.com/SKU-00004.jpg',
             'TP-00004', 'http://monetate.com/4', 4.99, 'test ,    Clothing > Jeans', 'Jean Pants', update_time, "de",
             False),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00005', 'test', 'http://monetate.com/SKU-00005.jpg',
             'TP-00005', 'http://monetate.com/5', 5.99, 'Clothing > Jeans', 'Jean Pants', update_time, "ef", False),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00006', 'test', 'http://monetate.com/SKU-00006.jpg',
             'TP-00005', 'http://monetate.com/5', 6.99, 'test,Clothing > Jeans', 'Jean Pants', update_time, "fg", True),
        )
        cutoff_time = now - timedelta(minutes=10)
        cls.conn.execute(
            """
            INSERT INTO config_dataset_data_expiration
                (id, dataset_id, cutoff_time, availability_time)
            VALUES
                (%s, %s, %s, %s)
            """,
            (20, cls.product_catalog_id, cutoff_time, cutoff_time + timedelta(minutes=30))
        )

    @patch_invalidations
    def _run_recs_test(self, algorithm, lookback, filter_json, expected_result=None, expected_result_arr=None,
                       geo_target="none", pushdown_filter_hashes=None, retailer_market_scope=None, market=None):
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
            )

        if retailer_market_scope is True or market is True:
            self.assertEqual([self.account_id],  get_account_ids_for_market_driven_recsets(recset, -1))

        # A run_id is added to path as part of the setup in SnowflakeTestCase to update stages
        unload_path, sent_time = precompute_utils.create_unload_target_path(self.account.id, recset.id)

        s3_url = get_stage_s3_uri_prefix(self.conn, unload_path)

        with mock.patch('monetate.common.job_timing.record_job_timing'),\
             mock.patch('contextlib.closing', return_value=self.conn),\
             mock.patch('monetate.dio.models.Schema.active_field_set', simpleQSMock), \
             mock.patch('sqlalchemy.engine.Connection.close'),\
             mock.patch('monetate_recommendations.precompute_utils.create_unload_target_path',
                        autospec=True) as mock_suffix,\
            mock.patch('monetate_recommendations.precompute_utils.unload_target_pid_path',
                        autospec=True) as mock_pid_suffix:
            mock_suffix.return_value = unload_path, sent_time

            FUNC_MAP[algorithm]([recset])
        expected_results = expected_result_arr or [expected_result]

        actual_results = [json.loads(line.strip()) for line in s3_filereader2.read_s3_gz(s3_url)]

        self.assertEqual(len(actual_results), len(expected_results))
        for result_line in range(0, len(expected_results)):
            expected_result = expected_results[result_line]
            # lookup actual_result by pushdown_filter_hash if included otherwise assume results are in order
            actual_result = [result for result in actual_results if
                             result['document']['pushdown_filter_hash'] == pushdown_filter_hashes[result_line]][0] \
                if pushdown_filter_hashes else actual_results[result_line]
            # equal number product records vs expected
            self.assertEqual(len(actual_result['document']['data']), len(expected_result))
            self.assertEqual(actual_result['account']['id'], recset.account.id)
            self.assertEqual(actual_result['schema']['feed_type'], 'RECSET_NONCOLLAB_RECS')
            self.assertEqual(actual_result['schema']['id'], recset.id)

            # records match expected
            for i, item in enumerate(expected_result):
                self.assertEqual(item[0], actual_result['document']['data'][i]['ID'])
                self.assertEqual(item[1], actual_result['document']['data'][i]['RANK'])

    @classmethod
    def _setup_market(cls, setup):
        if setup is True:
            cls.market = Market.objects.create(
                name="Market from test",
                retailer=cls.account.retailer
            )
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
        within_7_day = datetime.now() - timedelta(days=5)
        within_30_day = datetime.now() - timedelta(days=29)

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
        ]
        cls.conn.execute(
            """
            INSERT INTO m_dedup_purchase_line
            (account_id, fact_time, mid_epoch, mid_ts, mid_rnd, purchase_id, line, product_id, sku, quantity, currency,
             currency_unit_price, usd_unit_price, usd_unit_cost)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, *[(e[0][0], e[1], e[0][1], e[0][3], e[0][2], e[4], 1, e[2], e[3], 1, 'USD', 3.0, 3.0, 3.0) for e in p]
        )

    @patch_invalidations
    def _run_collab_recs_test(self, algorithm, lookback, recsets, expected_results,
                              account=None, market=None, retailer=None):

        recset_group = recs_models.PrecomputeQueue.objects.get(
                account=account,
                market=market,
                retailer=retailer,
                algorithm=algorithm,
                lookback_days=lookback,
            )

        unload_pid_path, pid_send_time = precompute_utils.unload_target_pid_path(recset_group.account,
                                                                                 recset_group.market,
                                                                                 recset_group.retailer,
                                                                                 algorithm, lookback)

        s3_url_pid_pid = get_stage_s3_uri_prefix(self.conn, unload_pid_path)
        unload_result = []
        s3_urls = []
        for recset in recsets:
            unload_path, sent_time = precompute_utils.create_unload_target_path(self.account.id, recset.id)
            unload_result.append((unload_path, sent_time))
            s3_urls.append(get_stage_s3_uri_prefix(self.conn, unload_path))
        with mock.patch('monetate.common.job_timing.record_job_timing'), \
                mock.patch('contextlib.closing', return_value=self.conn), \
                mock.patch('monetate.dio.models.Schema.active_field_set', simpleQSMock), \
                mock.patch('sqlalchemy.engine.Connection.close'), \
                mock.patch('monetate_recommendations.precompute_utils.create_unload_target_path',
                           autospec=True) as mock_suffix, \
                mock.patch('monetate_recommendations.precompute_utils.unload_target_pid_path',
                           autospec=True) as mock_pid_suffix:
            mock_pid_suffix.return_value = unload_pid_path, pid_send_time
            mock_suffix.side_effect = [(unload_path, sent_time) for unload_path, sent_time in unload_result]
            COLLAB_FUNC_MAP[algorithm]([recset_group])

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
            actual_results = [json.loads(line.strip()) for line in s3_filereader2.read_s3_gz(s3_urls[index])]
            self.assertEqual(len(expected_result_arr), len(actual_results))

            for i, item in enumerate(expected_result_arr):
                actual_result = actual_results[i]
                if recset.account:
                    self.assertEqual(actual_result['account']['id'], recset.account.id)
                self.assertEqual(actual_result['schema']['feed_type'], 'RECSET_COLLAB_RECS')

                self.assertEqual(len(actual_result['document']['data']), len(item[1]))
                self.assertEqual(actual_result['document']['lookup_key'], item[0])
                data = actual_result['document']['data']
                for i, row in enumerate(item[1]):
                    self.assertEqual(row[0], data[i]['id'])
                    self.assertEqual(row[1], data[i]['rank'])


