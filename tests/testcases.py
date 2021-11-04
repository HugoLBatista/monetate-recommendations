from datetime import datetime, timedelta
import mock
import json
import os

import monetate.common.s3_filereader2 as s3_filereader2
from monetate.common.warehouse.sqlalchemy_snowflake import get_stage_s3_uri_prefix
import monetate.recs.models as recs_models
from monetate.test.testcases import SnowflakeTestCase
import monetate.test.warehouse_utils as warehouse_utils
from monetate_recommendations import precompute_utils
from monetate_recommendations.precompute_algo_map import FUNC_MAP
import monetate.dio.models as dio_models
from monetate.retailer.cache import invalidation_context
import monetate.retailer.models as retailer_models
from monetate.market.models import Market, MarketAccount

# Duct tape fix for running test in monetate_recommendations. Normally this would run as part of the
# SnowflakeTestCase setup, but the snowflake_schema_path is not the same when ran from monetate_recommendations.
# This path will allow the tables_used variable to successfully create the necessary tables for the test.
from monetate.test import testcases

from monetate_recommendations.precompute_utils import get_account_ids_for_market_driven_recsets
from tests import patch_invalidations

testcases.snowflake_schema_path = os.path.join(os.path.abspath(os.path.join(os.getcwd(), '..', '..')), 'ec2-user',
                                               'monetate-server', 'snowflake', 'tables', 'public')


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
        """Accounts and product catalog setup common to the three algo tests."""
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
            cls.account.add_feature(retailer_models.ACCOUNT_FEATURES.ENABLE_NONCOLLAB_RECS_PRECOMPUTE)
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
                retailer_market_scope=retailer_market_scope,
                market=self._setup_market(market),
            )

        if retailer_market_scope is True or market is True:
            self.assertEqual([self.account_id],  get_account_ids_for_market_driven_recsets(recset, -1))

        # A run_id is added to path as part of the setup in SnowflakeTestCase to update stages
        unload_path, sent_time = precompute_utils.create_unload_target_path(self.account.id, recset.id)
        s3_url = get_stage_s3_uri_prefix(self.conn, unload_path)

        with mock.patch('monetate.common.job_timing.record_job_timing'),\
             mock.patch('contextlib.closing', return_value=self.conn),\
             mock.patch('sqlalchemy.engine.Connection.close'),\
             mock.patch('monetate_recommendations.precompute_utils.create_unload_target_path',
                        autospec=True) as mock_suffix:
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

    def _setup_market(self, setup):
        if setup is True:
            market = Market.objects.create(
                name="Market from test",
                retailer=self.account.retailer
            )
            MarketAccount.objects.create(
                account=self.account,
                market=market
            )
            return market
