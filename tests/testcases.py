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

# Duct tape fix for running test in monetate_recommendations. Normally this would run as part of the
# SnowflakeTestCase setup, but the snowflake_schema_path is not the same when ran from monetate_recommendations.
# This path will allow the tables_used variable to successfully create the necessary tables for the test.
from monetate.test import testcases
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
        cls.account_id = cls.account.id
        cls.retailer_id = cls.account.retailer.id
        cls.product_catalog_id = warehouse_utils.create_default_catalog_schema(cls.account).schema_id
        now = datetime.utcnow().replace(microsecond=0)
        update_time = (now - timedelta(seconds=1)).isoformat() + "Z"
        cls.conn.execute(
            """
            INSERT INTO config_account
            (account_id, name, instance, domain, timezone, currency, archived, session_cutover_time, retailer_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (cls.account_id, cls.account.name, 'p', 'example.com', 'EST', 'USD', 0, None, cls.retailer_id)
        )
        cls.conn.execute(
            """
            INSERT INTO product_catalog
                (retailer_id, dataset_id, id, description, image_link, item_group_id, link, price, product_type,
                 title, update_time)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00001', 'test', 'http://monetate.com/SKU-00001.jpg',
             'TP-00001', 'http://monetate.com/1', 9.99, 'Clothing > Pants', 'Jean Pants', update_time),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00002', 'test', 'http://monetate.com/SKU-00002.jpg',
             'TP-00002', 'http://monetate.com/2', 9.99, 'Clothing > Pants', 'Jean Pants', update_time),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00003', 'test', 'http://monetate.com/SKU-00003.jpg',
             'TP-00003', 'http://monetate.com/3', 9.99, 'Clothing > Pants', 'Jean Pants', update_time),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00004', 'test', 'http://monetate.com/SKU-00004.jpg',
             'TP-00004', 'http://monetate.com/4', 9.99, 'Clothing > Jeans', 'Jean Pants', update_time),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00005', 'test', 'http://monetate.com/SKU-00005.jpg',
             'TP-00005', 'http://monetate.com/5', 9.99, 'Clothing > Jeans', 'Jean Pants', update_time),
            (cls.retailer_id, cls.product_catalog_id, 'SKU-00006', 'test', 'http://monetate.com/SKU-00006.jpg',
             'TP-00005', 'http://monetate.com/5', 9.99, 'Clothing > Jeans', 'Jean Pants', update_time),
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

    def _run_recs_test(self, algorithm, lookback, filter_json, expected_result):
        # Insert row into config to mock out a lookback setting
        recs_models.AccountRecommendationSetting.objects.create(
            account=self.account,
            lookback=lookback,
            filter_json="",
        )

        with invalidation_context():
            recset = recs_models.RecommendationSet.objects.create(
                algorithm=algorithm,
                account=self.account,
                lookback_days=lookback,
                filter_json=filter_json,
                retailer=self.account.retailer,
                base_recommendation_on="none",
                geo_target="none",
                name="test",
                order="algorithm",
                version=1,
                product_catalog=dio_models.Schema.objects.get(id=self.product_catalog_id),
            )

        # A run_id is added to path as part of the setup in SnowflakeTestCase to update stages
        unload_path, manifest_path = precompute_utils.create_unload_target_paths(recset.id)
        s3_url = get_stage_s3_uri_prefix(self.conn, unload_path)
        manifest_s3_url = get_stage_s3_uri_prefix(self.conn, manifest_path)

        with mock.patch('monetate.common.job_timing.record_job_timing'),\
             mock.patch('contextlib.closing', return_value=self.conn),\
             mock.patch('sqlalchemy.engine.Connection.close'),\
             mock.patch('monetate_recommendations.precompute_utils.create_unload_target_paths',
                        autospec=True) as mock_suffix:
            mock_suffix.return_value = unload_path, manifest_path
            FUNC_MAP[algorithm]([recset])

        # Ensure a manifest file was written (DIO uses this for the complete list of files created)
        expected_manifest_line = {
            'entries': [
                {'url': '', 'meta': {'content_length': 0}}  # NOTE: contents not checked yet
            ]
        }
        actual_manifest = list(s3_filereader2.read_s3(manifest_s3_url))
        # Only one line expected.
        self.assertTrue(len(actual_manifest), 1)
        # Line has an entries key
        self.assertItemsEqual(json.loads(actual_manifest[0]), expected_manifest_line)

        actual_result = [line.strip() for line in s3_filereader2.read_s3_gz(s3_url)]
        self.assertItemsEqual(actual_result, expected_result)