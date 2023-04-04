from datetime import datetime, timedelta
import json
import hashlib
import six

from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCase


class TrendingTestCase(RecsTestCase):
    """Test recsets generated for tredning products"""

    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(TrendingTestCase, cls).setUpClass()

        factgen = WarehouseFactsTestGenerator()
        mid_us_pa = factgen.make_monetate_id(cls.account_id)
        mid_us_nj = factgen.make_monetate_id(cls.account_id)
        mid_ca_on = factgen.make_monetate_id(cls.account_id)
        mid_us_pa2 = factgen.make_monetate_id(cls.account_id)
        mid_ca_on2 = factgen.make_monetate_id(cls.account_id)
        mid_ca_on3 = factgen.make_monetate_id(cls.account_id)
        within_7_day = datetime.now() - timedelta(days=6)
        within_30_day = datetime.now() - timedelta(days=29)
        outside_30_day = datetime.now() - timedelta(days=40)

        customer0 = "customer0"
        customer1 = "customer1"
        customer2 = "customer2"

        cls.conn.execute(
            """
            INSERT INTO m_session_first_geo
            (start_date, account_id, start_time, end_time, mid_epoch, mid_rnd, mid_ts, country_code, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (within_7_day.date(), mid_us_pa[0], within_7_day, within_7_day + timedelta(hours=2),
             mid_us_pa[1], mid_us_pa[2], mid_us_pa[3], 'US', 'PA'),
            (within_7_day.date(), mid_us_nj[0], within_7_day, within_7_day + timedelta(hours=2),
             mid_us_nj[1], mid_us_nj[2], mid_us_nj[3], 'US', 'NJ'),
            (within_7_day.date(), mid_ca_on[0], within_7_day, within_7_day + timedelta(hours=2),
             mid_ca_on[1], mid_ca_on[2], mid_ca_on[3], 'CA', 'ON'),
            (within_30_day.date(), mid_us_pa2[0], within_30_day, within_30_day + timedelta(hours=2),
             mid_us_pa2[1], mid_us_pa2[2], mid_us_pa2[3], 'US', 'PA'),
            (within_30_day.date(), mid_ca_on2[0], within_30_day, within_30_day + timedelta(hours=2),
             mid_ca_on2[1], mid_ca_on2[2], mid_ca_on2[3], 'CA', 'ON'),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, outside_30_day + timedelta(hours=2),
             mid_ca_on3[1], mid_ca_on3[2], mid_ca_on3[3], 'CA', 'ON')
        )

        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        # TP-00004  20                   0                   1
        cls.conn.execute(
            """
            INSERT INTO m_dedup_purchase_line
            (account_id, fact_time, mid_epoch, mid_ts, mid_rnd, purchase_id, line, product_id, sku, quantity, currency,
             currency_unit_price, usd_unit_price, usd_unit_cost)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            # 7-day lookback
            # US/PA Purchased 3 x TP-00005 and 3 x TP-00002
            (mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3], mid_us_pa[2], 'Fake_PO_1', 1,
             'TP-00005', 'SKU-00005', 3, 'USD', 2.0, 2.0, 2.0),
            (mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3], mid_us_pa[2], 'Fake_PO_1', 2,
             'TP-00002', 'SKU-00002', 3, 'USD', 3.0, 3.0, 3.0),
            (mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3], mid_us_pa[2], 'Fake_PO_5',
             2, 'TP-00004', 'SKU-00004', 5, 'USD', 4.0, 4.0, 4.0),

            # US/NJ Purchased 2 x TP-00005
            (mid_us_nj[0], within_7_day, mid_us_nj[1], mid_us_nj[3], mid_us_nj[2], 'Fake_PO_2', 1,
             'TP-00005', 'SKU-00005', 2, 'USD', 2.0, 2.0, 2.0),

            # CA/ON Purchased 1 x TP-00005, 2 x TP-00002, 3 x TP-00003
            (mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3], mid_ca_on[2], 'Fake_PO_3', 1,
             'TP-00005', 'SKU-00005', 7, 'USD', 2.0, 2.0, 2.0),
            (mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3], mid_ca_on[2], 'Fake_PO_3', 2,
             'TP-00002', 'SKU-00002', 2, 'USD', 3.0, 3.0, 3.0),
            (mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3], mid_ca_on[2], 'Fake_PO_3', 3,
             'TP-00003', 'SKU-00003', 3, 'USD', 3.0, 3.0, 3.0),

            # 30-day lookback
            # CA/ON Purchased 3 x TP-00005, 1 x TP-00004
            (mid_ca_on2[0], within_30_day, mid_ca_on2[1], mid_ca_on2[3], mid_ca_on2[2], 'Fake_PO_5',
             1, 'TP-00005', 'SKU-00005', 3, 'USD', 2.0, 2.0, 2.0),
            (mid_ca_on2[0], within_30_day, mid_ca_on2[1], mid_ca_on2[3], mid_ca_on2[2], 'Fake_PO_5',
             2, 'TP-00004', 'SKU-00004', 1, 'USD', 4.0, 4.0, 4.0),

            # US/PA Purchased 10 x TP-00004
            (mid_us_pa2[0], within_30_day, mid_us_pa2[1], mid_us_pa2[3], mid_us_pa2[2], 'Fake_PO_5',
             2, 'TP-00004', 'SKU-00004', 10, 'USD', 4.0, 4.0, 4.0),

            # Additional facts beyond 30 day lookback
            (mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3], mid_ca_on3[2], 'Fake_PO_6',
             1, 'TP-00001', 'SKU-00001', 50, 'USD', 2.0, 2.0, 2.0),
            (mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3], mid_ca_on3[2], 'Fake_PO_6',
             2, 'TP-00004', 'SKU-00004', 50, 'USD', 4.0, 4.0, 4.0),
        )

        # offline_data
        offline_purchases = [
            (cls.retailer_id, customer0, within_7_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, customer0, within_7_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, customer0, within_7_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, customer0, within_7_day, 'purch_4', 'TP-00004', 'SKU-00001'),

            (cls.retailer_id, customer0, within_30_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, customer0, within_30_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, customer0, within_30_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, customer0, within_30_day, 'purch_4', 'TP-00004', 'SKU-00001'),

            (cls.retailer_id, customer1, within_7_day, 'purch_2', 'TP-00002', 'SKU-00005'),
            (cls.retailer_id, customer1, within_7_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, customer1, within_30_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, customer1, within_30_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, customer1, within_30_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, customer1, within_30_day, 'purch_4', 'TP-00004', 'SKU-00001'),

            (cls.retailer_id, customer2, within_7_day, 'purch_4', 'TP-00004', 'SKU-00001'),
            (cls.retailer_id, customer2, within_7_day, 'purch_5', 'TP-00005', 'SKU-00002'),
            (cls.retailer_id, customer2, within_30_day, 'purch_1', 'TP-00001', 'SKU-00005'),
            (cls.retailer_id, customer2, within_30_day, 'purch_2', 'TP-00002', 'SKU-00002'),
            (cls.retailer_id, customer2, within_30_day, 'purch_3', 'TP-00003', 'SKU-00004'),
            (cls.retailer_id, customer2, within_30_day, 'purch_4', 'TP-00004', 'SKU-00001'),
        ]

        cls.conn.execute(
            """
                INSERT INTO dio_purchase
                (retailer_id, dataset_id, customer_id, time, purchase_id, line, product_id, sku, currency,
                currency_unit_price, quantity, store_id, update_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(e[0], cls.account_id, e[1], e[2], e[3], 1, e[4], e[5], 'USD', 3.0, 1, 2, e[2]) for e in offline_purchases]
        )

    def test_trending_no_geo(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00004  20                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 7/10
        # TP-00004(SKU-00004): 5/20
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00004', 3)
        ], purchase_data_source="online")

    def test_trending_no_geo_offline_pos(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00001', 1),
            ('SKU-00002', 2),
            ('SKU-00003', 3),
            ('SKU-00004', 4),
            ('SKU-00005', 5),
            ('SKU-00006', 6)
        ], purchase_data_source="offline")

    def test_trending_no_geo_online_offline(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00004', 3),
            ('SKU-00001', 4),
            ('SKU-00002', 5),
            ('SKU-00003', 6)
        ], purchase_data_source="online_offline")

    def test_trending_with_country(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00004  20                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 7/10
        # TP-00004(SKU-00004): 5/20
        filter_json = json.dumps({"type": "and", "filters": []})
        pushdown_filter_json_arr = [u'{"_country_code":"ca","product_type":""}', u'{"_country_code":"us","product_type":""}']
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result_arr=[
            [
                ('SKU-00005', 1, "CA"),
                ('SKU-00006', 2, "CA"),
            ], [
                ('SKU-00004', 1, "US"),
            ]
        ], geo_target="country", pushdown_filter_hashes=[
            hashlib.sha1(six.ensure_binary('product_type=/country_code=CA'.lower())).hexdigest(),
            hashlib.sha1(six.ensure_binary('product_type=/country_code=US'.lower())).hexdigest(),
        ], purchase_data_source="online", pushdown_filter_json_arr=pushdown_filter_json_arr)

    def test_trending_with_region(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00004  20                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 7/10
        # TP-00004(SKU-00004): 5/20
        filter_json = json.dumps({"type": "and", "filters": []})
        pushdown_filter_json_arr = [u'{"_country_code":"ca","_region":"on","product_type":""}', u'{"_country_code":"us","_region":"pa","product_type":""}']
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result_arr=[
            [
                ('SKU-00005', 1, "CA", "ON"),
                ('SKU-00006', 2, "CA", "ON"),
            ], [
                ('SKU-00004', 1, "US", "PA"),
            ]
        ], geo_target="region", pushdown_filter_hashes=[
            hashlib.sha1(six.ensure_binary('product_type=/country_code=CA/region=ON'.lower())).hexdigest(),
            hashlib.sha1(six.ensure_binary('product_type=/country_code=US/region=PA'.lower())).hexdigest(),
        ], purchase_data_source="online", pushdown_filter_json_arr=pushdown_filter_json_arr)

    def test_trending_filter(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00004  20                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 7/10
        # TP-00004(SKU-00004): 5/20
        # skus matching product_type "Clothing > Jeans":
        # SKU-00005, SKU-00006, TP-00004
        filter_json = json.dumps({"type": "and", "filters": [{
            "type": "startswith",
            "left": {
                "type": "field",
                "field": "product_type"
            },
            "right": {
                "type": "value",
                "value": ["Clothing > Jeans"]
            }
        }]})
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00004', 3),
        ], purchase_data_source="online")

    def test_trending_filter_contains_multi(self):
        # skus matching product_type containing "jean":
        # SKU-00005, SKU-00006, SKU-00004
        filter_json = json.dumps({"type": "and", "filters": [{
            "type": "contains",
            "left": {
                "type": "field",
                "field": "product_type"
            },
            "right": {
                "type": "value",
                "value": ["jean"]
            }
        }]})
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00004', 3),
        ], purchase_data_source="online")

    def test_trending_filter_not_contains_multi(self):
        # Products not containing "jean"
        # SKU-00002, SKU-00003
        filter_json = json.dumps({"type": "and", "filters": [{
            "type": "not contains",
            "left": {
                "type": "field",
                "field": "product_type"
            },
            "right": {
                "type": "value",
                "value": ["jean"]
            }
        }]})
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result=[],
                            purchase_data_source="online")

    def test_trending_retailer_scope(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00004  20                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 7/10
        # TP-00004(SKU-00004): 5/20
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="trending",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00004', 3),
            ],
            retailer_market_scope=True, purchase_data_source="online"
        )

    def test_purchase_market_scope(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00004  20                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 7/10
        # TP-00004(SKU-00004): 5/20
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="trending",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00004', 3),
            ],
            market=True, purchase_data_source="online"
        )

    def test_sku_filter(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   7
        # TP-00004  5                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   10
        # TP-00004  20                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 7/10
        # TP-00004(SKU-00004): 5/20
        filter_json = json.dumps({"type": "and", "filters": [{
            "type": "in",
            "left": {
                "type": "field",
                "field": "id"
            },
            "right": {
                "type": "value",
                "value": ["SKU-00005"]
            },
            "type": "contains",
            "left": {
                "type": "field",
                "field": "id"
            },
            "right": {
                "type": "value",
                "value": ["SKU-00005"]
            },
            "type": "startswith",
            "right": {
                "type": "field",
                "field": "id"
            },
            "left": {
                "type": "value",
                "value": ["SKU-00006"]
            },
            "type": "!=",
            "left": {
                "type": "field",
                "field": "id"
            },
            "right": {
                "type": "value",
                "value": ["SKU-00004"]
            }
        }]})
        self._run_recs_test(
            algorithm="trending",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
            ],
            market=True, purchase_data_source="online"
        )