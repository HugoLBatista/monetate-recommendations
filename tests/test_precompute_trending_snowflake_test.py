from datetime import datetime, timedelta
import json
import hashlib

from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCase


class TrendingTestCase(RecsTestCase):
    """Test recsets generated for tredning products"""

    @classmethod
    def setUpClass(cls):
        super(TrendingTestCase, cls).setUpClass()

        factgen = WarehouseFactsTestGenerator()
        mid_us_pa = factgen.make_monetate_id(cls.account_id)
        mid_us_nj = factgen.make_monetate_id(cls.account_id)
        mid_ca_on = factgen.make_monetate_id(cls.account_id)
        mid_ca_on2 = factgen.make_monetate_id(cls.account_id)
        mid_ca_on3 = factgen.make_monetate_id(cls.account_id)
        within_7_day = datetime.now() - timedelta(days=6)
        within_30_day = datetime.now() - timedelta(days=29)
        outside_30_day = datetime.now() - timedelta(days=40)

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
            (within_30_day.date(), mid_ca_on2[0], within_30_day, within_30_day + timedelta(hours=2),
             mid_ca_on2[1], mid_ca_on2[2], mid_ca_on2[3], 'CA', 'ON'),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, outside_30_day + timedelta(hours=2),
             mid_ca_on3[1], mid_ca_on3[2], mid_ca_on3[3], 'CA', 'ON')
        )

        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   1
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   4
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        # TP-00004  0                   0                   1
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

            # US/NJ Purchased 2 x TP-00005
            (mid_us_nj[0], within_7_day, mid_us_nj[1], mid_us_nj[3], mid_us_nj[2], 'Fake_PO_2', 1,
             'TP-00005', 'SKU-00005', 2, 'USD', 2.0, 2.0, 2.0),

            # CA/ON Purchased 1 x TP-00005, 2 x TP-00002, 3 x TP-00003
            (mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3], mid_ca_on[2], 'Fake_PO_3', 1,
             'TP-00005', 'SKU-00005', 1, 'USD', 2.0, 2.0, 2.0),
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

            # Additional facts beyond 30 day lookback
            (mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3], mid_ca_on3[2], 'Fake_PO_6',
             1, 'TP-00001', 'SKU-00001', 50, 'USD', 2.0, 2.0, 2.0),
            (mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3], mid_ca_on3[2], 'Fake_PO_6',
             2, 'TP-00004', 'SKU-00004', 50, 'USD', 4.0, 4.0, 4.0),
        )

    def test_trending_no_geo(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="trending", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00002', 3),
            ('SKU-00003', 4),
        ])


    def test_trending_country(self):
        pass