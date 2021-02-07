import hashlib
import json
from datetime import datetime, timedelta

from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCase


class MostViewedTestCase(RecsTestCase):
    """Test recsets generated for most viewed products in a country."""

    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(MostViewedTestCase, cls).setUpClass()

        # Make view facts
        # (account_id, monetate_epoch, monetate_rnd, monetate_timestamp)
        factgen = WarehouseFactsTestGenerator()
        mid_us_pa = factgen.make_monetate_id(cls.account_id)
        mid_us_nj = factgen.make_monetate_id(cls.account_id)
        mid_ca_on = factgen.make_monetate_id(cls.account_id)
        mid_ca_on2 = factgen.make_monetate_id(cls.account_id)
        mid_ca_on3 = factgen.make_monetate_id(cls.account_id)
        qty = 8
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
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   2
        # TP-00003  0                   0                   3
        #
        # 30-day totals:
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   4
        # TP-00003  0                   0                   6
        # TP-00004  0                   0                   2
        cls.conn.execute(
            """
            INSERT INTO fact_product_view
            (fact_date, account_id, fact_time, mid_epoch, mid_ts, mid_rnd, product_id, qty_in_stock)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            # 7-day lookback
            # US/PA viewed 2 x TP-00005 and 5 x TP-00002
            (within_7_day.date(), mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3],
             mid_us_pa[2], 'TP-00005', qty),
            (within_7_day.date(), mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3],
             mid_us_pa[2], 'TP-00005', qty),
            (within_7_day.date(), mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3],
             mid_us_pa[2], 'TP-00002', qty),
            (within_7_day.date(), mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3],
             mid_us_pa[2], 'TP-00002', qty),
            (within_7_day.date(), mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3],
             mid_us_pa[2], 'TP-00002', qty),
            (within_7_day.date(), mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3],
             mid_us_pa[2], 'TP-00002', qty),
            (within_7_day.date(), mid_us_pa[0], within_7_day, mid_us_pa[1], mid_us_pa[3],
             mid_us_pa[2], 'TP-00002', qty),

            # US/NJ viewed 2 x TP-00005
            (within_7_day.date(), mid_us_nj[0], within_7_day, mid_us_nj[1], mid_us_nj[3],
             mid_us_nj[2], 'TP-00005', qty),
            (within_7_day.date(), mid_us_nj[0], within_7_day, mid_us_nj[1], mid_us_nj[3],
             mid_us_nj[2], 'TP-00005', qty),

            # CA/ON viewed 3 x TP-00003, 2 x TP-00002, 1 x TP-00005
            (within_7_day.date(), mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3],
             mid_ca_on[2], 'TP-00003', qty),
            (within_7_day.date(), mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3],
             mid_ca_on[2], 'TP-00003', qty),
            (within_7_day.date(), mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3],
             mid_ca_on[2], 'TP-00003', qty),
            (within_7_day.date(), mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3],
             mid_ca_on[2], 'TP-00002', qty),
            (within_7_day.date(), mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3],
             mid_ca_on[2], 'TP-00002', qty),
            (within_7_day.date(), mid_ca_on[0], within_7_day, mid_ca_on[1], mid_ca_on[3],
             mid_ca_on[2], 'TP-00005', qty),

            # 30-day lookback
            # CA/ON viewed 3 x TP-00003, 2 x TP-00004
            (within_30_day.date(), mid_ca_on2[0], within_30_day, mid_ca_on2[1], mid_ca_on2[3],
             mid_ca_on2[2], 'TP-00003', qty),
            (within_30_day.date(), mid_ca_on2[0], within_30_day, mid_ca_on2[1], mid_ca_on2[3],
             mid_ca_on2[2], 'TP-00003', qty),
            (within_30_day.date(), mid_ca_on2[0], within_30_day, mid_ca_on2[1], mid_ca_on2[3],
             mid_ca_on2[2], 'TP-00003', qty),
            (within_30_day.date(), mid_ca_on2[0], within_30_day, mid_ca_on2[1], mid_ca_on2[3],
             mid_ca_on2[2], 'TP-00004', qty),
            (within_30_day.date(), mid_ca_on2[0], within_30_day, mid_ca_on2[1], mid_ca_on2[3],
             mid_ca_on2[2], 'TP-00004', qty),

            # Additional facts beyond 30 day lookback
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00001', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00001', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00001', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00001', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00001', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00005', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00005', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00005', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00005', qty),
            (outside_30_day.date(), mid_ca_on3[0], outside_30_day, mid_ca_on3[1], mid_ca_on3[3],
             mid_ca_on3[2], 'TP-00005', qty),
        )

    def test_view_by_no_geo_7_days(self):
        # 7-day totals:
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00002(SKU-00002): 7
        # TP-00005(SKU-00005/SKU-00006): 5
        # TP-00003(SKU-00003): 3
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="view", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00002', 1),
            ('SKU-00005', 2),
            ('SKU-00006', 3),
            ('SKU-00003', 4),
        ])

    def test_view_by_no_geo_30_days(self):
        # 30-day totals:
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   4
        # TP-00003  0                   0                   6
        # TP-00004  0                   0                   2
        #
        # TP-00002(SKU-00002): 9
        # TP-00003(SKU-00003): 6
        # TP-00005(SKU-00005/SKU-00006): 5
        # TP-00004(SKU-00004): 2
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="view", lookback=30, filter_json=filter_json, expected_result=[
            ('SKU-00002', 1),
            ('SKU-00003', 2),
            ('SKU-00005', 3),
            ('SKU-00006', 4),
            ('SKU-00004', 5),
        ])

    def test_view_filter(self):
        # 7-day totals:
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00002(SKU-00002): 7
        # TP-00005(SKU-00005/SKU-00006): 5
        # TP-00003(SKU-00003): 3
        #
        # skus matching product_type "Clothing > Jeans":
        # SKU-00005, SKU-00006
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
        self._run_recs_test(algorithm="view", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
        ])

    def test_view_filter_multi(self):
        # 7-day totals:
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00002(SKU-00002): 7
        # TP-00005(SKU-00005/SKU-00006): 5
        # TP-00003(SKU-00003): 3
        #
        # skus matching product_type ["Clothing > Jeans", "Clothing > Pants"]:
        # SKU-00002, SKU-00003, SKU-00005, SKU-00006
        filter_json = json.dumps({"type": "and", "filters": [{
            "type": "startswith",
            "left": {
                "type": "field",
                "field": "product_type"
            },
            "right": {
                "type": "value",
                "value": ["Clothing > Jeans", "Clothing > Pants"]
            }
        }]})
        self._run_recs_test(algorithm="view", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00002', 1),
            ('SKU-00005', 2),
            ('SKU-00006', 3),
            ('SKU-00003', 4),
        ])

    def test_view_filter_dynamic(self):
        # 30-day totals:
        #
        # TP-00002(SKU-00002): 9, product_type: "Clothing > Pants, test"
        # TP-00003(SKU-00003): 6, product_type: "Clothing > Pants"
        # TP-00005(SKU-00005): 5, product_type: "Clothing > Jeans"
        # TP-00005(SKU-00006): 5, product_type: "test,Clothing > Jeans"
        # TP-00004(SKU-00004): 2, product_type: "test ,    Clothing > Jeans"
        filter_json = json.dumps({
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "product_type"
                },
                "right": {
                    "type": "function",
                    "value": "any_item_in_cart"
                }
            }]
        })
        self._run_recs_test(algorithm="view", lookback=30, filter_json=filter_json, expected_result_arr=[
            [  # product_type=""
                ('SKU-00002', 1),
                ('SKU-00003', 2),
                ('SKU-00005', 3),
                ('SKU-00006', 4),
                ('SKU-00004', 5),
            ], [  # product_type="Clothing > Jeans"
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00004', 3),
            ], [  # product_type="Clothing > Pants"
                ('SKU-00002', 1),
                ('SKU-00003', 2),
            ], [  # product_type="test"
                ('SKU-00002', 1),
                ('SKU-00006', 2),
                ('SKU-00004', 3),
            ]
        ], pushdown_filter_hashes=[
            hashlib.sha1('product_type='.lower()).hexdigest(),
            hashlib.sha1('product_type=Clothing > Jeans'.lower()).hexdigest(),
            hashlib.sha1('product_type=Clothing > Pants'.lower()).hexdigest(),
            hashlib.sha1('product_type=test'.lower()).hexdigest(),
        ])

    def test_view_retailer_scope(self):
        # 7-day totals:
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00002(SKU-00002): 7
        # TP-00005(SKU-00005/SKU-00006): 5
        # TP-00003(SKU-00003): 3
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00002', 1),
                ('SKU-00005', 2),
                ('SKU-00006', 3),
                ('SKU-00003', 4),
            ],
            retailer_market_scope=True,
        )

    def test_view_market_scope(self):
        # 7-day totals:
        # PRODUCT   Views in US/PA      Views in US/NJ      Views in CA/ON
        # TP-00005  2                   2                   1
        # TP-00002  5                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00002(SKU-00002): 7
        # TP-00005(SKU-00005/SKU-00006): 5
        # TP-00003(SKU-00003): 3
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00002', 1),
                ('SKU-00005', 2),
                ('SKU-00006', 3),
                ('SKU-00003', 4),
            ],
            market=True,
        )
