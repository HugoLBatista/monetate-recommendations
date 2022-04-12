from datetime import datetime, timedelta
import json
import hashlib
import six

from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCase


class PurchaseCountTestCase(RecsTestCase):
    """Test recsets generated for most viewed products in a country."""

    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(PurchaseCountTestCase, cls).setUpClass()

        # Make view facts
        # (account_id, monetate_epoch, monetate_rnd, monetate_timestamp)
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

    def test_purchase_no_geo_7_days(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   1
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00005(SKU-00005/SKU-00006): 6
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00002', 3),
            ('SKU-00003', 4),
        ])

    def test_purchase_no_geo_30_days(self):
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   4
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        # TP-00004  0                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 9
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
        # TP-00004(SKU-00004): 1
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="purchase", lookback=30, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00002', 3),
            ('SKU-00003', 4),
            ('SKU-00004', 5),
        ])

    def test_purchase_with_country_geo_30_days(self):
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   4
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        # TP-00004  0                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 9
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
        # TP-00004(SKU-00004): 1
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="purchase", lookback=30, filter_json=filter_json, expected_result_arr=[
            [
                ('SKU-00005', 1, "CA"),
                ('SKU-00006', 2, "CA"),
                ('SKU-00003', 3, "CA"),
                ('SKU-00002', 4, "CA"),
                ('SKU-00004', 5, "CA"),
            ], [
                ('SKU-00005', 1, "US"),
                ('SKU-00006', 2, "US"),
                ('SKU-00002', 3, "US"),
            ]
        ], geo_target="country", pushdown_filter_hashes=[
            hashlib.sha1(six.ensure_binary('product_type=/country_code=CA'.lower())).hexdigest(),
            hashlib.sha1(six.ensure_binary('product_type=/country_code=US'.lower())).hexdigest(),
        ])

    def test_purchase_with_region_geo_30_days(self):
        # 30-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   4
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        # TP-00004  0                   0                   1
        #
        # TP-00005(SKU-00005/SKU-00006): 9
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
        # TP-00004(SKU-00004): 1
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(algorithm="purchase", lookback=30, filter_json=filter_json, expected_result_arr=[
            [
                ('SKU-00005', 1, "CA", "ON"),
                ('SKU-00006', 2, "CA", "ON"),
                ('SKU-00003', 3, "CA", "ON"),
                ('SKU-00002', 4, "CA", "ON"),
                ('SKU-00004', 5, "CA", "ON"),
            ], [
                ('SKU-00005', 1, "US", "NJ"),
                ('SKU-00006', 2, "US", "NJ"),
            ], [
                ('SKU-00002', 1, "US", "PA"),
                ('SKU-00005', 2, "US", "PA"),
                ('SKU-00006', 3, "US", "PA"),
            ]
        ], geo_target="region", pushdown_filter_hashes=[
            hashlib.sha1(six.ensure_binary('product_type=/country_code=CA/region=ON'.lower())).hexdigest(),
            hashlib.sha1(six.ensure_binary('product_type=/country_code=US/region=NJ'.lower())).hexdigest(),
            hashlib.sha1(six.ensure_binary('product_type=/country_code=US/region=PA'.lower())).hexdigest(),
        ])

    def test_purchase_filter(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   1
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00005(SKU-00005/SKU-00006): 6
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
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
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
        ])

    def test_purchase_filter_multi(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   1
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00005(SKU-00005/SKU-00006): 6
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
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
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
            ('SKU-00002', 3),
            ('SKU-00003', 4),
        ])

    def test_purchase_filter_contains_multi(self):
        # skus matching product_type containing "jean":
        # SKU-00005, SKU-00006
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
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00005', 1),
            ('SKU-00006', 2),
        ])

    def test_purchase_filter_not_contains_multi(self):
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
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=filter_json, expected_result=[
            ('SKU-00002', 1),
            ('SKU-00003', 2),
        ])

    def test_complex_filters(self):
        filter_json = {"type": "and", "filters": [{
            "type": "not contains",
            "left": {
                "type": "field",
                "field": "product_type"
            },
            "right": {
                "type": "value",
                "value": ["jean"]
            }
        }, {
            "type": "contains",
            "left": {
                "type": "field",
                "field": "brand"
            },
            "right": {
                "type": "value",
                "value": ["c"]
            }
        }, {
            "type": ">=",
            "left": {
                "type": "field",
                "field": "price"
            },
            "right": {
                "type": "value",
                "value": 2.99
            }
        }, {
            "type": "<=",
            "left": {
                "type": "field",
                "field": "price"
            },
            "right": {
                "type": "value",
                "value": 3.99
            }
        }, {
            "type": "==",
            "left": {
                "type": "field",
                "field": "is_bundle"
            },
            "right": {
                "type": "value",
                "value": True
            }
        }, {
            "type": "==",
            "left": {
                "type": "field",
                "field": "is_bundle_dsfds" # tests that we exclude this filter, since we don't recognize it
            },
            "right": {
                "type": "value",
                "value": False
            }
        }]}
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=json.dumps(filter_json), expected_result=[
            ('SKU-00002', 1),
            ('SKU-00003', 2),
        ])
        # replace the max price with a strictly less than
        filter_json["filters"][3]["type"] = "<"
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=json.dumps(filter_json), expected_result=[
            ('SKU-00002', 1),
        ])

        # revert max price to lt or eq
        filter_json["filters"][3]["type"] = "<="
        # change brand contains filter to "d"
        filter_json["filters"][1]["right"]["value"][0] = "d"
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=json.dumps(filter_json), expected_result=[
            ('SKU-00003', 1),
        ])

        # require is_bundle to be False
        filter_json["filters"][4]["right"]["value"] = False
        self._run_recs_test(algorithm="purchase", lookback=7, filter_json=json.dumps(filter_json), expected_result=[])

    def test_unrecognized_filter_with_or_prevents_filtering(self):
        filter_json = {"type": "or", "filters": [{
            "type": "contains",
            "left": {
                "type": "field",
                "field": "brand"
            },
            "right": {
                "type": "value",
                "value": ["c"]
            }
        }, {
            "type": "==",
            "left": {
                "type": "field",
                "field": "is_bundle_dsfds"  # tests an unrecognized filter
            },
            "right": {
                "type": "value",
                "value": False
            }
        }]}
        # tests that an or filter with an unrecognized input prevents application of any filters at all
        self._run_recs_test(
            algorithm="purchase",
            lookback=7,
            filter_json=json.dumps(filter_json),
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00002', 3),
                ('SKU-00003', 4),
            ],
            retailer_market_scope=True,
        )

    def test_unrecognized_filter_with_or_prevents_filtering_2(self):
        filter_json = {"type": "or", "filters": [{
            "type": "contains",
            "left": {
                "type": "field",
                "field": "brand"
            },
            "right": {
                "type": "value",
                "value": ["c"]
            }
        }, {
            "type": "dd",  # tests an unrecognized filter
            "left": {
                "type": "field",
                "field": "is_bundle"
            },
            "right": {
                "type": "value",
                "value": False
            }
        }]}
        # tests that an or filter with an unrecognized input prevents application of any filters at all
        self._run_recs_test(
            algorithm="purchase",
            lookback=7,
            filter_json=json.dumps(filter_json),
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00002', 3),
                ('SKU-00003', 4),
            ],
            retailer_market_scope=True,
        )

    def test_or_filtering(self):
        filter_json = {"type": "or", "filters": [{
            "type": "contains",
            "left": {
                "type": "field",
                "field": "brand"
            },
            "right": {
                "type": "value",
                "value": ["c"]
            }
        }, {
            "type": "==",
            "left": {
                "type": "field",
                "field": "is_bundle"  # tests an unrecognized filter
            },
            "right": {
                "type": "value",
                "value": False
            }
        }]}
        # tests that an or filter with an unrecognized input prevents application of any filters at all
        self._run_recs_test(
            algorithm="purchase",
            lookback=7,
            filter_json=json.dumps(filter_json),
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00002', 2),
                ('SKU-00003', 3),
            ],
            retailer_market_scope=True,
        )

    def test_purchase_retailer_scope(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   1
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00005(SKU-00005/SKU-00006): 6
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00002', 3),
                ('SKU-00003', 4),
            ],
            retailer_market_scope=True,
        )

    def test_purchase_market_scope(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   1
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00005(SKU-00005/SKU-00006): 6
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00002', 3),
                ('SKU-00003', 4),
            ],
            market=True,
        )

    # test different variations of sku filters
    def test_sku_filter(self):
        # 7-day totals:
        # PRODUCT   Purchases in US/PA  Purchases in US/NJ  Purchases in CA/ON
        # TP-00005  3                   2                   1
        # TP-00002  3                   0                   2
        # TP-00003  0                   0                   3
        #
        # TP-00005(SKU-00005/SKU-00006): 6
        # TP-00002(SKU-00002): 5
        # TP-00003(SKU-00003): 3
        filter_json = json.dumps({"type": "and", "filters": [{
            "type": "in",
            "left": {
                "type": "field",
                "field": "id"
            },
            "right": {
                "type": "value",
                "value": ["SKU-00003"]
            },
            "type": "contains",
            "left": {
                "type": "field",
                "field": "id"
            },
            "right": {
                "type": "value",
                "value": ["SKU-00006"]
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
                "value": ["SKU-00002"]
            }
        }]})
        self._run_recs_test(
            algorithm="purchase",
            lookback=7,
            filter_json=filter_json,
            expected_result=[
                ('SKU-00005', 1),
                ('SKU-00006', 2),
                ('SKU-00003', 3),
            ],
            market=True,
        )