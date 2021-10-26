import hashlib
import json
from datetime import datetime, timedelta

from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCaseWithData


class PurchaseAlsoPurchase(RecsTestCaseWithData):
    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(PurchaseAlsoPurchase, cls).setUpClass()

    def test_30_day_purchase_also_purchase_account_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=False
        )

    def test_30_day_purchase_also_purchase_account_level_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=True
        )

    def test_30_day_purchase_also_purchase_account_level_retailer_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=False,
            retailer_market_scope=True
        )

    def test_30_day_purchase_also_purchase_global_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            global_recset=True
        )

    def test_30_day_purchase_also_purchase_global_level_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=True,
            global_recset=True
        )

    def test_30_day_purchase_also_purchase_global_level_retauler_market(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00004', [('SKU-00003', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00003', [('SKU-00004', 1), ('SKU-00002', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
                ('TP-00005', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3), ('SKU-00001', 4)]),
                ('TP-00002', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00001', 3), ('SKU-00006', 4), ('SKU-00005', 5)]),
            ],
            market=False,
            global_recset=True
        )

    def test_7_purchase_also_purchase_day_account_level(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="purchase_also_purchase",
            lookback=7,
            filter_json=filter_json,
            expected_result_arr= [
                ('TP-00001', [('SKU-00004', 1), ('SKU-00003', 2), ('SKU-00002', 3)]),
                ('TP-00004', [('SKU-00006', 1), ('SKU-00005', 2), ('SKU-00003', 3), ('SKU-00002', 4), ('SKU-00001', 5)]),
                ('TP-00003', [('SKU-00002', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),
                ('TP-00005', [('SKU-00004', 1)]),
                ('TP-00002', [('SKU-00003', 1), ('SKU-00004', 2), ('SKU-00001', 3)]),

            ],
            market=False
        )

    def test_7_purchase_also_purchase_global_level(self):
        pass

    def test_30_day_purchase_also_purchasew_recset_level_filters(self):
        pass


