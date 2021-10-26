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
        pass

    def test_30_day_purchase_also_purchase_account_level_market(self):
        pass

    def test_30_day_purchase_also_purchase_account_level_retailer_market(self):
        pass

    def test_30_day_purchase_also_purchase_global_level(self):
        pass

    def test_30_day_purchase_also_purchase_global_level_market(self):
        pass

    def test_30_day_purchase_also_purchase_global_level_retauler_market(self):
        pass

    def test_7_purchase_also_purchase_day_account_level(self):
        pass

    def test_7_purchase_also_purchase_global_level(self):
        pass

    def test_30_day_purchase_also_purchasew_recset_level_filters(self):
        pass


