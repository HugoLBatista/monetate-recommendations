import hashlib
import json
from datetime import datetime, timedelta

from . import patch_invalidations
from monetate.warehouse.fact_generator import WarehouseFactsTestGenerator
from .testcases import RecsTestCaseWithData


class ViewAlsoViewTestCase(RecsTestCaseWithData):
    @classmethod
    @patch_invalidations
    def setUpClass(cls):
        super(ViewAlsoViewTestCase, cls).setUpClass()

    def test_30_day_view_also_view(self):
        filter_json = json.dumps({"type": "and", "filters": []})
        self._run_recs_test(
            algorithm="view_also_view",
            lookback=30,
            filter_json=filter_json,
            expected_result_arr=[
                ('TP-00005', [('SKU-00002', 1)]),
                ('TP-00002', [('SKU-00006', 1), ('SKU-00005', 1)]),
            ],
            market=True,
        )
