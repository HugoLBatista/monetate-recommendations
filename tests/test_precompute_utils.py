import json
from monetate.test.testcases import TestCase
from monetate_recommendations import precompute_utils


class PrecomputeUtilsTestCase(TestCase):
    def test_parse_product_type_filter(self):
        self.maxDiff = None
        filter_json = json.dumps({
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "product_type"
                },
                "right": {
                    "type": "value",
                    "value": ["Apparel > Jeans", "Halloween > Texas"]
                }
            }, {
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
                },
                "right": {
                    "type": "value",
                    "value": ["Monetate"]
                }
            }]
        })
        result, has_dynamic = precompute_utils.parse_product_type_filter(filter_json)
        expected = {
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "product_type"
                },
                "right": {
                    "type": "value",
                    "value": ["Apparel > Jeans", "Halloween > Texas"]
                }
            }]
        }
        self.assertEqual(result, expected)
        self.assertFalse(has_dynamic)

    def test_parse_product_type_filter_empty(self):
        filter_json = json.dumps({
            "type": "and",
            "filters": []
        })
        result, has_dynamic = precompute_utils.parse_product_type_filter(filter_json)
        expected = {
            "type": "and",
            "filters": []
        }
        self.assertEqual(result, expected)
        self.assertFalse(has_dynamic)

    def test_parse_product_type_filter_dynamic(self):
        filter_json = json.dumps({
            "type": "and",
            "filters": [{
                "type": "in",
                "left":
                {
                    "type": "field",
                    "field": "product_type"
                },
                "right":
                {
                    "type": "function",
                    "value": "any_item_in_cart"
                }
            }]
        })
        result, has_dynamic = precompute_utils.parse_product_type_filter(filter_json)
        expected = {
            "type": "and",
            "filters": []
        }
        self.assertEqual(result, expected)
        self.assertTrue(has_dynamic)

    def test_parse_product_type_filter_multiple(self):
        filter_json = json.dumps({
            "type": "or",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "product_type"
                },
                "right": {
                    "type": "value",
                    "value": ["Apparel > Jeans"]
                }
            }, {
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "product_type"
                },
                "right": {
                    "type": "value",
                    "value": ["Halloween > Texas"]
                }
            }]
        })
        result, has_dynamic = precompute_utils.parse_product_type_filter(filter_json)
        expected = {
            "type": "or",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "product_type"
                },
                "right": {
                    "type": "value",
                    "value": ["Apparel > Jeans"]
                }
            }, {
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "product_type"
                },
                "right": {
                    "type": "value",
                    "value": ["Halloween > Texas"]
                }
            }]
        }
        self.assertEqual(result, expected)
        self.assertFalse(has_dynamic)

    def test_get_unload_target_path(self):
        account_id = 123
        recset_id = 456
        unload_path, send_time = precompute_utils.create_unload_target_path(account_id, recset_id)
        self.assertTrue('{:%Y%m%dT%H%M%S.000Z}'.format(send_time) in unload_path)
        self.assertEqual(unload_path, '@test_db.public.test_reco_merch_stage_v1/recs_global/{t:%Y/%m/%d}/recs_global-{t:%Y%m%dT%H%M%S.000Z}_PT1M-524288-655360-precompute_123_456.json.gz'.format(t=send_time))
