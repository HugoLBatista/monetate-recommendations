import json

from monetate.test.testcases import TestCase
from monetate_recommendations import precompute_utils


class PrecomputeUtilsTestCase(TestCase):
    def test_parse_product_type_filter(self):
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
        result = precompute_utils.parse_product_type_filter(filter_json)
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

    def test_parse_product_type_filter_empty(self):
        filter_json = json.dumps({
            "type": "and",
            "filters": []
        })
        result = precompute_utils.parse_product_type_filter(filter_json)
        expected = {
            "type": "and",
            "filters": []
        }
        self.assertEqual(result, expected)

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
        result = precompute_utils.parse_product_type_filter(filter_json)
        expected = {
            "type": "and",
            "filters": []
        }
        self.assertEqual(result, expected)

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
        result = precompute_utils.parse_product_type_filter(filter_json)
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

    def test_get_filter_hash(self):
        json_dict = {
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
        expected_result = 'ebe64db5c4b60875fe2306bbe4dfc29d3dbfc19d'
        self.assertEqual(precompute_utils.get_filter_hash(json_dict), expected_result)
