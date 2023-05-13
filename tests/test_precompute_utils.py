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
            }, {
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brandabc"  # tests that we exclude fields which we don't support
                },
                "right": {
                    "type": "value",
                    "value": ["Monetate"]
                }
            },  {
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "customstring"  # tests that we include valid custom fields
                },
                "right": {
                    "type": "value",
                    "value": ["Monetate"]
                }
            }]
        })
        catalog_fields = [
            {
                'name': 'product_type',
                'data_type': 'string'
            },
            {
                'name': 'brand',
                'data_type': 'string'
            },
            {
                'name': 'customstring',
                'data_type': 'string'
            }
        ]
        early_filter, result, has_dynamic = precompute_utils.parse_non_collab_filters(filter_json, catalog_fields)
        expected_early = {
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
                },
                "right": {
                    "type": "value",
                    "value": ["Monetate"]
                }
            }, {
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "customstring"
                },
                "right": {
                    "type": "value",
                    "value": ["Monetate"]
                }
            }]
        }
        self.assertEqual(early_filter, expected_early)
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
        early_filter, result, has_dynamic = precompute_utils.parse_non_collab_filters(filter_json, [])
        expected = {
            "type": "and",
            "filters": []
        }
        self.assertEqual(result, expected)
        self.assertEqual(early_filter, expected)
        self.assertFalse(has_dynamic)

    def test_case_sensitivity_filter(self):
        filter_json = json.dumps({
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "CustomField"
                },
                "right": {
                    "type": "value",
                    "value": ["Apparel > Jeans"]
                }
            }]
        })
        catalog_fields = [
            {
                'name': 'CustomField',
                'data_type': 'string'
            }
        ]
        early_filter, result, has_dynamic = precompute_utils.parse_non_collab_filters(filter_json, catalog_fields)
        expected = {
            "type": "and",
            "filters": []
        }
        expected_early = {
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "CustomField"
                },
                "right": {
                    "type": "value",
                    "value": ["Apparel > Jeans"]
                }
            }]
        }
        self.assertEqual(result, expected)
        self.assertEqual(early_filter, expected_early)
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
        catalog_fields = [
            {
                'name': 'product_type',
                'data_type': 'string'
            }
        ]
        early_filter, result, has_dynamic = precompute_utils.parse_non_collab_filters(filter_json, catalog_fields)
        expected = {
            "type": "and",
            "filters": []
        }
        self.assertEqual(result, expected)
        self.assertEqual(early_filter, expected)
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
        catalog_fields = [
            {
                'name': 'product_type',
                'data_type': 'string'
            }
        ]
        early_filter, result, has_dynamic = precompute_utils.parse_non_collab_filters(filter_json, catalog_fields)
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
        expected_early = {
            "type": "or",
            "filters": []
        }
        self.assertEqual(early_filter, expected_early)
        self.assertEqual(result, expected)
        self.assertFalse(has_dynamic)

    def test_parse_or_across_product_type_and_normal(self):
        # we can't 'or' across product_type and other filters because of how the filters are performed in two separate
        # spots, so this should be excluded
        filter_json = json.dumps({
            "type": "or",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
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
        catalog_fields = [
            {
                'name': 'product_type',
                'data_type': 'string'
            },
            {
                'name': 'brand',
                'data_type': 'string'
            }
        ]
        early_filter, result, has_dynamic = precompute_utils.parse_non_collab_filters(filter_json, catalog_fields)
        expected = {
            "type": "or",
            "filters": []
        }
        self.assertEqual(result, expected)
        self.assertEqual(early_filter, expected)
        self.assertFalse(has_dynamic)

    def test_parse_and_across_product_type_and_normal(self):
        # we can't 'or' across product_type and other filters because of how the filters are performed in two separate
        # spots, so this should be excluded
        filter_json = json.dumps({
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
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
        catalog_fields = [
            {
                'name': 'product_type',
                'data_type': 'string'
            },
            {
                'name': 'brand',
                'data_type': 'string'
            }
        ]
        early_filter, result, has_dynamic = precompute_utils.parse_non_collab_filters(filter_json, catalog_fields)
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
                    "value": ["Halloween > Texas"]
                }
            }]
        }
        expected_early = {
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
                },
                "right": {
                    "type": "value",
                    "value": ["Apparel > Jeans"]
                }
            }]
        }
        self.assertEqual(result, expected)
        self.assertEqual(early_filter, expected_early)
        self.assertFalse(has_dynamic)

    def test_get_unload_target_path(self):
        account_id = 123
        recset_id = 456
        unload_path, new_unload_path, send_time = precompute_utils.create_unload_target_path(account_id, recset_id)
        self.assertTrue('{:%Y%m%dT%H%M%S.000Z}'.format(send_time) in unload_path)
        self.assertTrue('{:%Y%m%dT%H%M%S.000Z}'.format(send_time) in new_unload_path)
        self.assertEqual(unload_path, '@test_db.public.test_reco_merch_stage_v1/recs_global/{t:%Y/%m/%d}/recs_global-{t:%Y%m%dT%H%M%S.000Z}_PT1M-524288-655360-precompute_123_456.json.gz'.format(t=send_time))
        self.assertEqual(new_unload_path, '@test_db.public.test_reco_merch_stage_v1/recs_global/{t:%Y/%m/%d}/recs_global-{t:%Y%m%dT%H%M%S.000Z}_PT1M-524288-655360-precompute_123_456_new.json.gz'.format(t=send_time))

    def test_parse_collab_static_dynamic_filter(self):
        # we can't 'or' across product_type and other filters because of how the filters are performed in two separate
        # spots, so this should be excluded
        filter_json = json.dumps({
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
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
            },

                {
                    "type": "startswith",
                    "left": {
                        "type": "field",
                        "field": "product_type"
                    },
                    "right": {
                        "type": "function",
                        "value": "items_from_base_recommendation_on"
                    }
                }
            ]
        })
        catalog_fields = [
            {
                'name': 'product_type',
                'data_type': 'string'
            },
            {
                'name': 'brand',
                'data_type': 'string'
            }
        ]
        static_filter, dynamic_filter, has_hashable_dynamic_product_type_filter = precompute_utils.parse_collab_filters(filter_json, catalog_fields)
        expected_static = {
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
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
        expected_dynamic = {
            "type": "and",
            "filters": [{
                    "type": "startswith",
                    "left": {
                        "type": "field",
                        "field": "product_type"
                    },
                    "right": {
                        "type": "function",
                        "value": "items_from_base_recommendation_on"
                    }
                }]
        }
        self.assertEqual(static_filter, expected_static)
        self.assertEqual(dynamic_filter, expected_dynamic)

    def test_parse_collab_unsopported_dynamic_filter(self):
        filter_json = json.dumps({
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
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
            },

                {
                    "type": "startswith",
                    "left": {
                        "type": "field",
                        "field": "product_type"
                    },
                    "right": {
                        "type": "function",
                        "value": "any_item_in_cart"
                    }
                }
            ]
        })
        catalog_fields = [
            {
                'name': 'product_type',
                'data_type': 'string'
            },
            {
                'name': 'brand',
                'data_type': 'string'
            }
        ]
        static_filter, dynamic_filter, has_hashable_dynamic_product_type_filter = precompute_utils.parse_collab_filters(filter_json, catalog_fields)
        expected_static = {
            "type": "and",
            "filters": [{
                "type": "startswith",
                "left": {
                    "type": "field",
                    "field": "brand"
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
        expected_dynamic = {
            "type": "and",
            "filters": [
                        {
                            "left": {
                                    "field": "product_type",
                                    "type": "field"
                                    },
                            "right": {
                                    "type": "function",
                                    "value": "any_item_in_cart"
                                    },
                            "type": "startswith"
                        }
                    ]
                }
        self.assertEqual(static_filter, expected_static)
        self.assertEqual(dynamic_filter, expected_dynamic)
