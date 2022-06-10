import json
from monetate.test.testcases import TestCase
from monetate_recommendations import precompute_utils
from monetate_recommendations import supported_prefilter_expression

valid_filter_json = json.dumps({
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
})
empty_filter_json = json.dumps({
    "type": "or",
    "filters": []
})

catalog_fields = [{'name': 'id', 'data_type': 'STRING'},
                  {'name': 'title', 'data_type': 'STRING'},
                  {'name': 'description', 'data_type': 'STRING'},
                  {'name': 'link', 'data_type': 'STRING'},
                  {'name': 'image_link', 'data_type': 'STRING'},
                  {'name': 'additional_image_link', 'data_type': 'STRING'},
                  {'name': 'mobile_link', 'data_type': 'STRING'},
                  {'name': 'availability', 'data_type': 'STRING'},
                  {'name': 'availability_date', 'data_type': 'DATETIME'},
                  {'name': 'expiration_date', 'data_type': 'DATETIME'},
                  {'name': 'price', 'data_type': 'NUMBER'},
                  {'name': 'sale_price', 'data_type': 'NUMBER'},
                  {'name': 'sale_price_effective_date_begin', 'data_type': 'DATETIME'},
                  {'name': 'sale_price_effective_date_end', 'data_type': 'DATETIME'},
                  {'name': 'loyalty_points', 'data_type': 'STRING'},
                  {'name': 'product_type', 'data_type': 'STRING'},
                  {'name': 'brand', 'data_type': 'STRING'},
                  {'name': 'mpn', 'data_type': 'STRING'},
                  {'name': 'condition', 'data_type': 'STRING'},
                  {'name': 'adult', 'data_type': 'BOOLEAN'}
                  ]

class SupportedPrefilterTestCase(TestCase):
    def test_converted_and_not_none(self):
        expected_combined_converted = "((product_type LIKE :product_type_1 || '%%') OR (product_type LIKE '%%' || :product_type_2 || '%%') OR (product_type LIKE :product_type_3 || '%%') OR (product_type LIKE '%%' || :product_type_4 || '%%')) AND ((product_type LIKE :product_type_5 || '%%') OR (product_type LIKE '%%' || :product_type_6 || '%%') OR (product_type LIKE :product_type_7 || '%%') OR (product_type LIKE '%%' || :product_type_8 || '%%'))"
        expected_single_converted = "(product_type LIKE :product_type_1 || '%%') OR (product_type LIKE '%%' || :product_type_2 || '%%') OR (product_type LIKE :product_type_3 || '%%') OR (product_type LIKE '%%' || :product_type_4 || '%%')"
        expected_empty_converted = None

        early_filter, late_filter, has_dynamic = precompute_utils.parse_supported_filters(valid_filter_json, catalog_fields)
        empty_early_filter, empty_late_filter, empty_has_dynamic = precompute_utils.parse_supported_filters(
            empty_filter_json, catalog_fields)
        empty_first_test = supported_prefilter_expression.and_with_convert_without_null(empty_early_filter, late_filter, catalog_fields)
        empty_both_test = supported_prefilter_expression.and_with_convert_without_null(empty_early_filter,
                                                                                       empty_early_filter, catalog_fields)
        empty_second_test = supported_prefilter_expression.and_with_convert_without_null(late_filter,
                                                                                         empty_early_filter, catalog_fields)
        valid_both_test = supported_prefilter_expression.and_with_convert_without_null(late_filter, late_filter, catalog_fields)

        self.assertEqual(str(empty_first_test), expected_single_converted)
        self.assertEqual(str(empty_second_test), expected_single_converted)
        self.assertEqual(empty_both_test, expected_empty_converted)
        self.assertEqual(str(valid_both_test), expected_combined_converted)

    def test_get_query_and_variables(self):
        valid_early_filter, valid_late_filter, valid_has_dynamic = precompute_utils.parse_supported_filters(
            valid_filter_json, catalog_fields)
        empty_early_filter, empty_late_filter, empty_has_dynamic = precompute_utils.parse_supported_filters(
            empty_filter_json, catalog_fields)
        valid_result = supported_prefilter_expression.get_query_and_variables(
            valid_early_filter, valid_late_filter, empty_early_filter, empty_late_filter, catalog_fields)
        empty_result = supported_prefilter_expression.get_query_and_variables(
            empty_early_filter, empty_late_filter, empty_early_filter, empty_late_filter, catalog_fields
        )

        self.assertEqual(valid_result[0], "")
        self.assertEqual(valid_result[1],
                         "WHERE ((product_type LIKE :product_type_1 || '%%') OR (product_type LIKE '%%' || :product_type_2 || '%%') OR (product_type LIKE :product_type_3 || '%%') OR (product_type LIKE '%%' || :product_type_4 || '%%'))")
        self.assertEqual(empty_result[0], "")
        self.assertEqual(empty_result[1], "")
