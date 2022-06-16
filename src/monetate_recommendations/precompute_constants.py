# NOTE: availability/availability_date/expiration_date/sale_price_effective_date_begin/sale_price_effective_date_end
# not included so that such filters make the results update quickly
NON_PRODUCT_TYPE_PREFILTER_FIELDS = [
    'shipping_label', 'description', 'shipping_height', 'mpn', 'price', 'material', 'tax', 'shipping_weight',
    'image_link', 'color', 'link', 'adult', 'promotion_id', 'multipack', 'age_group', 'id', 'condition', 'size',
    'shipping', 'shipping_length', 'product_type', 'energy_efficiency_class', 'title', 'gender', 'size_type',
    'shipping_width', 'is_bundle', 'additional_image_link', 'loyalty_points', 'pattern', 'sale_price', 'mobile_link',
    'brand', 'item_group_id', 'availability']
SUPPORTED_PREFILTER_FIELDS = NON_PRODUCT_TYPE_PREFILTER_FIELDS + ['product_type']
UNSUPPORTED_PREFILTER_FIELDS = [
    'retailer_id', 'dataset_id', 'availability_date', 'expiration_date', 'sale_price_effective_date_begin',
    'sale_price_effective_date_end', 'update_time'
]
SUPPORTED_PREFILTER_FUNCTIONS = ['items_from_base_recommendation_on']
SUPPORTED_DATA_TYPES = [
    'string', 'number', 'datetime', 'boolean'
]

# currently not supporting multistring and google_product_category
DATA_TYPE_TO_SNOWFLAKE_TYPE = {
    'string': 'string',
    'number': 'number',
    'datetime': 'datetime',
    'boolean': 'boolean'
}