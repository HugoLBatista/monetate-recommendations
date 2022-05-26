from sqlalchemy import literal_column, text, case, select, func, Table
import monetate.dio.models as dio_models

DEFAULT_FIELDS = ['shipping_label', 'description', 'shipping_height', 'mpn', 'price', 'material', 'tax',
                  'shipping_weight','image_link', 'color', 'link', 'adult', 'promotion_id', 'multipack', 'age_group',
                  'id', 'condition','size', 'shipping', 'shipping_length', 'product_type', 'energy_efficiency_class',
                  'title', 'gender','size_type', 'shipping_width', 'is_bundle', 'additional_image_link', 'loyalty_points',
                  'pattern', 'sale_price', 'mobile_link', 'brand', 'item_group_id', 'availability','availability_date',
                  'sale_price_effective_date_begin','sale_price_effective_date_end']

def get_weights_query(weights_json, catalog_id, account, market, retailer, lookback_days):
    converted_cols = []
    selected_attributes = []
    active_fields = {field['name']:field['data_type'] for field in
                     dio_models.Schema.objects.get(id=catalog_id).active_version.fields.values("name", "data_type")}

    for attribute in weights_json:
        col = attribute['catalog_attribute']
        #check if the selected attribute is one of active attribute for the account
        #if yes then check if its one of the custom fields if yes then retrieve custom column attribute with the
        # name and datatype as custom:name::datatype ; else access the column with just the column name
        if col in active_fields.keys():
            if col not in DEFAULT_FIELDS:
                data_type = active_fields[col]
                query_col = 'custom:' + col + '::' + data_type
            else:
                query_col = col
            selected_attributes.append(col)
            weight = attribute.get('weight', None)

            if weight:
                statement = case(
                    [
                        (literal_column("pc1." + query_col).__eq__(literal_column("pc2." + query_col)), weight)
                    ],
                    else_=0)
            else:
                statement = case(
                    [
                        (literal_column("pc1." + query_col).__eq__(literal_column("pc2." + query_col)),
                         text('(SELECT 1/count(*) FROM scratch.retailer_product_catalog_{account_id}_{market_id}_{retailer_id}_{lookback_days}\
                          WHERE {column}=pc1.{column})'.format(column=query_col, account_id=account, market_id=market,
                                                        retailer_id=retailer, lookback_days=lookback_days)))
                    ],
                    else_=0)
            converted_cols.append(statement)

    return ",\n".join(
        [str(exp.compile(compile_kwargs={"literal_binds": True})) + ' AS ' + selected_attributes[index] for index, exp
         in enumerate(converted_cols)]) if \
               converted_cols else None, "+".join(selected_attributes)