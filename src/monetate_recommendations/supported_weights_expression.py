from sqlalchemy import literal_column, text, case, select, func, Table, and_
import monetate.dio.models as dio_models

DEFAULT_FIELDS = ['shipping_label', 'description', 'shipping_height', 'mpn', 'price', 'material', 'tax',
                  'shipping_weight','image_link', 'color', 'link', 'adult', 'promotion_id', 'multipack', 'age_group',
                  'id', 'condition','size', 'shipping', 'shipping_length', 'product_type', 'energy_efficiency_class',
                  'title', 'gender','size_type', 'shipping_width', 'is_bundle', 'additional_image_link', 'loyalty_points',
                  'pattern', 'sale_price', 'mobile_link', 'brand', 'item_group_id', 'availability','availability_date',
                  'sale_price_effective_date_begin','sale_price_effective_date_end']

def get_weights_query(weights_json, catalog_id, account, market, retailer, lookback_days):
    query_statements = []
    selected_attributes = []
    #Get the active fields for the given catalog as a dict `attribute_name:data_type`
    active_attributes = {field['name']:field['data_type'] for field in
                     dio_models.Schema.objects.get(id=catalog_id).active_field_set.values("name", "data_type")}

    for attribute in weights_json:
        attribute_name = attribute['catalog_attribute']
        #check if the selected attribute is one of active attribute for the account
        #if yes then check if its one of the custom fields
        #if yes then retrieve custom column attribute with the name and datatype as custom:name::datatype
        #else access the column with just the column name
        if attribute_name in active_attributes.keys():
            if attribute_name not in DEFAULT_FIELDS:
                data_type = active_attributes[attribute_name]
                query_column_name = 'custom:' + attribute_name + '::' + data_type
            else:
                query_column_name = attribute_name
            selected_attributes.append(attribute_name)
            weight = attribute.get('weight', None)

            if weight:
                statement = case(
                    [
                        (and_(
                            literal_column("pc1." + query_column_name).isnot(None),
                            literal_column("pc2." + query_column_name).isnot(None),
                            literal_column("pc1." + query_column_name).__eq__(literal_column("pc2." + query_column_name))
                        ),
                         weight)
                    ],
                    else_=0)
            else:
                statement = case(
                    [
                        (and_(
                            literal_column("pc1." + query_column_name).isnot(None),
                            literal_column("pc2." + query_column_name).isnot(None),
                            literal_column("pc1." + query_column_name).__eq__(literal_column("pc2." + query_column_name))
                        ),
                         text('(SELECT COALESCE(1/NULLIF(count(*),0), 0) FROM \
                         scratch.retailer_product_catalog_{account_id}_{market_id}_{retailer_id}_{lookback_days} \
                         WHERE {column}=pc1.{column})'.format(column=query_column_name, account_id=account, market_id=market,
                                                              retailer_id=retailer, lookback_days=lookback_days)))
                    ],
                    else_=0)
            query_statements.append(statement)

    return ",\n".join(
        [str(statement.compile(compile_kwargs={"literal_binds": True})) + ' AS ' + selected_attributes[index] for index, statement
         in enumerate(query_statements)]) if \
               query_statements else None, "+".join(selected_attributes)