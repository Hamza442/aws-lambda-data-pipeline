def get_unique_desc_from_mapping_tbl(mapping_tbl):
    return list(set(row[1] for row in mapping_tbl))


def filter_mapping_tbl(mapping_table, id):
    filtered_result = [] 
    if len(id)>0:
        filtered_result = [t for t in mapping_table if int(t[2]) == int(id)]
    return filtered_result

def process_data_for_mapping(data_to_match,mapping_tables_data):
    value = ""
    for tup in mapping_tables_data:
        if len(tup) == 2:
            car_id, car_description = tup
        if len(tup) == 3:
            car_id, car_description,fk = tup
        
        if car_description == data_to_match:
            value  = str(car_id)
    return value

def map_data(car_data, mapping_tables, column_to_map, table):
    for column_name in column_to_map[:-2]:
        if column_name in car_data.keys():
            mapping_tables_data = mapping_tables.get(table[column_name])           
            car_data[f"{column_name}_id"] = process_data_for_mapping(car_data[column_name], mapping_tables_data)
    return car_data

def map_data_model_spec(car_data, mapping_tables, column_name):
    if column_name in car_data.keys():       
        car_data[f"{column_name}_id"] = process_data_for_mapping(car_data[column_name], mapping_tables)
    return car_data

def call_mapping_model_spec(car,mapping_tables,col_name,fk):
    filtered_mapping_table = filter_mapping_tbl(mapping_tables,fk)
    if filtered_mapping_table:
        car = map_data_model_spec(car,filtered_mapping_table,col_name)
    else:
        car[f"{col_name}_id"] = ''
    return car