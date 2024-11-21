import re
import json
import boto3
import pymysql
import paramiko
import pandas as pd
from io import StringIO
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
from conf import fields_data,rename_mastercode_cache,COMPOSITE_KEY,cols_to_map,cols_to_mapping_tbl


def get_secret(secret, region, aws_access_key: str, aws_secret_key: str) -> str:
    session = boto3.session.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    client = session.client(service_name='secretsmanager', region_name=region)
    response = client.get_secret_value(SecretId=secret)
    return response['SecretString']


def get_mapping_tables(tables: list[str], secret, region, aws_access_key, aws_secret_key, host, rds_pem_key) \
        -> dict[str, list[str]]:
    secrets: dict[str, str] = json.loads(get_secret(secret, region, aws_access_key, aws_secret_key))
    return get_mapping_table_desc(
        secrets['host'], secrets['username'], secrets['password'], secrets['database'], int(secrets['port']),
        secrets['ssh_hostname'], secrets['ssh_username'], int(secrets['ssh_port']), tables, host, aws_access_key,
        aws_secret_key, rds_pem_key, region)


def get_mapping_table_desc(sql_hostname: str, sql_username: str, sql_password: str, sql_main_database: str,
                           sql_port: str, ssh_host: str, ssh_user: str, ssh_port: str, tables: list[str], host: str,
                           aws_access_key: str, aws_secret_key: str, rds_pem_key: str, region: str) \
        -> dict[str, list[str]]:

    rds_key = get_secret(rds_pem_key, region, aws_access_key, aws_secret_key)
    myp_key = paramiko.RSAKey.from_private_key(StringIO(rds_key))
    mapping = {}
    mapping_tables_dict = {}
    with SSHTunnelForwarder((ssh_host, ssh_port), ssh_username=ssh_user, ssh_pkey=myp_key,
                            remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(
            host=host, user=sql_username, passwd=sql_password, db=sql_main_database, port=tunnel.local_bind_port)
        for table in tables:
            
            if table == 'bb_model':
                query = "SELECT id, upper(description) as description, make_id FROM {};".format(table)
                data = pd.read_sql_query(query, conn)
                mapping_tables_dict[table] = {str(makecode) + description: id for id, description, makecode in data.values}
            elif table == 'bb_specifications':
                query = "SELECT id, upper(description) as description, model_id FROM {};".format(table)
                data = pd.read_sql_query(query, conn)
                mapping_tables_dict[table] = {str(modelid) + description: id for id, description, modelid in data.values}
            elif table == 'mastercodes_cache':
                query = "SELECT admeid, model_year, make, model, doors, body_type, transmission, no_of_cyls, fuel, gears, seats, spec FROM {};".format(table)
                data = pd.read_sql_query(query, conn)
                data  = data.rename(columns=rename_mastercode_cache)
                mapping_tables_dict[table] = data.to_dict(orient='records')
            else:
                query = "SELECT id, upper(description) as description FROM {};".format(table)
                data = pd.read_sql_query(query, conn)
                mapping_tables_dict[table] = data.set_index('description')['id'].to_dict()

            if table != 'mastercodes_cache':
                query = "SELECT DISTINCT description FROM {};".format(table)
                data = pd.read_sql_query(query, conn)
                mapping[table] = data['description'].tolist()
        conn.close()
    return mapping, mapping_tables_dict


def rename_columns(contents):
    return [{fields_data.get(key, key): value for key, value in json.loads(item.decode('utf-8')).items()}
            for item in contents]


def extract_event_name(event_string):
    parts = event_string.split('/')
    event_name = parts[1].lower()
    return event_name


def count_months(date):
    current_date = datetime.now().date()
    year1 = current_date.year
    year2 = date.year
    month1 = current_date.month
    month2 = date.month
    diff = ((year2 - year1) * 12) + (month2 - month1)
    return str(diff)


def extract_numbers(input_string):
    pattern = r'[-+]?\d*\.?\d+'
    numbers = re.findall(pattern, input_string)
    return [num for num in numbers][0]


def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def parse_date(date_string):
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%Y-%b-%d', '%d-%b-%Y', '%b-%Y', '%Y-%b', '%d/%m/%Y', '%Y/%m/%d']
    try:
        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        # If none of the formats work, raise an exception or return None
        raise ValueError("Date string does not match any known format = ", date_string)
    except Exception as e:
        return False


def get_key(search_string, models):
    return models.index(search_string.upper()) if search_string.upper() in models else False


def custom_sort(item):
    return item['words']


def remove_makes(model, makes):
    for make in makes:
        model = model.replace(make, '')
    return model.strip()


def remove_descriptions(model, descriptions):
    for desc in descriptions:
        model = model.replace(desc, '')
    return model.strip().upper().replace("   ", " ")


def find_key_by_value(dict_list, value):
    for dictionary in dict_list:
        if value in dictionary.values():
            return next(iter(dictionary.keys()))  # Returns the first key found
    return None


def get_new_descriptions(data):
    new_descriptions = [desc.replace(' ', '').replace('-', '') for desc in data]
    result = [{desc: new_desc} for desc, new_desc in zip(data, new_descriptions)]
    return result


def add_admeid(car_data, mapping_tables):
    mastercode_cache = mapping_tables['mastercodes_cache']
    if not car_data['year_id'] or not car_data['make_id'] or not car_data['model_id']:
        car_data['admeid'] = ''
    else:
        # Rename keys for master code cache
        # - do renaming in mastercode_cache df and then convert to list of dicts
        # Create a new dictionary containing only non-empty values for keys present in COMPOSITE_KEY
        filtered_car_data = {key: car_data[key] for key in COMPOSITE_KEY if car_data.get(key)}
        updated_composite_key = list(filtered_car_data.keys())
        # Filter mastercode_cache except admeid
        filtered_mastercode_cache = [{key: dictionary[key] for key in dictionary if key in updated_composite_key or key == 'admeid'} for dictionary in mastercode_cache]
        match_found = False
        for mastercode_cache_dict in filtered_mastercode_cache:
            # Check if all values in original_dict match the values in additional_dict
            if all(filtered_car_data[key] == mastercode_cache_dict[key] for key in filtered_car_data):
                print("here")
                # Add the 'admeid' value to the original dictionary
                car_data['admeid'] = mastercode_cache_dict['admeid']
                match_found = True
                break  # Stop iterating if a match is found
        if not match_found:
            # Handle the case when a match is not found
            car_data['admeid'] = ''

            
    return car_data

def map_data(car_data,mapping_tables):
    
    for col_name in cols_to_map[:-2]:
        if col_name in car_data.keys(): # check column to map is in car_data
            mapping_table_to_process = mapping_tables.get(cols_to_mapping_tbl[col_name])
            if str(car_data[col_name]) in mapping_table_to_process:
                car_data[col_name + '_id'] = str(mapping_table_to_process[str(car_data[col_name])])
            else:
                car_data[col_name + '_id'] = ''
    return car_data

def map_data_model_spec(car_data,mapping_tables,col_name, fk):
    if col_name in car_data.keys(): # check column to map is in car_data
        mapping_table_to_process = mapping_tables.get(cols_to_mapping_tbl[col_name])
        key_to_check = ''.join([str(car_data[fk]),car_data[col_name]])
        if key_to_check in mapping_table_to_process:
            car_data[col_name + '_id'] = mapping_table_to_process[key_to_check]
        else:
            car_data[col_name + '_id'] = ''
    return car_data


def add_id_keys(raw_car, clean_car):
    id_keys = {key: value for key, value in clean_car.items() if key.endswith("_id")}
    raw_car.update(id_keys)
    return raw_car
    
        