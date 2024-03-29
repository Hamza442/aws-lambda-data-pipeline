import re
import json
import boto3
import pymysql
import paramiko
from io import StringIO
import pandas as pd
from datetime import datetime
from conf import fields_data
from sshtunnel import SSHTunnelForwarder


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
    with SSHTunnelForwarder((ssh_host, ssh_port), ssh_username=ssh_user, ssh_pkey=myp_key,
                            remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(
            host=host, user=sql_username, passwd=sql_password, db=sql_main_database, port=tunnel.local_bind_port)
        for table in tables:
            query = "SELECT DISTINCT description FROM {};".format(table)
            data = pd.read_sql_query(query, conn)
            mapping[table] = data['description'].tolist()
        conn.close()
    return mapping


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
