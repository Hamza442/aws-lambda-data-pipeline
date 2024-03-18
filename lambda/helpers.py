import re
import os
import json
import boto3
import gzip
import uuid
import logging
import pymysql
import paramiko
from io import StringIO
import pandas as pd
from datetime import datetime
from conf import fields_data
from sshtunnel import SSHTunnelForwarder
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
# Replace with environment variable or configs
rdsPkey = "prod/rds-pem"
Pkeyregion = "ap-south-1"
HOST = "127.0.0.1"

ACESS_KEY = os.environ['aws_access_key']
SECRET_KEY = os.environ['aws_secret_key']


def get_secret(secret,region):

    session = boto3.session.Session(aws_access_key_id=ACESS_KEY
                                    ,aws_secret_access_key=SECRET_KEY)
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret
        )
    except ClientError as e:
        raise e

    return get_secret_value_response['SecretString']

def get_mapping_table_desc(sql_hostname
                     ,sql_username
                     ,sql_password
                     ,sql_main_database
                     ,sql_port
                     ,ssh_host
                     ,ssh_user
                     ,ssh_port
                     ,table):
    
    rdsKey = get_secret(rdsPkey,Pkeyregion)
    mypkey = paramiko.RSAKey.from_private_key(StringIO(rdsKey))

    with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(host=HOST, user=sql_username,
                passwd=sql_password, db=sql_main_database,
                port=tunnel.local_bind_port)
        query = "SELECT DISTINCT description FROM {};".format(table)
        data = pd.read_sql_query(query, conn)
        conn.close()
    return data['description'].tolist()


def rename_columns(data):
    return [{fields_data.get(key, key): value for key, value in item.items()} for item in data]


def extract_file_name(string):

    pattern = r'/([^/]+)\.json$'
    match = re.search(pattern, string)
    if match:
        return match.group(1)
    else:
        return None


def read_from_s3(bucket_name,s3_key):
    
    try:
        logging.info(f"Reading file from = s3://{bucket_name}/{s3_key}")
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        json_data = response['Body'].read().decode('utf-8')
        return json.loads(json_data)
    except Exception as e:
        logging.exception("Error while reading data from s3 = ",e)


def write_to_s3(data,bucket_name,key_prefix,file_name):
    try:
        logging.info("======== Writing data to s3 ========")
        json_data = json.dumps(data)
        compressed_data = gzip.compress(json_data.encode('utf-8'))
        
        current_date = datetime.now()
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        
        if file_name:
            dst_file_name = file_name
        else:
            dst_file_name = str(uuid.uuid4())
            
        s3_key = f"{key_prefix}/year={year}/month={month}/day={day}/{dst_file_name}.json.gz"
        upload_res = s3.put_object(Bucket=bucket_name
                    ,Key=s3_key
                    ,Body=compressed_data)

        logging.info(f"Data uploaded to S3: s3://{bucket_name}/{s3_key}")
        
        return {
        'destination_file_name': f"s3://{s3_key}",
        'status_code': upload_res['ResponseMetadata']['HTTPStatusCode']
        }
    except Exception as e:
        logging.exception("Error while writing data to s3 = ",e)


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
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%Y-%b-%d', '%d-%b-%Y','%b-%Y','%Y-%b','%d/%m/%Y','%Y/%m/%d']
    try:
        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        # If none of the formats work, raise an exception or return None
        raise ValueError("Date string does not match any known format = ",date_string)
    except Exception as e:
        logging.exception(e)
        return False

  
def save_job_run_details(job_id
                         ,job_start_time
                         ,source_file
                         ,destination_file
                         ,start_time
                         ,end_time
                         ,total_execution_time
                         ,status_code
                         ,dynamodb_table):
    try:
        logging.info("======== Writing lambda run details to dynamodb ========")
        dynamodb = boto3.resource('dynamodb')
        job_run_details_item = {
            'job_id': job_id
            ,'job_start_time': job_start_time
            ,'source_file': source_file
            ,'destination_file': destination_file
            ,'start_time': str(start_time)
            ,'end_time': str(end_time)
            ,'total_execution_time': str(total_execution_time)+ " seconds"
            ,'status_code': status_code
            ,'created_at': str(datetime.now())
        }
        job_run_details_table = dynamodb.Table(dynamodb_table)
        job_run_details_table.put_item(Item=job_run_details_item)
        logging.info(f"Lambda run details inserted to dynamodb table : {dynamodb_table}")
    except Exception as e:
        logging.exception("Error while writing to dynamodb",e)


def get_key(search_string,models):
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
    return model.strip().upper().replace("   "," ")


def find_key_by_value(dict_list, value):
    for dictionary in dict_list:
        if value in dictionary.values():
            return next(iter(dictionary.keys()))  # Returns the first key found
    return None


def get_new_descriptions(data):
    newdescriptions = [desc.replace(' ', '').replace('-', '') for desc in data]
    result = [{desc: newdesc} for desc, newdesc in zip(data, newdescriptions)]
    return result


def get_mapping_tables(secret,region):
    tables = ['bb_fuel', 'bb_enginesize','bb_body','bb_hp','bb_specifications','bb_model','bb_make']
    secrets = json.loads(get_secret(secret,region))
    mapping_dict = {}
    try:
        for table_name in tables:
            mapping_dict[table_name] = get_mapping_table_desc(secrets['host']
                                                            , secrets['username']
                                                            , secrets['password']
                                                            , secrets['database']
                                                            , int(secrets['port'])
                                                            , secrets['ssh_hostname']
                                                            , secrets['ssh_username']
                                                            , int(secrets['ssh_port'])
                                                            , table_name)
        return mapping_dict
    except Exception as e:
        logging.exception("Error while reading making tables",e)