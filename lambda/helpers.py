import re
import json
import boto3
import gzip
import uuid
from datetime import datetime
from conf import fields_data

s3 = boto3.client('s3')

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
        print(f"Reading file from = s3://{bucket_name}/{s3_key}")
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        json_data = response['Body'].read().decode('utf-8')
        return json.loads(json_data)
    except Exception as e:
        print("Error while reading data from s3 = ",e)


def write_to_s3(data,bucket_name,key_prefix,file_name):
    try:
        print("======== Writing data to s3 ========")
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

        print(f"Data uploaded to S3: s3://{bucket_name}/{s3_key}")
        
        return {
        'destination_file_name': f"s3://{s3_key}",
        'status_code': upload_res['ResponseMetadata']['HTTPStatusCode']
        }
    except Exception as e:
        print("Error while writing data to s3 = ",e)


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
        print(e)
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
        print("======== Writing lambda run details to dynamodb ========")
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
        print(f"Lambda run details inserted to dynamodb table : {dynamodb_table}")
    except Exception as e:
        print("Error while writing to dynamodb",e)