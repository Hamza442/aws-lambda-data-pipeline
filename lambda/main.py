import time
import json
import datetime
from datetime import datetime as dt
import logging
import logging
logger = logging.getLogger()
logger.setLevel("INFO")
from helpers import extract_file_name,read_from_s3,write_to_s3,save_job_run_details,rename_columns,get_mapping_tables
from clean import trim_and_upper\
                    ,cleaning_fuel_type\
                    ,clean_transmission\
                    ,clean_engine_size\
                    ,clean_cylinders\
                    ,cleaning_hp\
                    ,clean_by_type\
                    ,clean_seller_type\
                    ,clean_for_duration\
                    ,clean_body_type\
                    ,clean_data\
                    ,cleaningSpec\
                    ,cleaningModel

cleaning_functions = {
    'make':trim_and_upper
    ,'year':trim_and_upper
    ,'transmission':clean_transmission
    ,'engine_size':clean_engine_size
    ,'no_of_cylinders':clean_cylinders
    ,'fuel_type':cleaning_fuel_type
    ,'top_speed_kph':trim_and_upper
    ,'doors':clean_by_type
    ,'seats':clean_by_type
    ,'gears':clean_by_type
    ,'torque_nm':trim_and_upper
    ,'colour_exterior':trim_and_upper
    ,'colour_interior':trim_and_upper
    ,'seller_type': clean_seller_type
    ,'warranty_untill_when': clean_for_duration
    ,'service_contract_untill_when': clean_for_duration
    ,'hp': cleaning_hp
    ,'body_type': clean_body_type
    ,'spec': cleaningSpec
    ,'model':  cleaningModel
}

# needs to be replace by env's
destination_bucket = "raw-autodata-vd-ml-data"
destination_prefix = "zyte_feed"
lambda_job_id = "lambda-cleaning-job"
dynamodb_table = "lambda_run_details"
secret = "prod/auto-data/vd-readonly"
region = "ap-south-1"


def lambda_handler(event, context):
    logger.info("======== Executing lambda function ========")
    res = {}
    car_data = []
    
    # time for dynamodb logging
    job_start_time = int(time.mktime(dt.now().timetuple()))
    start_time = datetime.datetime.now()
    
    try:
        bucket_name = json.loads(json.loads(event["Records"][0]["body"])["Message"])["Records"][0]["s3"]["bucket"]["name"]
        bucket_key = json.loads(json.loads(event["Records"][0]["body"])["Message"])["Records"][0]["s3"]["object"]["key"]
        car_data = read_from_s3(bucket_name,bucket_key)
    except Exception as e:
        print("Error reading from S3",e)
    
    if car_data:
        cols_renamed = rename_columns(car_data)
        mapping_tables = get_mapping_tables(secret,region)
        cleaned_data = clean_data(cols_renamed,cleaning_functions,mapping_tables)
        file_name = extract_file_name(bucket_key)
        res = write_to_s3(cleaned_data,destination_bucket,destination_prefix,file_name)
    
    # time for dynamodb logging
    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    elapsed_seconds = elapsed_time.total_seconds()
    
    save_job_run_details(lambda_job_id
                         ,job_start_time
                         ,f"s3://{bucket_name}/{bucket_key}"
                         ,res['destination_file_name']
                         ,start_time
                         ,end_time
                         ,elapsed_seconds
                         ,res['status_code']
                         ,dynamodb_table)