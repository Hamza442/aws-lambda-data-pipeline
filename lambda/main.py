import time
import json
import datetime
from datetime import datetime as dt
import logging
from helpers import extract_event_name, read_file_contents_from_s3, write_to_s3, save_job_run_details, rename_columns,\
    get_mapping_tables
from clean import trim_and_upper, cleaning_fuel_type, clean_transmission, clean_engine_size, clean_cylinders,\
    cleaning_hp, clean_by_type, clean_seller_type, clean_for_duration, clean_body_type, clean_data, cleaning_spec,\
    cleaningModel

logger = logging.getLogger()
logger.setLevel("INFO")

cleaning_functions = {
    'make': trim_and_upper,
    'year': trim_and_upper,
    'transmission': clean_transmission,
    'engine_size': clean_engine_size,
    'no_of_cylinders': clean_cylinders,
    'fuel_type': cleaning_fuel_type,
    'top_speed_kph': trim_and_upper,
    'doors': clean_by_type,
    'seats': clean_by_type,
    'gears': clean_by_type,
    'torque_nm': trim_and_upper,
    'colour_exterior': trim_and_upper,
    'colour_interior': trim_and_upper,
    'seller_type': clean_seller_type,
    'warranty_untill_when': clean_for_duration,
    'service_contract_untill_when': clean_for_duration,
    'hp': cleaning_hp,
    'body_type': clean_body_type,
    'spec': cleaning_spec,
    'model': cleaningModel
}

# needs to be replace by env's
destination_bucket = "raw-autodata-vd-ml-data"
destination_prefix = "zyte_feed"
lambda_job_id = "lambda-cleaning-job"
dynamodb_table = "lambda_run_details"
secret = "prod/auto-data/vd-readonly"
region = "ap-south-1"


def lambda_handler(event, context):
    logger.info("======== Lambda Execution started ========")
    for record in event["Records"]:
        body = json.loads(record["body"])
        message = json.loads(body["Message"])
        for event_record in message["Records"]:
            bucket = event_record["s3"]["bucket"]["name"]
            key = event_record["s3"]["object"]["key"]
            process_file(bucket, key)
    logger.info("======== Lambda Execution finished ========")


def process_file(bucket, key):
    try:
        logger.info(f"Processing started for file:e s3://{bucket}/{key}")
        # time for dynamodb logging
        job_start_time = int(time.mktime(dt.now().timetuple()))
        start_time = datetime.datetime.now()
        # Reading file
        contents = read_file_contents_from_s3(bucket, key)
        cols_renamed = rename_columns(contents)
        mapping_tables = get_mapping_tables(secret, region)
        cleaned_data = clean_data(cols_renamed, cleaning_functions, mapping_tables)
        event_name = extract_event_name(bucket)
        response = write_to_s3(cleaned_data, destination_bucket, destination_prefix, event_name)
        # time for dynamodb logging
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        elapsed_seconds = elapsed_time.total_seconds()
        save_job_run_details(
            lambda_job_id, job_start_time, f"s3://{bucket}/{key}", response['destination_file_name'], start_time,
            end_time, elapsed_seconds, response['status_code'], dynamodb_table)
        logger.info(f"Processing completed for file:e s3://{bucket}/{key}")
    except Exception:
        logger.error(f"Error while processing the file s3://{bucket}/{key}")
