import os
import logging
import boto3
from clean import trim_and_upper, cleaning_fuel_type, clean_transmission, clean_engine_size, clean_cylinders,\
    cleaning_hp, clean_by_type, clean_seller_type, clean_for_duration, clean_body_type, cleaning_spec, cleaning_model
from src.event_processor import EventProcessor

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
    'model': cleaning_model
}

# Replace with environment variable or configs
destination_bucket = "raw-autodata-vd-ml-data"
destination_prefix = "zyte_feed"
lambda_job_id = "lambda-cleaning-job"
dynamodb_table = "lambda_run_details"
secret = "prod/auto-data/vd-readonly"
region = "ap-south-1"  # pem key region and this region should be same
rds_pem_key = "prod/rds-pem"
HOST = "127.0.0.1"

ACCESS_KEY = os.environ['aws_access_key']
SECRET_KEY = os.environ['aws_secret_key']

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger()


def lambda_handler(event, context):
    processor: EventProcessor = EventProcessor(
        s3, ACCESS_KEY, SECRET_KEY, destination_prefix, destination_bucket, secret, region, rds_pem_key, lambda_job_id,
        dynamodb_table, HOST, cleaning_functions, dynamodb, logger)
    processor.process_event(event)

