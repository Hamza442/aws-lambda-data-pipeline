from abc import ABC
import time
import json
import datetime
from datetime import datetime as dt

from helpers import extract_event_name, read_file_contents_from_s3, write_to_s3, save_job_run_details, rename_columns,\
    get_mapping_tables
from clean import clean_data


class EventProcessor(ABC):
    def __init__(self, s3_client, aws_access_key: str, aws_secret_key: str, destination_prefix: str,
                 destination_bucket: str, secret_name: str, region: str, rds_pem_key: str, job_id: str,
                 dynamodb_table: str, host: str, cleaning_functions,  logger):
        self.destination_bucket = destination_bucket
        self.destination_prefix = destination_prefix
        self.job_id = job_id
        self.dynamodb_table = dynamodb_table
        self.secret_name = secret_name
        self.region = region
        self.rds_pem_key = rds_pem_key
        self.host = host
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.s3_client = s3_client
        self.cleaning_functions = cleaning_functions
        self.logger = logger

    def process_event(self, event):
        self.logger.info("======== Lambda Execution started ========")
        for record in event["Records"]:
            body = json.loads(record["body"])
            message = json.loads(body["Message"])
            for event_record in message["Records"]:
                bucket = event_record["s3"]["bucket"]["name"]
                key = event_record["s3"]["object"]["key"]
                self.process_file(bucket, key)
        self.logger.info("======== Lambda Execution finished ========")

    def process_file(self, bucket, key):
        try:
            self.logger.info(f"Processing started for file:e s3://{bucket}/{key}")
            # time for dynamodb logging
            job_start_time = int(time.mktime(dt.now().timetuple()))
            start_time = datetime.datetime.now()
            # Reading file
            contents = read_file_contents_from_s3(bucket, key, self.s3_client)
            cols_renamed = rename_columns(contents)
            mapping_tables = get_mapping_tables(
                self.secret_name, self.region, self.aws_access_key, self.aws_secret_key, self.host, self.rds_pem_key)
            cleaned_data = clean_data(cols_renamed, self.cleaning_functions, mapping_tables)
            event_name = extract_event_name(key)
            response = write_to_s3(
                self.s3_client, cleaned_data, self.destination_bucket, self.destination_prefix, event_name)
            # time for dynamodb logging
            end_time = datetime.datetime.now()
            elapsed_time = end_time - start_time
            elapsed_seconds = elapsed_time.total_seconds()
            save_job_run_details(
                self.job_id, job_start_time, f"s3://{bucket}/{key}", response['destination_file_name'], start_time,
                end_time, elapsed_seconds, response['status_code'], self.dynamodb_table)
            self.logger.info(f"Processing completed for file:e s3://{bucket}/{key}")
        except Exception:
            self.logger.error(f"Error while processing the file s3://{bucket}/{key}")
