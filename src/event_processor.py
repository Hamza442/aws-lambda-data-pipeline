import gzip
from abc import ABC
import time
import json
from datetime import datetime

import pytz

from helpers import extract_event_name, rename_columns, get_mapping_tables
from clean import clean_data


class EventProcessor(ABC):
    def __init__(self, s3_client, aws_access_key: str, aws_secret_key: str, destination_prefix: str,
                 destination_bucket: str, secret_name: str, region: str, rds_pem_key: str, job_id: str,
                 dynamodb_table: str, host: str, cleaning_functions, dynamodb_client,  logger):
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
        self.dynamodb_client = dynamodb_client
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
            job_start_time = int(time.mktime(datetime.now().timetuple()))
            start_time = datetime.now()
            # Reading file
            contents = self.read_file_contents_from_s3(bucket, key, self.s3_client)
            cols_renamed = rename_columns(contents)
            mapping_tables = get_mapping_tables(
                self.secret_name, self.region, self.aws_access_key, self.aws_secret_key, self.host, self.rds_pem_key)
            cleaned_data = clean_data(cols_renamed, self.cleaning_functions, mapping_tables)
            event_name = extract_event_name(key)
            response = self.write_to_s3(
                self.s3_client, cleaned_data, self.destination_bucket, self.destination_prefix, event_name)
            # time for dynamodb logging
            end_time = datetime.now()
            elapsed_time = end_time - start_time
            elapsed_seconds = elapsed_time.total_seconds()
            self.save_job_details_in_dynamodb(
                self.job_id, job_start_time, f"s3://{bucket}/{key}", response['destination_file_name'], start_time,
                end_time, elapsed_seconds, response['status_code'], self.dynamodb_client,  self.dynamodb_table)
            self.logger.info(f"Processing completed for file:e s3://{bucket}/{key}")
        except Exception:
            self.logger.error(f"Error while processing the file s3://{bucket}/{key}")

    def read_file_contents_from_s3(self, bucket_name: str, s3_key: str, s3_client):
        self.logger.info(f"Reading file started from = s3://{bucket_name}/{s3_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        iterator = response['Body'].iter_lines()
        self.logger.info(f"Reading file completed from = s3://{bucket_name}/{s3_key}")
        return iterator

    def write_to_s3(self, s3_client, data, bucket_name, key_prefix, event_name):
        self.logger.info("======== Writing data to s3 ========")
        json_data = json.dumps(data)
        compressed_data = gzip.compress(json_data.encode('utf-8'))
        current_date = datetime.now(pytz.utc)
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        hour = current_date.strftime('%H')
        file_name = f"{event_name}_{current_date.strftime('%Y-%m-%dT%H-%M-%S')}"
        s3_key = f"{key_prefix}/{event_name}/year={year}/month={month}/day={day}/hour={hour}/{file_name}.json.gz"
        upload_res = s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=compressed_data)
        self.logger.info(f"Data uploaded to S3: s3://{bucket_name}/{s3_key}")
        return {
            'destination_file_name': f"s3://{s3_key}",
            'status_code': upload_res['ResponseMetadata']['HTTPStatusCode']
        }

    def save_job_details_in_dynamodb(self, job_id, job_start_time, source_file, destination_file, start_time, end_time,
                                     total_execution_time, status_code, dynamodb_client, dynamodb_table):
        try:
            self.logger.info("======== Writing lambda run details to dynamodb ========")
            job_run_details_item = {
                'job_id': job_id,
                'job_start_time': job_start_time,
                'source_file': source_file,
                'destination_file': destination_file,
                'start_time': str(start_time),
                'end_time': str(end_time),
                'total_execution_time': str(total_execution_time) + " seconds",
                'status_code': status_code,
                'created_at': str(datetime.now())
            }
            job_run_details_table = dynamodb_client.Table(dynamodb_table)
            job_run_details_table.put_item(Item=job_run_details_item)
            self.logger.info(f"Lambda run details inserted to dynamodb table : {dynamodb_table}")
        except Exception as e:
            self.logger.exception("Error while writing to dynamodb", e)