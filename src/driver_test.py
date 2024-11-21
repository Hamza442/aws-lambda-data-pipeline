import logging

import boto3

from clean import trim_and_upper, cleaning_fuel_type, clean_transmission, clean_engine_size, clean_cylinders, \
    cleaning_hp, clean_by_type, clean_seller_type, clean_for_duration, clean_body_type, cleaning_spec, cleaning_model
from event_processor import EventProcessor

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
destination_raw_bucket = "raw-dl-autodata"
destination_stg_bucket = "stage-dl-autodata"
lambda_job_id = "lambda-cleaning-job"
dynamodb_table = "lambda_run_details"
secret = "prod/auto-data/vd-readonly"
region = "ap-south-1"  # pem key region and this region should be same
rds_pem_key = "prod/rds-pem"
HOST = "127.0.0.1"
# ,
mapping_tables_names = ['bb_modelyear', 'bb_doors', 'bb_seats', 'bb_gears', 'bb_noofcyls', 'bb_hp', 'bb_fuel', 'bb_body', 'bb_enginesize', 'bb_transmissions','bb_make','bb_model','bb_specifications','mastercodes_cache']

ACCESS_KEY = 'AKIAQFBJES7PFU5PQMDW'
SECRET_KEY = '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'

s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=region)
dynamodb = boto3.resource(
    'dynamodb', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=region)
logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger()
bucket = "raw-autodata-ml-data"
# key = "zyte_feed/dubizzle_uae_1/2024-03-26T22-47-45/dubizzle_uae_1-2.json"
key = "zyte_feed/mannual_uploads/items_dubizzle_uae_1_1033.json"
body = '{\n  "Type" : "Notification",\n  "MessageId" : "0e232f45-0b43-5705-b978-71385999d7a5",\n  "TopicArn" : ' \
       '"arn:aws:sns:ap-south-1:010823702494:raw-autodata-ml-data-topic",\n  "Subject" : "Amazon S3 Notification",' \
       '\n  "Message" : "{\\"Records\\":[{\\"eventVersion\\":\\"2.1\\",\\"eventSource\\":\\"aws:s3\\",' \
       '\\"awsRegion\\":\\"ap-south-1\\",\\"eventTime\\":\\"2024-03-26T22:47:53.630Z\\",' \
       '\\"eventName\\":\\"ObjectCreated:Put\\",\\"userIdentity\\":{' \
       '\\"principalId\\":\\"AWS:AIDAQFBJES7PEUFAG2PYP\\"},\\"requestParameters\\":{' \
       '\\"sourceIPAddress\\":\\"5.9.89.211\\"},\\"responseElements\\":{' \
       '\\"x-amz-request-id\\":\\"7ZAS3K72HE0XGFQ9\\",' \
       '\\"x-amz-id-2\\":\\"RpLZOTslQZ5Iuz0tNGClBYiG3EkDmy4s0qRYPbS2zZby4+a2LFfM7UkDF5i' \
       '+4cl7PGaLvxnWGcpVkaWRRvqYAxyQXtShkAwH\\"},\\"s3\\":{\\"s3SchemaVersion\\":\\"1.0\\",' \
       '\\"configurationId\\":\\"raw-autodata-ml-data-event\\",\\"bucket\\":{\\"name\\":\\"BUCKET_NAME\\",' \
       '\\"ownerIdentity\\":{\\"principalId\\":\\"A2IRVUX5GRQKOH\\"},' \
       '\\"arn\\":\\"arn:aws:s3:::raw-autodata-ml-data\\"},\\"object\\":{' \
       '\\"key\\":\\"KEY\\",\\"size\\":2104402,' \
       '\\"eTag\\":\\"feebd63cbfc9825139d49caeb7851126\\",\\"versionId\\":\\"ZyZR0AJ2FFUmAPWkqhRzgdH50EGNjE9V\\",' \
       '\\"sequencer\\":\\"00660350988583B540\\"}}}]}",\n  "Timestamp" : "2024-03-26T22:47:54.360Z",' \
       '\n  "SignatureVersion" : "1",\n  "Signature" : ' \
       '"Hr8djTilEPEf1+0Gl8A+swVbrFGSoLsiVRCOqKrnT5Zkr+3qUHT28HJ7+54rCKPgLzANJWCiDlhMl46HCZB' \
       '/l9u2AFe7P1u4Ftx4lzhocVprzKr5zCEYXOb0npm8p+rlCuE6Q97MWr7lNr0WYkrqtbWNcOnetnt5drtstWbzA6i7WgnBh/XfMEVPL' \
       '+IZoisbgpVCxRacmdQGluGfrEzxh61XcPf4yHNwLnkSUWu/M/SReiCOrdls/W2dhJiHU7kwiizY9TdZyde/tLFRbKwq+qxpgEDiFy0H6ksK' \
       '/sQK5VIEbLpRoaqCQtarEtNC8uZQpbaoX4Oc4Mydh/NwaVDcVw==",\n  "SigningCertURL" : ' \
       '"https://sns.ap-south-1.amazonaws.com/SimpleNotificationService-60eadc530605d63b8e62a523676ef735.pem",' \
       '\n  "UnsubscribeURL" : "https://sns.ap-south-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns' \
       ':ap-south-1:010823702494:raw-autodata-ml-data-topic:8b7fd09b-c161-4880-a712-a20cc789af38"\n} '
body = body.replace("BUCKET_NAME", bucket).replace("KEY", key)

event = {'Records': [{'messageId': 'c9fdc26a-31ba-469b-b4b2-e40df2d13319',
                      'receiptHandle': 'AQEBxuPUpfQDMF2FcddsLicN5UepMfgW9ItXvcAMDm3uEY8ZaE84tkHNfyw+Zs+rUbW2rFs2KE7pi2gKWSynX0u+ypfGszr5xXxPfiHyQV3yIHFlp8E5+z354Dp+pNlZSMXCKELzSadZcvrosHUx2Hx7AP1miQExzvnfeMdT6meAue5QR7ZoiuXxR4jkcG1zS7sgAqPErDN6BMajWGKed/d5Ww0NrHOxC4s63TKsmbOMExxDY5R3edt3GsHGaSv9RpAb1o3K9WUbT7MpRHMjfOTgGEkX2hPT5elNSp0D9t8E6uKWoeKrPNZHeSM4JRrQ2wj70i6Tchrj7m8ScPh/HpwnX/BUuvqaH9J6jn/RVU87YsKUm6TrforoPhjx0ofJJPSoGi7a+T4356MvRLFuQdC83EXsL9j0aYmtuIZfLKUjmEU=',
                      'body': body,
                      'attributes': {'ApproximateReceiveCount': '1096', 'SentTimestamp': '1711493274383',
                                     'SenderId': 'AIDAJKTPXYYAO6CI6DPKS',
                                     'ApproximateFirstReceiveTimestamp': '1711493274385'}, 'messageAttributes': {},
                      'md5OfBody': '4e3d0073623d56f3805c7acb4882bcbe', 'eventSource': 'aws:sqs',
                      'eventSourceARN': 'arn:aws:sqs:ap-south-1:010823702494:raw-autodata-ml-data-queue',
                      'awsRegion': 'ap-south-1'}]}
processor: EventProcessor = EventProcessor(
    s3, ACCESS_KEY, SECRET_KEY, destination_raw_bucket, destination_stg_bucket, secret, region, rds_pem_key, lambda_job_id,
    dynamodb_table, HOST, cleaning_functions, dynamodb, mapping_tables_names, logger)
processor.process_event(event)
