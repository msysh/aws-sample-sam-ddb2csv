import datetime
import dateutil
import logging
import os
import re
import time
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOG_LEVEL', 'WARNING'))

s3_dst_bucket = os.environ.get('S3_BUCKET')
s3_dst_prefix_format = os.environ.get('S3_PREFIX_FORMAT')
s3_dst_timezone_for_prefix_format = os.environ.get('S3_TIMEZONE_FOR_PREFIX_FORMAT', 'UTC')
s3_dst_csv_file_name = os.environ.get('S3_CSV_FILE_NAME')

s3 = boto3.resource('s3')

regexp_s3_location = re.compile(r's3://([^/]+?)/(.+\.csv$)')

def lambda_handler(event, context):

    logger.debug('event:')
    logger.debug(event)

    export_timestamp = event['ExportTime']
    (s3_src_bucket, s3_src_key) = extract_s3_location(event['queryResult']['QueryExecution']['ResultConfiguration']['OutputLocation'])

    if s3_dst_prefix_format != '':
        export_datetime = datetime.datetime.fromtimestamp(export_timestamp)
        export_datetime = export_datetime.astimezone(dateutil.tz.gettz(s3_dst_timezone_for_prefix_format))
        logger.debug("export datetime: {}".format(export_datetime))

        s3_dst_prefix = export_datetime.strftime(s3_dst_prefix_format)
        s3_dst_key = "{}/{}".format(s3_dst_prefix, s3_dst_csv_file_name)
    else:
        s3_dst_key = s3_dst_csv_file_name

    logger.debug("Source bucket:{}, key:{} / Destination bucket:{}, key:{}".format(s3_src_bucket, s3_src_key, s3_dst_bucket, s3_dst_key))

    try:
        move_object(s3_src_bucket, s3_src_key, s3_dst_bucket, s3_dst_key)
        delete_object(s3_src_bucket, "{}.metadata".format(s3_src_key))

    except Exception as e:
        raise(e)

    return {
        "statusCode": 200,
        "output": "Success",
    }

def extract_s3_location(s3_location):
    m = regexp_s3_location.match(s3_location)
    return (m.group(1), m.group(2))

def move_object(s3_src_bucket, s3_src_key, s3_dst_bucket, s3_dst_key):
    copy_object(s3_src_bucket, s3_src_key, s3_dst_bucket, s3_dst_key)
    delete_object(s3_src_bucket, s3_src_key)

def copy_object(s3_src_bucket, s3_src_key, s3_dst_bucket, s3_dst_key):
    copy_src = {
        'Bucket': s3_src_bucket,
        'Key': s3_src_key
    }
    bucket = s3.Bucket(s3_dst_bucket)
    dst = bucket.Object(s3_dst_key)
    result = dst.copy(copy_src)
    logger.info(result)

def delete_object(s3_bucket, s3_key):
    bucket = s3.Bucket(s3_bucket)
    obj = bucket.Object(s3_key)
    result = obj.delete()
    logger.info(result)
