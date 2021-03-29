import logging
import os
import re
import time
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOG_LEVEL', 'WARNING'))

dynamodb = boto3.client('dynamodb')

regexp_export_arn = re.compile(r'arn:aws:dynamodb:[\w\d-]+:\d{12}:table/.+/export/([\w\d-]+)')

def lambda_handler(event, context):

    logger.debug('event:')
    logger.debug(event)

    try:
        response = dynamodb.describe_export(
            ExportArn=event['ExportArn']
        )
        logger.info(response)
    except Exception as e:
        raise(e)
    else:
        output = {
            'ExportArn': response['ExportDescription']['ExportArn'],
            'ExportStatus': response['ExportDescription']['ExportStatus'],
            'ExportId': extract_export_id_from_export_arn(response['ExportDescription']['ExportArn']),
            'StartTime': time.mktime((response['ExportDescription']['StartTime']).timetuple()),
            'TableArn': response['ExportDescription']['TableArn'],
            'ExportTime': time.mktime((response['ExportDescription']['ExportTime']).timetuple()),
            'ClientToken': response['ExportDescription']['ClientToken'],
            'S3Bucket': response['ExportDescription']['S3Bucket'],
            'S3Prefix': response['ExportDescription']['S3Prefix']
        }

    if 'FailureCode' in response['ExportDescription']:
        logger.error(response)
        if response['ExportDescription']['FailureCode'] == 'S3AccessDenied':
            statusCode = 403
    else:
        statusCode = 200

    return {
        "statusCode": statusCode,
        "output": output,
    }

def extract_export_id_from_export_arn(export_arn):
    m = regexp_export_arn.match(export_arn)
    return m.group(1)