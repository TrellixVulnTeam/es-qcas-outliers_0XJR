import json
import boto3
import traceback
import os

# S et up clients
sns = boto3.client('sns')
s3 = boto3.resource('s3')

# Environment variables
arn = os.environ['arn']
bucket_name = os.environ['bucket']
output_file = os.environ['output_file']


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__

        )
    )


def lambda_handler(event, context):
    try:
        # This may need to change as per payload indexing
        error_message = event['data']['lambdaresult']['error']

        sns.publish(TopicArn=arn, Message=error_message)

        object = s3.Object(bucket_name, output_file)
        object.put(Body=error_message)

    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }
