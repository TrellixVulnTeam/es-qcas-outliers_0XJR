import json
import boto3
import traceback
import os

# clients
sns = boto3.client('sns')
sqs = boto3.client('sqs')

# env variables
sns_queue = os.environ['sns_queue']
sqs_queue = os.environ['sqs_queue']


def _get_traceback(exception):
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def lambda_handler(event, context):
    try:
        # receive message
        message = '{"success": True, "message": "QCAS Outliers has run successfully.", "anomalies":"PLACEHOLDER"}'

        # send to sns
        sns.publish(TopicArn=sns_queue, Message=message)

        # purge sqs queue
        sqs.purge_queue(QueueUrl=sqs_queue)

    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }
