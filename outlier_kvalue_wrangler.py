import json
import traceback
import pandas as pd
import os
import random
import boto3


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
    # Set up clients
    sqs = boto3.client('sqs')
    lambda_client = boto3.client('lambda')
    sns = boto3.client('sns')

    # Sqs
    queue_url = os.environ['queue_url']
    sqs_messageid_name = os.environ['sqs_messageid_name']

    # Sns
    arn = os.environ['arn']
    checkpoint = os.environ['checkpoint']

    method_name = os.environ['method_name']

    k_value_column = os.environ['k_value_column']
    try:

        # Reads in Data from SQS Queue
        response = sqs.receive_message(QueueUrl=queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])

        # Used for clearing the Queue
        receipt_handle = message['ReceiptHandle']

        data = pd.DataFrame(message_json)

        data[k_value_column] = 0

        data_json = data.to_json(orient='records')

        wrangled_data = lambda_client.invoke(FunctionName=method_name,
                                             Payload=json.dumps(data_json))

        json_response = wrangled_data.get('Payload').read().decode("UTF-8")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json_response, MessageGroupId=sqs_messageid_name,
                         MessageDeduplicationId=str(random.getrandbits(128)))

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        send_sns_message(checkpoint, sns, arn)

    except Exception as exc:

        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint
    }


def send_sns_message(checkpoint, sns, arn):
    sns_message = {
        "success": True,
        "module": "outlier_aggregation",
        "checkpoint": checkpoint
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )
