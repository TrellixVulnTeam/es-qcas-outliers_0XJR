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
    """
                Prepares data for and calls outlier_oweight_method.
                - adds on the required columns needed by the method.
                :param event: lambda event
                :param context: lambda context
                :return: string
                """
    # Set up clients
    s3 = boto3.resource('s3')
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

    o_weight_column = os.environ['o_weight_column']

    bucket_name = os.environ['bucket_name']
    try:

        # Reads in Data from SQS Queue
        response = sqs.receive_message(QueueUrl=queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])

        # Used for clearing the Queue
        receipt_handle = message['ReceiptHandle']

        data = pd.DataFrame(message_json)

        data[o_weight_column] = 0

        data_json = data.to_json(orient='records')

        wrangled_data = lambda_client.invoke(FunctionName=method_name, Payload=json.dumps(data_json))

        json_response = wrangled_data.get('Payload').read().decode("UTF-8")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json_response, MessageGroupId=sqs_messageid_name,
                         MessageDeduplicationId=str(random.getrandbits(128)))
        send_to_s3(json_response, bucket_name)

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        send_sns_message(checkpoint, arn, sns)

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


def send_sns_message(checkpoint, arn, sns):
    """
                    This function is responsible for sending notifications to the SNS Topic.
                    :param checkpoint: checkpoint.
                    :param sns: SNS client
                    :param arn: SNS arn
                    :return: None
                    """
    sns_message = {
        "success": True,
        "module": "outlier_aggregation",
        "checkpoint": checkpoint
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def send_to_s3(message, bucket_name):
    """
                    This function is responsible for writing the file to the s3 bucket.
                    :param message: The data to write to file.
                    :param bucket_name: Name of the bucket to write the file to.
                    :return: String: Name of the file or error string.
                    """
    try:
        s3 = boto3.resource('s3')

        encoded_string = message.encode("utf-8")

        bucket = s3.Bucket(bucket_name)

        file_name = "outlier_result.json"

        with open('/tmp/' + file_name, 'w+') as data:

            data.write(message)

        key = 'outlier_result.json'

        bucket.upload_file('/tmp/' + file_name, key)

        return file_name

    except Exception as error:
        return str(error)
