import traceback
import json
import os
import pandas as pd
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
    lambda_client = boto3.client('lambda')

    error_handler_arn = os.environ['error_handler_arn']

    o_weight_column = os.environ['o_weight_column']
    k_value_column = os.environ['k_value_column']
    a_weight_column = os.environ['a_weight_column']
    g_weight_column = os.environ['g_weight_column']
    aggregate_column = os.environ['aggregate_column']
    try:
        input_data = pd.read_json(event)
        outlier_weight_df = calc_outlier_weight(input_data, o_weight_column, k_value_column, a_weight_column,
                                                g_weight_column, aggregate_column)
        json_out = outlier_weight_df.to_json(orient='records')
        final_output = json.loads(json_out)

    except Exception as exc:

        # Invoke error handler lambda
        lambda_client.invoke(
            FunctionName=error_handler_arn,
            InvocationType='Event',
            Payload=json.dumps({'test': 'ccow'})
        )

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return final_output


def calc_outlier_weight(input_table, new_column_name, k_value_col, a_weight_col, g_weight_col, aggregate_col):
    input_table[new_column_name] = ((input_table[aggregate_col] + (
            (input_table[a_weight_col] * input_table[g_weight_col] - 1) * input_table[k_value_col])) / (
                                            input_table[a_weight_col] * input_table[g_weight_col])) / input_table[
                                       aggregate_col]
    return input_table
