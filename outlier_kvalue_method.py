import traceback
import json
import boto3
import os
import pandas as pd



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

    k_value_column = os.environ['k_value_column']
    winsfitted_column = os.environ['winsfitted_column']
    a_weight_column = os.environ['a_weight_column']
    g_weight_column = os.environ['g_weight_column']
    l_value_column = os.environ['l_value_column']

    try:
        input_data = pd.read_json(event)
        k_value_df = calc_k_value(input_data, k_value_column, winsfitted_column,a_weight_column,g_weight_column,l_value_column)
        json_out = k_value_df.to_json(orient='records')
        final_output = json.loads(json_out)

    except Exception as exc:

        # Invoke error handler lambda
        lambda_client.invoke(
            FunctionName=error_handler_arn,
            InvocationType='Event',
            Payload=json.dumps({'test':'ccow'})
        )

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return final_output

def calc_k_value(input_table,new_column_name,winsfitted_col,a_weight_col,g_weight_col,l_value_col):
    input_table[new_column_name] = input_table[winsfitted_col]+((input_table[l_value_col]/(input_table[a_weight_col]*input_table[g_weight_col] - 1)))
    return input_table