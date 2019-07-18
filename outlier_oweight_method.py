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
    """
                    Initialises the environment variables and calls the
                    function to calculate outlier_weights.
                    :param event: Event object
                    :param context: Context object
                    :return: JSON string
                """
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
    """
                    Generates a DataFrame containing a new column with calculated outlier_weights in it.
                    :param input_table: DataFrame containing the columns
                    :param new_column_name: Column to write the calculated outlier weights to
                    :param k_value_col: Column which provides values for the k value variable
                    :param a_weight_col: Column which provides values for a weight variable
                    :param g_weight_col: Column which provides values for g weight variable
                    :param aggregate_col: Column which provides the aggregated values
                    :return: DataFrame
                    """
    input_table[new_column_name] = ((input_table[aggregate_col] + (
            (input_table[a_weight_col] * input_table[g_weight_col] - 1) * input_table[k_value_col])) / (
                                            input_table[a_weight_col] * input_table[g_weight_col])) / input_table[
                                       aggregate_col]
    return input_table
