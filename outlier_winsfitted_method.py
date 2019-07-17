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
    Calculates the winsorization fitted value for each row based
    on the strata's ratio, this method is called by the
    winsfitted wrangler.
    :param event: lambda event
    :param context: lambda context
    :return: json dataset
    """
    # Set up clients
    lambda_client = boto3.client('lambda')

    error_handler_arn = os.environ['error_handler_arn']

    # Set up parameters

    winsfitted_column = os.environ['winsfitted_column']
    ratio_column = os.environ['ratio']
    selection_data_column = os.environ['selection_data']
    try:
        input_data = pd.read_json(event)
        winsfitted_df = winsfitted(input_data, winsfitted_column, ratio_column, selection_data_column)

        json_out = winsfitted_df.to_json(orient='records')
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


def winsfitted(input_data, new_column_name, ratio_col, selection_data):
    """
    Calculates the fitted value by multiplying the strata's ratio with the column's selection data
    :param input_data: Input Dataframe
    :param new_column_name: Winsfitted Column Name
    :param ratio_col: Ratio Column name
    :param selection_data: Selection Data Name
    :return: input_data with fitted values in the new columns
    """
    input_data[new_column_name] = input_data[ratio_col] * input_data[selection_data]
    return input_data
