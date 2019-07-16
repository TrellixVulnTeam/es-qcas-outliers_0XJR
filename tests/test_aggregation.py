import unittest
from unittest import mock
import os
import json
import pandas as pd
from pandas.util.testing import assert_frame_equal
import outlier_aggregation_wrangler
import outlier_aggregation_method

class TestOutliers(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict('os.environ', {
            'arn': 'mock_arn',
            'checkpoint': '1',
            'method_name': 'outlier_aggregation_method',
            'queue_url': 'mock_queue',
            'sqs_messageid_name': 'mocking_aggregation',
            'sum_aggregate_column': 'sum_selection_data',
            'strata': 'period classification cell_no question',
            'error_handler_arn': 'mock_arn',
            'aggregate_column': 'selection_data'})

        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        # Stop the mocking of the os stuff
        cls.mock_os_patcher.stop()

    @mock.patch('outlier_aggregation_wrangler.boto3')
    def test_wrangler(self, mock_boto):
        # patch boto3 environ on second_mean_method
        with open('agg_input.json', 'r') as file:
            json_content = json.loads(file.read())
        with mock.patch('json.loads')as json_loads:
            json_loads.return_value = json_content
            outlier_aggregation_wrangler.lambda_handler(None, None)
        payload = mock_boto.client.return_value.invoke.call_args[1]['Payload']

        with open("agg_wrangler_output.json", "w+") as file:
            file.write(payload)

        payloadDF = pd.read_json(json.loads(payload))

        required_cols = {os.environ['sum_aggregate_column']}

        self.assertTrue(required_cols.issubset(set(payloadDF.columns)), 'Means columns are not in the DataFrame')

        new_cols = payloadDF[required_cols]
        self.assertFalse(new_cols.isnull().values.any())

    @mock.patch('outlier_aggregation_method.boto3')
    def test_method(self, mock_boto):
        SORTING_COLS = ['responder_id', 'region', 'question']
        input_file = 'agg_wrangler_output.json'

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())

        output = json.dumps(outlier_aggregation_method.lambda_handler(json_content, None))

        with open("test_calc_ratio_input.json", "w+") as file:
            file.write(output)

        expectedDF = pd.read_csv('aggData.csv').sort_values(SORTING_COLS).reset_index()

        responseDF = pd.read_json(output).sort_values(SORTING_COLS).reset_index()

        # Set responseDF columns to be ordered in the same way as expectedDF
        responseDF = responseDF.reindex(expectedDF.columns, axis=1)

        responseDF = responseDF.round(5)
        expectedDF = expectedDF.round(5)

        assert_frame_equal(responseDF, expectedDF)

    @mock.patch('outlier_aggregation_wrangler.boto3')
    def test_wrangler_exception_handling(self, mock_boto):
        response = outlier_aggregation_wrangler.lambda_handler(None, None)
        assert not response['success']

    @mock.patch('outlier_aggregation_method.boto3')
    def test_method_exception_handling(self, mock_boto):
        json_content = '[{"movement_Q601_asphalting_sand":0.0},{"movement_Q601_asphalting_sand":0.857614899}]'

        response = outlier_aggregation_method.lambda_handler(json_content, None)
        assert not response['success']

    @mock.patch('outlier_aggregation_wrangler.boto3')
    def test_wrangler_success_responses(self, mock_boto):
        with mock.patch('outlier_aggregation_wrangler.json') as mock_json:
            response = outlier_aggregation_wrangler.lambda_handler(None, None)
            assert mock_json.dumps.call_args[0][0]['success']
            assert response


if __name__ == '__main__':
    unittest.main
