# es-qcas-outliers
A repository for the Econ Stats QCAS Outlier code

## Outlier Step Function

**Start**
When live, the step function will probably start by retrieving a JSON file from an SQS queue (though this is subject to change). However, for our testing, we didn't have data in the SQS queue and had to copy it there from an S3 bucket each time we wanted to test the step function. Hence, it made sense to create another lambda to extract the data from the S3 bucket, into the SQS queue, and incorporate this lamba into our step function.
So, the Step Function starts at "FillSQSQueue" which calls lambda "outlierS3BucketToSQS". 
We set up Environment variables for the S3 bucket name, the SQS Queue URL, and SQS Message ID name, and the name of the input JSON file, as below:-

Variable Name | Value
------------- | -------------
bucket_name   | es-algo-poc
checkpoint    | 0
input_data    | *Name of JSON file, will be removed, as input will come from some other service in production*
queue_url     | *url of queue*

 
These are passed into the lambda function.

**Failure and Success States in Step Functions**
Processing then passes to a choice state which checks if the above lambda has ended successfully, or with a failure. 
If it has ended successfully processing passes to the next function (or to the success handler in the case of the final lambda).  
If it fails, the rest of the processing is stopped and we pass directly to the failure handler. The failure handler calls lambda "outlierRuntimeErrorCapture", which publishes a failure message to the SNS queue and error details to the S3 bucket.

**Outlier Processing.**
After the initial state, which fills the SQS queue (above). We have 6 other functions/methods which run sequentially to produce a final Outlier Weight value  for each row in our dataframe.
Each one of the functions calculates components of the final outlier weight calculation. Some of these calculations could be combined into a single lambda, but we have decided to separate them out, so that each component could potentially be used in other step functions, if needed.

**Functions and Method**
Each one of the states of the step function performs essentially the same process. The step function calls a data wrangler. The wrangler gets the JSON file from the SQS queue and converts it to a dataframe.  We then add a new column to the dataframe and populate this column with zeros.  The dataframe is then converted back to a JSON and passes it to the function/method. 
The method reads the JSON file, converts it to a dataframe and then populates the new column we have created in the wrangler, with the results of the equation that the function performs.  We then convert it back to a JSON and pass it back to the wrangler. The wrangler then passes the JSON back to the SQS queue. We then pass onto the check state to determine if the function has been a success or failure. This has been noted in the above code and passed into a variable called "Success" which takes the values True or False.

## Individual Functions
**1) OutlierAggregation1  and (2) OutlierAggregation2**
These two states call the same code but with different Enviroment variables. Each of the two lambdas aggregates a column (adjusted_value and selection_data repectively) by given strata (period, classification, cell_no, question) and stores the results in a new column (sum_adjusted_value and sum_selection_data respectively)
We set up the following environment variables for each lambda

##### OutlierAggregation1

Variable Name | Value
------------- | -------------
aggregate_column   | adjusted_value
error_handler_arn    | *Error handler for failure*
strata    | period classification cell_no question
sum_aggregate_column     | sum_adjusted_value
 
##### OutlierAggregation2

Variable Name | Value
------------- | -------------
aggregate_column   | selection_data
error_handler_arn    | *Error handler for failure*
strata    | period classification cell_no question
sum_aggregate_column     | sum_adjusted_data
 
**3) OutlierCalcRatio**
This funtion calculates a ratio of the two newly created columns above (sum_adjusted_value / sum_selection_data) and puts the results in a new column (called ratio_val here).
We set up the following environment variables

Variable Name | Value
------------- | -------------
error_handler_arn    | *Error handler for failure*
ratio    | ratio_val
ratio_denominator     | sum_selection_data
ratio_numerator | sum_adjusted_value
 
**4) OutlierWinsFitted**
Now we calculate a "WinsFitted" value which, for each row of the data frame is the ratio calculated above multiplied by the selection_data column. The result is again stored in a new column (which we call winsFitted)
We set up the following environment variables

Variable Name | Value
------------- | -------------
error_handler_arn    | *Error handler for failure*
ratio    | ratio_val
selection_data     | selection_data
winsfitted_column | winsFitted

 
**5) OutlierKValue**
Now calculate the K value for each row in another column. For this, we need the input data to have an a_weight column, a g_weight column and an L value column. If these don't exist, the process will fail. Again, a new column is created (Called K_value)
The formula is:   K_Value = WinsFitted Value + (L-Value/(A_weight*G_weight - 1)) 
We set up the following environment variables

Variable Name | Value
------------- | -------------
a_weight_column   | a_weight
error_handler_arn    | *Error handler for failure*
g_weight_column   | g_weight
k_value_column     | K_value
l_value_column     | l_value
winsfitted_column | winsFitted

 
**6) OutlierOWeight**
The final method, uses the components we've calculated above to calculate the O_weight value. Again, we need the a_weight and g_weight columns and the process will fail if we don't have them. Also, again, we put the results in a new column (called O_weight)
We set up the following environment variables

Variable Name | Value
------------- | -------------
a_weight_column   | a_weight
aggregate_column   | adjusted_value
error_handler_arn    | *Error handler for failure*
g_weight_column   | g_weight
k_value_column     | K_value
o_weight_column     | O_weight
 
===============END OF DOC===================
