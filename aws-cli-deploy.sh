#!/bin/bash

# Configuration
FUNCTION_NAME="s3-compression"
SOURCE_BUCKET="your-source-bucket"
ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-s3-role"
REGION="us-east-1"

# Create deployment package
./deploy.sh

# Create or update Lambda function
aws lambda create-function \
    --function-name $FUNCTION_NAME \
    --runtime python3.9 \
    --role $ROLE_ARN \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://s3-compression-lambda.zip \
    --timeout 900 \
    --memory-size 1024 \
    --environment Variables="{SOURCE_BUCKET=$SOURCE_BUCKET,MINUTES_BACK=5}" \
    --region $REGION

# If function already exists, update it
# aws lambda update-function-code \
#     --function-name $FUNCTION_NAME \
#     --zip-file fileb://s3-compression-lambda.zip \
#     --region $REGION

echo "Lambda function deployed successfully"