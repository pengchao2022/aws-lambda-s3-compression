#!/bin/bash
# deploy.sh - Script to create Lambda deployment package

# Create a temporary directory for packaging
mkdir -p package

# Copy the Lambda function code
cp lambda_function.py package/

# Install dependencies into package directory
pip install -r requirements.txt -t package/ --break-system-package

# Create ZIP file
cd package
zip -r ../s3-compression-lambda.zip .
cd ..

# Clean up
rm -rf package

echo "Deployment package created: s3-compression-lambda.zip"