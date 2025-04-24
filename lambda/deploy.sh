#!/bin/bash

# Configuration
ENVIRONMENT="dev"
STACK_NAME="audio-analytics-${ENVIRONMENT}"
REGION="us-east-1"
S3_BUCKET="your-deployment-bucket"  # Replace with your S3 bucket
S3_PREFIX="lambda-functions"

# Create deployment package
echo "Creating deployment package..."
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create zip file
zip -r function.zip process_events.py
cd .venv/lib/python3.9/site-packages
zip -r ../../../../function.zip .
cd ../../../../

# Upload to S3
echo "Uploading Lambda package to S3..."
aws s3 cp function.zip "s3://${S3_BUCKET}/${S3_PREFIX}/function.zip"

# Deploy CloudFormation stack
echo "Deploying CloudFormation stack..."
aws cloudformation deploy \
    --template-file cloudformation.yaml \
    --stack-name ${STACK_NAME} \
    --parameter-overrides \
        Environment=${ENVIRONMENT} \
        LambdaS3Bucket=${S3_BUCKET} \
        LambdaS3Key="${S3_PREFIX}/function.zip" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region ${REGION}

# Get stack outputs
echo "Stack outputs:"
aws cloudformation describe-stacks \
    --stack-name ${STACK_NAME} \
    --query 'Stacks[0].Outputs' \
    --output table \
    --region ${REGION}

echo "Deployment complete!" 