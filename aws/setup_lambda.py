#!/usr/bin/env python3
"""
Script to create and deploy the Lambda function for processing Kinesis events.
"""

import boto3
import json
import logging
import os
import time
import zipfile
import io
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS clients
iam = boto3.client('iam')
lambda_client = boto3.client('lambda')

def create_lambda_role():
    """Create IAM role for Lambda with necessary permissions."""
    try:
        # Create the IAM role
        role_name = 'ProcessKinesisEventsRole'
        
        # Basic Lambda trust policy
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        
        try:
            role = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy)
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logger.info(f"Role {role_name} already exists")
                role = iam.get_role(RoleName=role_name)
            else:
                raise
        
        # Attach necessary policies
        policy_arns = [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
            'arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole'
        ]
        
        # Create custom policy for DynamoDB access
        dynamodb_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "dynamodb:PutItem",
                    "dynamodb:GetItem",
                    "dynamodb:UpdateItem"
                ],
                "Resource": "arn:aws:dynamodb:*:*:table/PlaybackEvents"
            }]
        }
        
        try:
            custom_policy = iam.create_policy(
                PolicyName='LambdaDynamoDBAccess',
                PolicyDocument=json.dumps(dynamodb_policy)
            )
            policy_arns.append(custom_policy['Policy']['Arn'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                account_id = boto3.client('sts').get_caller_identity()['Account']
                policy_arns.append(f'arn:aws:iam::{account_id}:policy/LambdaDynamoDBAccess')
            else:
                raise
        
        # Attach all policies to role
        for policy_arn in policy_arns:
            try:
                iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'EntityAlreadyExists':
                    raise
        
        # Wait for role to be ready
        logger.info("Waiting for role to be ready...")
        time.sleep(10)
        
        return role['Role']['Arn']
        
    except Exception as e:
        logger.error(f"Error creating IAM role: {str(e)}")
        raise

def create_lambda_function(role_arn):
    """Create or update the Lambda function."""
    try:
        # Create a ZIP file in memory containing the Lambda function code
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Read the Lambda function code
            with open('aws/lambda_function.py', 'r') as f:
                lambda_code = f.read()
            # Add the code to the ZIP file
            zip_file.writestr('lambda_function.py', lambda_code)
        
        # Get the ZIP file bytes
        zip_buffer.seek(0)
        zip_bytes = zip_buffer.read()
        
        function_name = 'ProcessKinesisEvents'
        
        try:
            # Try to create new function
            response = lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role=role_arn,
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_bytes},
                Description='Process events from Kinesis and store in DynamoDB',
                Timeout=300,
                MemorySize=256,
                Publish=True
            )
            logger.info(f"Created Lambda function: {function_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                # Update existing function
                response = lambda_client.update_function_code(
                    FunctionName=function_name,
                    ZipFile=zip_bytes
                )
                logger.info(f"Updated Lambda function: {function_name}")
            else:
                raise
        
        # Add Kinesis trigger
        try:
            stream_arn = boto3.client('kinesis').describe_stream(
                StreamName='AudiobookPlaybackStream'
            )['StreamDescription']['StreamARN']
            
            lambda_client.create_event_source_mapping(
                EventSourceArn=stream_arn,
                FunctionName=function_name,
                StartingPosition='LATEST',
                BatchSize=100
            )
            logger.info("Added Kinesis trigger to Lambda function")
            
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceConflictException':
                raise
            logger.info("Kinesis trigger already exists")
        
        return response['FunctionArn']
        
    except Exception as e:
        logger.error(f"Error creating Lambda function: {str(e)}")
        raise

def main():
    """Main function to set up the Lambda function and its dependencies."""
    try:
        # Create IAM role
        logger.info("Creating IAM role...")
        role_arn = create_lambda_role()
        
        # Create Lambda function
        logger.info("Creating Lambda function...")
        function_arn = create_lambda_function(role_arn)
        
        logger.info("Setup complete!")
        logger.info(f"Lambda function ARN: {function_arn}")
        
    except Exception as e:
        logger.error(f"Setup failed: {str(e)}")
        raise

if __name__ == '__main__':
    main() 