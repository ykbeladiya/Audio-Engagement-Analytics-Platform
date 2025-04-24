"""
Pytest configuration file with common fixtures for testing.
"""

import os
import pytest
import boto3
from moto import mock_aws

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def mock_aws_services(aws_credentials):
    """Mock all AWS services used in the project."""
    with mock_aws():
        yield

@pytest.fixture
def cloudwatch_client(mock_aws_services):
    """Create a mock CloudWatch client."""
    return boto3.client('cloudwatch', region_name='us-east-1')

@pytest.fixture
def kinesis_client(mock_aws_services):
    """Create a mock Kinesis client."""
    return boto3.client('kinesis', region_name='us-east-1')

@pytest.fixture
def dynamodb_client(mock_aws_services):
    """Create a mock DynamoDB client."""
    return boto3.client('dynamodb', region_name='us-east-1')

@pytest.fixture
def s3_client(mock_aws_services):
    """Create a mock S3 client."""
    return boto3.client('s3', region_name='us-east-1') 