#!/usr/bin/env python3
"""
Script to create and configure IAM role for the Lambda function.
Sets up necessary permissions for Kinesis, DynamoDB, and S3 access.
"""

import json
import logging
import os
from typing import Dict, Optional

import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
ROLE_NAME = 'AudiobookEventsProcessorRole'
POLICY_NAME = 'AudiobookEventsProcessorPolicy'

class IAMSetup:
    """Handles IAM role and policy creation."""
    
    def __init__(self, region: str = 'us-east-1'):
        self.iam = boto3.client('iam', region_name=region)
        
    def create_role(self) -> Optional[str]:
        """
        Create IAM role for Lambda function.
        
        Returns:
            Role ARN if successful, None otherwise
        """
        try:
            # Check if role already exists
            try:
                response = self.iam.get_role(RoleName=ROLE_NAME)
                logger.info(f"Role {ROLE_NAME} already exists")
                return response['Role']['Arn']
            except ClientError:
                pass
            
            # Create trust policy for Lambda
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            # Create role
            response = self.iam.create_role(
                RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='Role for Audiobook Events Processor Lambda function'
            )
            
            role_arn = response['Role']['Arn']
            logger.info(f"Created role: {role_arn}")
            
            return role_arn
            
        except ClientError as e:
            logger.error(f"Failed to create role: {e}")
            return None
            
    def attach_policy(self, role_arn: str) -> bool:
        """
        Create and attach policy to the IAM role.
        
        Args:
            role_arn: ARN of the role to attach policy to
            
        Returns:
            bool: True if successful
        """
        try:
            # Load policy document
            policy_path = os.path.join(os.path.dirname(__file__), 'iam_role.json')
            with open(policy_path, 'r') as f:
                policy_doc = json.load(f)
            
            # Check if policy exists
            try:
                response = self.iam.get_policy(
                    PolicyArn=f"arn:aws:iam::*:policy/{POLICY_NAME}"
                )
                policy_arn = response['Policy']['Arn']
                logger.info(f"Policy {POLICY_NAME} already exists")
            except ClientError:
                # Create policy
                response = self.iam.create_policy(
                    PolicyName=POLICY_NAME,
                    PolicyDocument=json.dumps(policy_doc),
                    Description='Policy for Audiobook Events Processor Lambda function'
                )
                policy_arn = response['Policy']['Arn']
                logger.info(f"Created policy: {policy_arn}")
            
            # Attach policy to role
            self.iam.attach_role_policy(
                RoleName=ROLE_NAME,
                PolicyArn=policy_arn
            )
            logger.info(f"Attached policy to role {ROLE_NAME}")
            
            # Also attach AWS Lambda basic execution role
            self.iam.attach_role_policy(
                RoleName=ROLE_NAME,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            )
            logger.info("Attached Lambda basic execution role")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to attach policy: {e}")
            return False
            
    def cleanup(self) -> bool:
        """
        Clean up IAM role and policy.
        
        Returns:
            bool: True if successful
        """
        try:
            # Detach policies
            for policy in self.iam.list_attached_role_policies(RoleName=ROLE_NAME)['AttachedPolicies']:
                self.iam.detach_role_policy(
                    RoleName=ROLE_NAME,
                    PolicyArn=policy['PolicyArn']
                )
                logger.info(f"Detached policy: {policy['PolicyArn']}")
            
            # Delete custom policy
            policy_arn = f"arn:aws:iam::*:policy/{POLICY_NAME}"
            try:
                self.iam.delete_policy(PolicyArn=policy_arn)
                logger.info(f"Deleted policy: {policy_arn}")
            except ClientError:
                pass
            
            # Delete role
            self.iam.delete_role(RoleName=ROLE_NAME)
            logger.info(f"Deleted role: {ROLE_NAME}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to cleanup IAM resources: {e}")
            return False

def main():
    """Main function to set up IAM role and policy."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Set up IAM role and policy for Lambda function'
    )
    parser.add_argument('--region', default='us-east-1',
                      help='AWS region')
    parser.add_argument('--cleanup', action='store_true',
                      help='Clean up IAM resources instead of creating them')
    args = parser.parse_args()
    
    setup = IAMSetup(region=args.region)
    
    if args.cleanup:
        if setup.cleanup():
            logger.info("Successfully cleaned up IAM resources")
        else:
            logger.error("Failed to clean up IAM resources")
    else:
        # Create role and attach policy
        role_arn = setup.create_role()
        if role_arn and setup.attach_policy(role_arn):
            logger.info("Successfully set up IAM role and policy")
            logger.info(f"Role ARN: {role_arn}")
        else:
            logger.error("Failed to set up IAM role and policy")

if __name__ == "__main__":
    main() 