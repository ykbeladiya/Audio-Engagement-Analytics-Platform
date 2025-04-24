#!/usr/bin/env python3
"""
Script to create and configure DynamoDB table for audiobook playback events.
Includes primary key on event_id and secondary indexes on user_id and book_id.
"""

import logging
import os
import time
import argparse
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_CONFIG = {
    'table_name': 'PlaybackEvents',
    'region': 'us-east-1',
    'read_capacity': 5,
    'write_capacity': 5
}

class DynamoDBSetup:
    """Handles DynamoDB table creation and configuration."""
    
    def __init__(
        self,
        table_name: str = DEFAULT_CONFIG['table_name'],
        region: str = DEFAULT_CONFIG['region'],
        read_capacity: int = DEFAULT_CONFIG['read_capacity'],
        write_capacity: int = DEFAULT_CONFIG['write_capacity']
    ):
        self.table_name = table_name
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity
        
        # Initialize AWS clients
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.client = boto3.client('dynamodb', region_name=region)
        
    def create_table(self) -> bool:
        """
        Create DynamoDB table with required schema and indexes.
        
        Returns:
            bool: True if table was created or already exists
        """
        try:
            # Check if table already exists
            existing_tables = self.client.list_tables()['TableNames']
            if self.table_name in existing_tables:
                logger.info(f"Table {self.table_name} already exists")
                return True
            
            # Create table with primary key and GSIs
            logger.info(f"Creating table {self.table_name}...")
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'event_id',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'event_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'user_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'book_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'timestamp',
                        'AttributeType': 'S'
                    }
                ],
                GlobalSecondaryIndexes=[
                    # Index for querying by user_id and timestamp
                    {
                        'IndexName': 'UserEventsIndex',
                        'KeySchema': [
                            {
                                'AttributeName': 'user_id',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'timestamp',
                                'KeyType': 'RANGE'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': self.read_capacity,
                            'WriteCapacityUnits': self.write_capacity
                        }
                    },
                    # Index for querying by book_id and timestamp
                    {
                        'IndexName': 'BookEventsIndex',
                        'KeySchema': [
                            {
                                'AttributeName': 'book_id',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'timestamp',
                                'KeyType': 'RANGE'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': self.read_capacity,
                            'WriteCapacityUnits': self.write_capacity
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': self.read_capacity,
                    'WriteCapacityUnits': self.write_capacity
                },
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'AudioEngagementAnalytics'
                    }
                ]
            )
            
            # Wait for table to be created
            logger.info("Waiting for table to become active...")
            waiter = self.client.get_waiter('table_exists')
            waiter.wait(
                TableName=self.table_name,
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': 24  # Wait up to 2 minutes
                }
            )
            
            # Enable TTL for data lifecycle management
            logger.info("Enabling TTL...")
            self.client.update_time_to_live(
                TableName=self.table_name,
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': 'ttl'
                }
            )
            
            logger.info(f"Table {self.table_name} created successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create table: {e}")
            return False
            
    def describe_table(self) -> Dict[str, Any]:
        """
        Get table description and configuration.
        
        Returns:
            Dict containing table details
        """
        try:
            response = self.client.describe_table(TableName=self.table_name)
            return response['Table']
        except ClientError as e:
            logger.error(f"Failed to describe table: {e}")
            return {}
            
    def delete_table(self) -> bool:
        """
        Delete the DynamoDB table.
        
        Returns:
            bool: True if table was deleted successfully
        """
        try:
            logger.info(f"Deleting table {self.table_name}...")
            self.client.delete_table(TableName=self.table_name)
            
            # Wait for table to be deleted
            waiter = self.client.get_waiter('table_not_exists')
            waiter.wait(
                TableName=self.table_name,
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': 24
                }
            )
            
            logger.info(f"Table {self.table_name} deleted successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete table: {e}")
            return False

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Set up DynamoDB table for audiobook playback events')
    parser.add_argument('--table-name', default=DEFAULT_CONFIG['table_name'],
                      help='Name of the DynamoDB table')
    parser.add_argument('--region', default=DEFAULT_CONFIG['region'],
                      help='AWS region')
    parser.add_argument('--read-capacity', type=int, default=DEFAULT_CONFIG['read_capacity'],
                      help='Read capacity units')
    parser.add_argument('--write-capacity', type=int, default=DEFAULT_CONFIG['write_capacity'],
                      help='Write capacity units')
    parser.add_argument('--delete', action='store_true',
                      help='Delete the table instead of creating it')
    return parser.parse_args()

def main():
    """Main function to set up DynamoDB table."""
    # Parse command line arguments
    args = parse_args()
    
    # Initialize setup
    setup = DynamoDBSetup(
        table_name=args.table_name,
        region=args.region,
        read_capacity=args.read_capacity,
        write_capacity=args.write_capacity
    )
    
    if args.delete:
        # Delete table if requested
        if setup.delete_table():
            logger.info("Table deletion completed")
        else:
            logger.error("Table deletion failed")
    else:
        # Create table
        if setup.create_table():
            # Get and display table details
            table_details = setup.describe_table()
            if table_details:
                logger.info("\nTable configuration:")
                logger.info(f"Status: {table_details.get('TableStatus')}")
                logger.info(f"Items: {table_details.get('ItemCount', 0)}")
                logger.info(f"Size (bytes): {table_details.get('TableSizeBytes', 0)}")
                logger.info("Indexes:")
                for gsi in table_details.get('GlobalSecondaryIndexes', []):
                    logger.info(f"  - {gsi['IndexName']}")
        else:
            logger.error("Failed to set up DynamoDB table")

if __name__ == "__main__":
    main() 