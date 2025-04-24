#!/usr/bin/env python3
"""
Script to set up AWS Athena table and run analytics queries over audiobook playback events.
Creates table schema for JSON data stored in S3 and provides sample analytics queries.
"""

import logging
import os
import time
from typing import Dict, List, Optional
import argparse

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
    'database_name': 'audiobook_analytics',
    'table_name': 'playback_events',
    's3_bucket': 'audio-engagement-data',
    's3_prefix': 'audiobook-events',
    'output_location': 's3://audio-engagement-data/athena-results/',
    'region': 'us-east-1'
}

class AthenaSetup:
    """Handles AWS Athena table creation and query execution."""
    
    def __init__(
        self,
        database_name: str = DEFAULT_CONFIG['database_name'],
        table_name: str = DEFAULT_CONFIG['table_name'],
        s3_bucket: str = DEFAULT_CONFIG['s3_bucket'],
        s3_prefix: str = DEFAULT_CONFIG['s3_prefix'],
        output_location: str = DEFAULT_CONFIG['output_location'],
        region: str = DEFAULT_CONFIG['region']
    ):
        self.database_name = database_name
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.output_location = output_location
        
        # Initialize Athena client
        self.athena = boto3.client('athena', region_name=region)
        
    def create_database(self) -> bool:
        """Create Athena database if it doesn't exist."""
        try:
            query = f"""
            CREATE DATABASE IF NOT EXISTS {self.database_name}
            COMMENT 'Database for audiobook engagement analytics'
            """
            self.run_query(query)
            logger.info(f"Created database: {self.database_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            return False
            
    def create_table(self) -> bool:
        """Create Athena table with JSON schema for playback events."""
        try:
            # Drop existing table if it exists
            self.run_query(f"DROP TABLE IF EXISTS {self.database_name}.{self.table_name}")
            
            # Create table with JSON SerDe
            query = f"""
            CREATE EXTERNAL TABLE {self.database_name}.{self.table_name} (
                event_id STRING,
                user_id STRING,
                book_id STRING,
                event_type STRING,
                timestamp TIMESTAMP,
                position INT,
                chapter INT,
                processed_at TIMESTAMP,
                metadata STRUCT<
                    device_type: STRING,
                    app_version: STRING,
                    network_type: STRING
                >
            )
            PARTITIONED BY (
                year STRING,
                month STRING,
                day STRING
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            LOCATION 's3://{self.s3_bucket}/{self.s3_prefix}/'
            TBLPROPERTIES (
                'has_encrypted_data'='false',
                'projection.enabled'='true',
                'projection.year.type'='integer',
                'projection.year.range'='2024,2030',
                'projection.month.type'='integer',
                'projection.month.range'='1,12',
                'projection.month.digits'='2',
                'projection.day.type'='integer',
                'projection.day.range'='1,31',
                'projection.day.digits'='2'
            )
            """
            self.run_query(query)
            logger.info(f"Created table: {self.table_name}")
            
            # Add partitions
            self.run_query("MSCK REPAIR TABLE {self.database_name}.{self.table_name}")
            logger.info("Added partitions")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
            
    def run_query(self, query: str) -> Optional[str]:
        """
        Execute Athena query and wait for completion.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query execution ID if successful, None otherwise
        """
        try:
            # Start query execution
            response = self.athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': self.database_name
                },
                ResultConfiguration={
                    'OutputLocation': self.output_location
                }
            )
            query_execution_id = response['QueryExecutionId']
            
            # Wait for query to complete
            while True:
                response = self.athena.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                state = response['QueryExecution']['Status']['State']
                
                if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    if state == 'SUCCEEDED':
                        logger.info(f"Query completed successfully: {query_execution_id}")
                        return query_execution_id
                    else:
                        error = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                        logger.error(f"Query failed: {error}")
                        return None
                        
                time.sleep(1)  # Wait before checking again
                
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return None
            
    def run_sample_queries(self) -> None:
        """Run sample analytics queries."""
        try:
            # 1. Most skipped books
            skipped_books_query = f"""
            WITH skips AS (
                SELECT 
                    book_id,
                    COUNT(*) as skip_count
                FROM {self.database_name}.{self.table_name}
                WHERE event_type = 'skip'
                GROUP BY book_id
            )
            SELECT 
                book_id,
                skip_count,
                RANK() OVER (ORDER BY skip_count DESC) as rank
            FROM skips
            ORDER BY skip_count DESC
            LIMIT 10
            """
            logger.info("Running query for most skipped books...")
            self.run_query(skipped_books_query)
            
            # 2. Average session time per user
            session_time_query = f"""
            WITH session_events AS (
                SELECT 
                    user_id,
                    book_id,
                    timestamp,
                    event_type,
                    LAG(timestamp) OVER (
                        PARTITION BY user_id, book_id 
                        ORDER BY timestamp
                    ) as prev_event_time
                FROM {self.database_name}.{self.table_name}
                WHERE event_type IN ('start', 'pause', 'resume', 'complete')
            )
            SELECT 
                user_id,
                AVG(
                    CAST(
                        DATE_DIFF('second', prev_event_time, timestamp) 
                        AS DOUBLE
                    )
                ) as avg_session_seconds
            FROM session_events
            WHERE 
                prev_event_time IS NOT NULL
                AND DATE_DIFF('hour', prev_event_time, timestamp) < 4
            GROUP BY user_id
            ORDER BY avg_session_seconds DESC
            LIMIT 10
            """
            logger.info("Running query for average session time...")
            self.run_query(session_time_query)
            
            # 3. Completion rate per book
            completion_rate_query = f"""
            WITH book_stats AS (
                SELECT 
                    book_id,
                    COUNT(DISTINCT user_id) as total_users,
                    COUNT(DISTINCT CASE 
                        WHEN event_type = 'complete' 
                        THEN user_id 
                    END) as completed_users
                FROM {self.database_name}.{self.table_name}
                GROUP BY book_id
            )
            SELECT 
                book_id,
                total_users,
                completed_users,
                CAST(completed_users AS DOUBLE) / CAST(total_users AS DOUBLE) * 100 as completion_rate
            FROM book_stats
            WHERE total_users >= 10
            ORDER BY completion_rate DESC
            LIMIT 10
            """
            logger.info("Running query for book completion rates...")
            self.run_query(completion_rate_query)
            
        except Exception as e:
            logger.error(f"Failed to run sample queries: {e}")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Set up AWS Athena table and run analytics queries'
    )
    parser.add_argument('--database', default=DEFAULT_CONFIG['database_name'],
                      help='Athena database name')
    parser.add_argument('--table', default=DEFAULT_CONFIG['table_name'],
                      help='Athena table name')
    parser.add_argument('--bucket', default=DEFAULT_CONFIG['s3_bucket'],
                      help='S3 bucket name')
    parser.add_argument('--prefix', default=DEFAULT_CONFIG['s3_prefix'],
                      help='S3 prefix for event data')
    parser.add_argument('--output', default=DEFAULT_CONFIG['output_location'],
                      help='S3 location for query results')
    parser.add_argument('--region', default=DEFAULT_CONFIG['region'],
                      help='AWS region')
    parser.add_argument('--run-queries', action='store_true',
                      help='Run sample analytics queries')
    return parser.parse_args()

def main():
    """Main function to set up Athena and run queries."""
    args = parse_args()
    
    setup = AthenaSetup(
        database_name=args.database,
        table_name=args.table,
        s3_bucket=args.bucket,
        s3_prefix=args.prefix,
        output_location=args.output,
        region=args.region
    )
    
    # Create database and table
    if setup.create_database() and setup.create_table():
        logger.info("Athena setup completed successfully")
        
        # Run sample queries if requested
        if args.run_queries:
            logger.info("Running sample analytics queries...")
            setup.run_sample_queries()
    else:
        logger.error("Athena setup failed")

if __name__ == "__main__":
    main() 