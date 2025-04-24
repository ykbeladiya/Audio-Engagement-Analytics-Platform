#!/usr/bin/env python3
"""
Script to send audiobook playback events to AWS Kinesis stream.
Handles both stream creation and data ingestion.
"""

import json
import time
import sys
import logging
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import datetime

# Add parent directory to path to import from events package
sys.path.append(str(Path(__file__).parent.parent))
from events.simulate_playback_events import EventGenerator

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger(__name__)

class KinesisStreamManager:
    """Manages interactions with AWS Kinesis stream."""
    
    def __init__(
        self,
        stream_name: str,
        region: str = os.getenv('AWS_REGION', 'us-east-1'),
        profile: Optional[str] = os.getenv('AWS_PROFILE', None)
    ):
        self.stream_name = stream_name
        self.region = region
        
        # Initialize boto3 session with optional profile
        session = boto3.Session(profile_name=profile, region_name=region)
        self.kinesis_client = session.client('kinesis')
        
    def create_stream(self, shard_count: int = 1, wait: bool = True) -> bool:
        """
        Create a Kinesis stream if it doesn't exist.
        Args:
            shard_count: Number of shards for the stream
            wait: Whether to wait for the stream to become active
        Returns:
            bool: True if stream was created or already exists
        """
        try:
            # Check if stream already exists
            try:
                self.kinesis_client.describe_stream(StreamName=self.stream_name)
                logger.info(f"Stream {self.stream_name} already exists")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    raise
            
            # Create the stream
            logger.info(f"Creating stream {self.stream_name} with {shard_count} shards...")
            self.kinesis_client.create_stream(
                StreamName=self.stream_name,
                ShardCount=shard_count
            )
            
            if wait:
                # Wait for the stream to become active
                logger.info("Waiting for stream to become active...")
                waiter = self.kinesis_client.get_waiter('stream_exists')
                waiter.wait(
                    StreamName=self.stream_name,
                    WaiterConfig={'Delay': 2, 'MaxAttempts': 30}
                )
                logger.info(f"Stream {self.stream_name} is now active")
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create stream: {e}")
            return False
    
    def log_sample_event(self, event: Dict):
        """Log a sample event to show the exact format being sent to Kinesis."""
        logger.info("\nSample event format:")
        logger.info("-" * 50)
        logger.info("Raw event dictionary:")
        logger.info(json.dumps(event, indent=2))
        
        logger.info("\nKinesis record format:")
        record = {
            'Data': json.dumps(event).encode('utf-8'),
            'PartitionKey': event['user_id']
        }
        logger.info(json.dumps({
            'Data': json.dumps(event),  # Show as string instead of bytes for readability
            'PartitionKey': record['PartitionKey']
        }, indent=2))
        logger.info("-" * 50)
    
    def send_events(
        self,
        events: List[Dict],
        batch_size: int = 100,
        retry_attempts: int = 3
    ) -> bool:
        """
        Send events to Kinesis stream in batches with retry logic.
        Args:
            events: List of event dictionaries to send
            batch_size: Number of events to send in each batch
            retry_attempts: Number of times to retry failed batches
        Returns:
            bool: True if all events were sent successfully
        """
        try:
            total_events = len(events)
            sent_count = 0
            failed_records = []
            
            # Log the first event as a sample
            if events:
                self.log_sample_event(events[0])
            
            # Process events in batches
            for i in range(0, total_events, batch_size):
                batch = events[i:i + batch_size]
                records = [
                    {
                        'Data': json.dumps(event).encode('utf-8'),
                        'PartitionKey': event['user_id']
                    }
                    for event in batch
                ]
                
                # Try to send batch with retries
                for attempt in range(retry_attempts):
                    try:
                        response = self.kinesis_client.put_records(
                            Records=records,
                            StreamName=self.stream_name
                        )
                        
                        failed_count = response.get('FailedRecordCount', 0)
                        if failed_count == 0:
                            sent_count += len(batch)
                            break
                        elif attempt < retry_attempts - 1:
                            logger.warning(
                                f"Attempt {attempt + 1}: {failed_count} records failed. Retrying..."
                            )
                            # Only retry failed records
                            failed_indices = [
                                idx for idx, record in enumerate(response['Records'])
                                if 'ErrorCode' in record
                            ]
                            records = [records[idx] for idx in failed_indices]
                        else:
                            failed_records.extend(records)
                    
                    except ClientError as e:
                        if attempt < retry_attempts - 1:
                            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                            time.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            failed_records.extend(records)
                            logger.error(f"Failed to send batch after {retry_attempts} attempts")
                
                # Log progress
                logger.info(f"Progress: {sent_count}/{total_events} events sent")
            
            if failed_records:
                logger.error(f"Failed to send {len(failed_records)} records after all retries")
                return False
            
            logger.info(f"Successfully sent all {sent_count} events to Kinesis")
            return True
            
        except Exception as e:
            logger.error(f"Unexpected error while sending events: {e}")
            return False

class KinesisEventPublisher:
    """Class for publishing events to Kinesis stream."""
    
    def __init__(self, stream_name: str, region: str, client: Optional[Any] = None):
        """
        Initialize the Kinesis event publisher.
        
        Args:
            stream_name: Name of the Kinesis stream
            region: AWS region
            client: Optional boto3 Kinesis client for testing
        """
        self.stream_name = stream_name
        self.region = region
        self.client = client or boto3.client('kinesis', region_name=region)
        self.max_retries = 3
        self.retry_delay = 1  # seconds
        
    def send_event(self, event: Any) -> Dict[str, str]:
        """
        Send a single event to Kinesis.
        
        Args:
            event: Event object with to_dict method
            
        Returns:
            Dict containing ShardId and SequenceNumber
            
        Raises:
            ValueError: If event has invalid partition key
            ClientError: If AWS API call fails
        """
        if not event.user_id:
            raise ValueError("Event must have a valid user_id for partition key")
            
        event_data = event.to_dict()
        
        for attempt in range(self.max_retries):
            try:
                response = self.client.put_record(
                    StreamName=self.stream_name,
                    Data=json.dumps(event_data),
                    PartitionKey=event.user_id
                )
                logger.debug(f"Successfully sent event {event.event_id} to Kinesis")
                return {
                    'ShardId': response['ShardId'],
                    'SequenceNumber': response['SequenceNumber']
                }
            except ClientError as e:
                if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))
                        continue
                logger.error(f"Failed to send event {event.event_id}: {str(e)}")
                raise
                
    def send_events_batch(self, events: List[Any], batch_size: int = 500) -> List[Dict[str, str]]:
        """
        Send multiple events to Kinesis in batches.
        
        Args:
            events: List of event objects
            batch_size: Maximum number of events per batch (default: 500)
            
        Returns:
            List of response dicts containing ShardId and SequenceNumber
            
        Raises:
            ValueError: If batch size exceeds limits
            ClientError: If AWS API call fails
        """
        if len(events) > batch_size:
            raise ValueError(f"Batch size cannot exceed {batch_size} events")
            
        responses = []
        records = []
        
        for event in events:
            if not event.user_id:
                raise ValueError("All events must have valid user_ids for partition keys")
                
            event_data = event.to_dict()
            records.append({
                'Data': json.dumps(event_data),
                'PartitionKey': event.user_id
            })
            
        for attempt in range(self.max_retries):
            try:
                response = self.client.put_records(
                    StreamName=self.stream_name,
                    Records=records
                )
                
                # Check for failed records
                if response['FailedRecordCount'] > 0:
                    failed_records = []
                    for i, record in enumerate(response['Records']):
                        if 'ErrorCode' in record:
                            failed_records.append(events[i])
                    
                    if attempt < self.max_retries - 1:
                        logger.warning(f"Retrying {len(failed_records)} failed records")
                        time.sleep(self.retry_delay * (2 ** attempt))
                        events = failed_records
                        continue
                        
                logger.info(f"Successfully sent {len(events)} events to Kinesis")
                return [{
                    'ShardId': record['ShardId'],
                    'SequenceNumber': record['SequenceNumber']
                } for record in response['Records'] if 'ShardId' in record]
                
            except ClientError as e:
                logger.error(f"Failed to send batch of {len(events)} events: {str(e)}")
                raise
                
def main():
    # Configuration
    STREAM_NAME = os.getenv('KINESIS_STREAM_NAME', 'AudiobookPlaybackStream')
    REGION = os.getenv('AWS_REGION', 'us-east-1')
    NUM_EVENTS = int(os.getenv('NUM_EVENTS', '1000'))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
    SHARD_COUNT = int(os.getenv('SHARD_COUNT', '1'))
    
    # Initialize Kinesis manager
    kinesis_manager = KinesisStreamManager(STREAM_NAME, REGION)
    
    # Create stream if it doesn't exist
    if not kinesis_manager.create_stream(shard_count=SHARD_COUNT):
        logger.error("Failed to create/verify Kinesis stream")
        return
    
    # Generate events
    logger.info(f"Generating {NUM_EVENTS} events...")
    start_date = datetime.datetime.now()
    end_date = start_date + datetime.timedelta(days=1)
    generator = EventGenerator(
        num_users=50,
        num_books=100,
        start_date=start_date,
        end_date=end_date
    )
    events = generator.generate_events(NUM_EVENTS)
    events_dict = [event.to_dict() for event in events]
    
    # Send events to Kinesis
    logger.info("Sending events to Kinesis...")
    if kinesis_manager.send_events(events_dict, batch_size=BATCH_SIZE):
        logger.info("Successfully completed sending events to Kinesis")
    else:
        logger.error("Failed to send some events to Kinesis")

if __name__ == "__main__":
    main() 