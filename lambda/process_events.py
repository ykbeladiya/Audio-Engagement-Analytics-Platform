#!/usr/bin/env python3
"""
AWS Lambda function to process audiobook playback events from Kinesis
and store them in DynamoDB and S3.
"""

import json
import logging
import os
import base64
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import uuid
import time
from contextlib import contextmanager

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add JSON formatter for structured logging
class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""
    
    def format(self, record):
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }
            
        # Add extra fields if present
        if hasattr(record, 'extra_data'):
            log_entry.update(record.extra_data)
            
        return json.dumps(log_entry)

# Configure JSON formatter for handler
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')

# Configuration
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'PlaybackEvents')
S3_BUCKET = os.environ.get('S3_BUCKET', 'audio-engagement-data')
S3_PREFIX = 'audiobook-events'
ALERT_SNS_TOPIC = os.environ.get('ALERT_SNS_TOPIC')
table = dynamodb.Table(DYNAMODB_TABLE)

@contextmanager
def timing_context(metric_name: str, dimensions: List[Dict[str, str]]):
    """Context manager to measure and publish execution time."""
    start_time = time.time()
    try:
        yield
    finally:
        duration = (time.time() - start_time) * 1000  # Convert to milliseconds
        publish_metric(metric_name, 'Milliseconds', duration, dimensions)

def publish_metric(name: str, unit: str, value: float, dimensions: List[Dict[str, str]]) -> None:
    """Publish metric to CloudWatch."""
    try:
        cloudwatch.put_metric_data(
            Namespace='AudiobookEvents',
            MetricData=[{
                'MetricName': name,
                'Value': value,
                'Unit': unit,
                'Dimensions': dimensions
            }]
        )
    except Exception as e:
        logger.warning(f"Failed to publish metric {name}: {e}")

def send_alert(message: str, context: Dict[str, Any]) -> None:
    """Send alert notification via SNS."""
    if ALERT_SNS_TOPIC:
        try:
            sns = boto3.client('sns')
            sns.publish(
                TopicArn=ALERT_SNS_TOPIC,
                Subject='Audiobook Events Processing Alert',
                Message=json.dumps({
                    'message': message,
                    'timestamp': datetime.utcnow().isoformat(),
                    'context': context
                }, indent=2)
            )
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

def get_s3_key(timestamp: str) -> str:
    """
    Generate S3 key based on timestamp using yyyy/mm/dd structure.
    
    Args:
        timestamp: ISO format timestamp string
        
    Returns:
        S3 key path
    """
    try:
        # Parse timestamp and create path
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return f"{S3_PREFIX}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{str(uuid.uuid4())}.json"
    except ValueError as e:
        logger.error(f"Invalid timestamp format: {e}")
        # Fallback to current date if timestamp parsing fails
        dt = datetime.utcnow()
        return f"{S3_PREFIX}/{dt.year}/{dt.month:02d}/{dt.day:02d}/error_{str(uuid.uuid4())}.json"

def store_in_s3(events: List[Dict[str, Any]]) -> bool:
    """
    Store batch of events in S3 using date-based partitioning.
    
    Args:
        events: List of event dictionaries to store
        
    Returns:
        bool: True if all events were stored successfully
    """
    try:
        # Group events by date
        date_groups: Dict[str, List[Dict]] = {}
        for event in events:
            s3_key = get_s3_key(event['timestamp'])
            if s3_key not in date_groups:
                date_groups[s3_key] = []
            date_groups[s3_key].append(event)
        
        # Store each group in S3
        for s3_key, event_group in date_groups.items():
            try:
                with timing_context('S3WriteLatency', [{'Name': 'Bucket', 'Value': S3_BUCKET}]):
                    s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=s3_key,
                        Body=json.dumps(event_group, indent=2),
                        ContentType='application/json'
                    )
                logger.info(
                    "Stored events in S3",
                    extra={'extra_data': {
                        'bucket': S3_BUCKET,
                        'key': s3_key,
                        'event_count': len(event_group)
                    }}
                )
            except ClientError as e:
                logger.error(
                    "Failed to store events in S3",
                    extra={'extra_data': {
                        'error': str(e),
                        'bucket': S3_BUCKET,
                        'key': s3_key
                    }}
                )
                send_alert(
                    f"Failed to store events in S3: {e}",
                    {'bucket': S3_BUCKET, 'key': s3_key}
                )
                return False
        
        return True
        
    except Exception as e:
        logger.error(
            "Unexpected error storing events in S3",
            extra={'extra_data': {'error': str(e)}}
        )
        send_alert(
            f"Unexpected error storing events in S3: {e}",
            {'bucket': S3_BUCKET}
        )
        return False

def parse_kinesis_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single Kinesis record and extract relevant fields.
    
    Args:
        record: Raw Kinesis record
        
    Returns:
        Dict containing parsed event data
        
    Raises:
        ValueError: If record data is invalid or missing required fields
    """
    try:
        # Decode and parse the record data
        payload = base64.b64decode(record['kinesis']['data'])
        event_data = json.loads(payload)
        
        # Extract required fields
        required_fields = ['user_id', 'book_id', 'event_type', 'timestamp', 'position']
        if not all(field in event_data for field in required_fields):
            missing_fields = [f for f in required_fields if f not in event_data]
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Create DynamoDB item
        item = {
            'event_id': event_data.get('event_id', f"{event_data['user_id']}_{event_data['timestamp']}"),
            'user_id': event_data['user_id'],
            'book_id': event_data['book_id'],
            'event_type': event_data['event_type'],
            'timestamp': event_data['timestamp'],
            'position': event_data['position'],
            'chapter': event_data.get('chapter'),  # Optional field
            'processed_at': datetime.utcnow().isoformat(),
        }
        
        # Add any metadata if present
        if 'metadata' in event_data:
            item['metadata'] = event_data['metadata']
            
        return item
        
    except json.JSONDecodeError as e:
        logger.error(
            "Invalid JSON in record data",
            extra={'extra_data': {'error': str(e)}}
        )
        raise ValueError(f"Invalid JSON in record data: {e}")
    except KeyError as e:
        logger.error(
            "Missing required Kinesis record field",
            extra={'extra_data': {'error': str(e)}}
        )
        raise ValueError(f"Missing required Kinesis record field: {e}")

def store_event(item: Dict[str, Any]) -> None:
    """
    Store an event in DynamoDB.
    
    Args:
        item: Parsed event data to store
        
    Raises:
        ClientError: If DynamoDB operation fails
    """
    try:
        with timing_context('DynamoDBWriteLatency', [{'Name': 'Table', 'Value': DYNAMODB_TABLE}]):
            table.put_item(Item=item)
    except ClientError as e:
        logger.error(
            "Failed to store event in DynamoDB",
            extra={'extra_data': {
                'error': str(e),
                'table': DYNAMODB_TABLE,
                'event_id': item.get('event_id')
            }}
        )
        send_alert(
            f"Failed to store event in DynamoDB: {e}",
            {'table': DYNAMODB_TABLE, 'event_id': item.get('event_id')}
        )
        raise

def process_records(records: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Process a batch of Kinesis records.
    
    Args:
        records: List of Kinesis records to process
        
    Returns:
        Dict containing processing statistics
    """
    stats = {'processed': 0, 'failed': 0}
    processed_events = []
    start_time = time.time()
    
    for record in records:
        try:
            # Parse the record
            item = parse_kinesis_record(record)
            
            # Store in DynamoDB
            store_event(item)
            
            # Add to list for S3 storage
            processed_events.append(item)
            
            stats['processed'] += 1
            logger.info(
                "Processed event",
                extra={'extra_data': {
                    'user_id': item['user_id'],
                    'book_id': item['book_id'],
                    'event_type': item['event_type']
                }}
            )
            
        except (ValueError, ClientError) as e:
            stats['failed'] += 1
            logger.error(
                "Failed to process record",
                extra={'extra_data': {'error': str(e)}}
            )
            # Continue processing other records
            continue
    
    # Store processed events in S3
    if processed_events:
        if not store_in_s3(processed_events):
            logger.error("Failed to store events in S3")
            
    # Calculate and publish events per minute
    duration_minutes = (time.time() - start_time) / 60
    if duration_minutes > 0:
        events_per_minute = stats['processed'] / duration_minutes
        publish_metric(
            'EventsProcessedPerMinute',
            'Count/Minute',
            events_per_minute,
            [{'Name': 'Function', 'Value': 'ProcessAudiobookEvents'}]
        )
    
    # Publish batch metrics
    publish_metric(
        'ProcessedEvents',
        'Count',
        stats['processed'],
        [{'Name': 'Function', 'Value': 'ProcessAudiobookEvents'}]
    )
    publish_metric(
        'FailedEvents',
        'Count',
        stats['failed'],
        [{'Name': 'Function', 'Value': 'ProcessAudiobookEvents'}]
    )
            
    return stats

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function entry point.
    
    Args:
        event: Lambda event containing Kinesis records
        context: Lambda context
        
    Returns:
        Dict containing processing results
    """
    try:
        logger.info(
            "Starting event processing",
            extra={'extra_data': {
                'record_count': len(event['Records']),
                'request_id': context.aws_request_id
            }}
        )
        
        # Process all records
        with timing_context('TotalProcessingTime', [{'Name': 'Function', 'Value': 'ProcessAudiobookEvents'}]):
            stats = process_records(event['Records'])
        
        # Log summary
        logger.info(
            "Event processing complete",
            extra={'extra_data': {
                'processed_count': stats['processed'],
                'failed_count': stats['failed'],
                'request_id': context.aws_request_id
            }}
        )
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Event processing complete',
                'processed_count': stats['processed'],
                'failed_count': stats['failed']
            }
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(
            error_msg,
            extra={'extra_data': {
                'request_id': context.aws_request_id,
                'error': str(e)
            }},
            exc_info=True
        )
        send_alert(
            error_msg,
            {'request_id': context.aws_request_id}
        )
        raise 