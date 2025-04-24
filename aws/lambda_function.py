import json
import base64
import boto3
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('PlaybackEvents')

def lambda_handler(event, context):
    """
    Lambda function to process Kinesis events and store them in DynamoDB.
    """
    logger.info(f"Processing {len(event['Records'])} records from Kinesis")
    
    processed_count = 0
    failed_count = 0
    
    for record in event['Records']:
        try:
            # Decode and parse the Kinesis record
            payload = base64.b64decode(record['kinesis']['data'])
            playback_event = json.loads(payload)
            
            # Add processing timestamp
            playback_event['processed_at'] = datetime.utcnow().isoformat()
            
            # Store in DynamoDB
            table.put_item(Item=playback_event)
            
            processed_count += 1
            logger.info(f"Successfully processed event: {playback_event['event_id']}")
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to process record: {str(e)}")
            continue
    
    logger.info(f"Processing complete. Successful: {processed_count}, Failed: {failed_count}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_count': processed_count,
            'failed_count': failed_count
        })
    } 