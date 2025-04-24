#!/bin/bash

# Load environment variables
source .env

# Configuration
STREAM_NAME="${KINESIS_STREAM_NAME:-AudiobookPlaybackStream}"
REGION="${AWS_REGION:-us-west-2}"  # Change this to your desired region
SHARD_COUNT="${SHARD_COUNT:-1}"

# Create the Kinesis stream
echo "Creating Kinesis stream: $STREAM_NAME"
aws kinesis create-stream \
    --stream-name $STREAM_NAME \
    --shard-count $SHARD_COUNT \
    --region $REGION

# Wait for the stream to become active
echo "Waiting for stream to become active..."
aws kinesis wait stream-exists \
    --stream-name $STREAM_NAME \
    --region $REGION

# Describe the stream
echo "Stream details:"
aws kinesis describe-stream \
    --stream-name $STREAM_NAME \
    --region $REGION

echo "Stream creation complete!"

# Optional commands (commented out):
# Delete stream:
# aws kinesis delete-stream --stream-name $STREAM_NAME --region $REGION

# List streams:
# aws kinesis list-streams --region $REGION

# Get shard iterator:
# aws kinesis get-shard-iterator \
#     --stream-name $STREAM_NAME \
#     --shard-id shardId-000000000000 \
#     --shard-iterator-type TRIM_HORIZON \
#     --region $REGION 