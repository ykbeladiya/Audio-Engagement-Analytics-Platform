#!/bin/bash

# Create Kinesis stream
aws kinesis create-stream \
    --stream-name AudiobookPlaybackStream \
    --shard-count 1 \
    --region us-east-1

# Wait for stream to become active
echo "Waiting for stream to become active..."
aws kinesis wait stream-exists \
    --stream-name AudiobookPlaybackStream \
    --region us-east-1

# Verify stream creation
aws kinesis describe-stream \
    --stream-name AudiobookPlaybackStream \
    --region us-east-1 \
    --output json 