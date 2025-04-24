# Audio Engagement Analytics Platform

A real-time analytics platform for tracking and analyzing audiobook listener engagement patterns. The platform processes playback events from audiobook applications, providing insights into listening behavior, popular content, and user engagement metrics.

## Key Features
- Real-time event processing using AWS Kinesis
- Serverless architecture with AWS Lambda
- Structured data storage in DynamoDB
- Raw event archival in S3
- Advanced analytics using AWS Athena
- Interactive dashboards for visualization
- Comprehensive monitoring and alerting

## Architecture Overview

![Serverless Audio Engagement Analytics Platform](docs/architecture.png)

For terminal compatibility, here's the ASCII version:

```ascii
┌──────────────┐    ┌──────────┐    ┌─────────────┐    ┌──────────────┐
│  Audiobook   │───>│ Kinesis  │───>│   Lambda    │───>│   DynamoDB   │
│ Application  │    │  Stream  │    │  Function   │    │    Table     │
└──────────────┘    └──────────┘    └─────────────┘    └──────────────┘
                                         │
                                         │
                                         ▼
                          ┌─────────────────────────────┐
                          │         S3 Bucket           │
                          │  (Raw Events & Analytics)   │
                          └─────────────────────────────┘
                                         │
                                         │
            ┌────────────────────────────┴────────────────────────┐
            │                                                      │
            ▼                                                      ▼
┌────────────────────┐                              ┌───────────────────┐
│  Athena Queries    │                              │    CloudWatch     │
│   & Analytics      │                              │     Dashboard     │
└────────────────────┘                              └───────────────────┘
```

## Setup Instructions

### Prerequisites
- Python 3.8+
- AWS CLI configured with appropriate credentials
- AWS account with permissions for:
  - Kinesis
  - Lambda
  - DynamoDB
  - S3
  - CloudWatch
  - Athena
- Node.js and npm (for dashboard development)

### Environment Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/audio-engagement-analytics.git
cd audio-engagement-analytics
```

2. Create and activate a Conda environment:
```bash
conda env create -f environment.yml
conda activate audio_analytics
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### AWS Infrastructure Setup

1. Configure AWS credentials:
```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Default region: us-east-1
# Default output format: json
```

2. Create the Kinesis stream:
```bash
cd aws
./create_stream.sh
```

3. Set up DynamoDB table:
```bash
python aws/setup_dynamodb.py
```

4. Create CloudWatch dashboard:
```bash
python aws/setup_dashboard.py
```

5. Create S3 bucket for raw events:
```bash
aws s3 mb s3://audiobook-events-{YOUR_UNIQUE_SUFFIX}
```

## Event Schema
```json
{
  "event_id": "string",
  "user_id": "string",
  "book_id": "string",
  "event_type": "START_PLAYBACK|PAUSE|RESUME|END_PLAYBACK|CHAPTER_CHANGE|BOOKMARK|SPEED_CHANGE",
  "timestamp": "ISO8601 timestamp",
  "position": "number",
  "chapter": "number",
  "metadata": {
    "device_type": "string",
    "app_version": "string",
    "network_type": "string"
  }
}
```

## Simulating Events

To generate and send sample audiobook playback events:

1. Generate events:
```bash
python events/simulate_playback_events.py
```

2. Send events to Kinesis:
```bash
python aws/send_to_kinesis.py
```

The simulation will generate 1000+ realistic audiobook playback events with various event types:
- START_PLAYBACK
- PAUSE_PLAYBACK
- RESUME_PLAYBACK
- END_PLAYBACK
- CHAPTER_CHANGE
- BOOKMARK
- SPEED_CHANGE

## Lambda Function Deployment

1. Package the Lambda function:
```bash
cd lambda
zip -r function.zip .
```

2. Deploy using AWS CLI:
```bash
aws lambda create-function \
  --function-name ProcessAudiobookEvents \
  --runtime python3.8 \
  --handler process_events.lambda_handler \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/AudiobookEventsProcessor \
  --zip-file fileb://function.zip
```

3. Configure the Kinesis trigger:
```bash
aws lambda create-event-source-mapping \
  --function-name ProcessAudiobookEvents \
  --event-source-arn arn:aws:kinesis:REGION:ACCOUNT_ID:stream/AudiobookPlaybackStream \
  --batch-size 100 \
  --starting-position LATEST
```

## Sample Athena Queries

### Popular Books by Total Listening Time
```sql
SELECT 
    book_id,
    SUM(duration_seconds) as total_listening_time,
    COUNT(DISTINCT user_id) as unique_listeners
FROM audiobook_events
WHERE event_type IN ('START_PLAYBACK', 'END_PLAYBACK')
GROUP BY book_id
ORDER BY total_listening_time DESC
LIMIT 10;
```

### User Engagement Patterns
```sql
SELECT 
    HOUR(timestamp) as hour_of_day,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM audiobook_events
GROUP BY HOUR(timestamp)
ORDER BY hour_of_day;
```

### Average Listening Session Duration
```sql
SELECT 
    user_id,
    AVG(session_duration) as avg_session_minutes
FROM (
    SELECT 
        user_id,
        TIMESTAMP_DIFF(
            MAX(timestamp),
            MIN(timestamp),
            MINUTE
        ) as session_duration
    FROM audiobook_events
    GROUP BY user_id, session_id
)
GROUP BY user_id
ORDER BY avg_session_minutes DESC
LIMIT 20;
```

## Dashboard & Visualization

The platform includes an interactive dashboard built with:
- React for the frontend
- Chart.js for visualizations
- AWS SDK for data fetching

Key metrics displayed:
1. Real-time listener count
2. Popular books and chapters
3. User engagement by time of day
4. Average session duration
5. Device and platform distribution

To set up the dashboard:
```bash
cd dashboard
npm install
npm start
```

## Monitoring and Alerts

The platform includes CloudWatch alarms for:
- High event processing latency (>1 second)
- Elevated error rates (>1%)
- Stream throttling
- Lambda function errors

## Troubleshooting

Common issues and solutions:

1. Kinesis Stream Throttling
```bash
aws kinesis update-shard-count --stream-name AudiobookPlaybackStream --target-shard-count 2
```

2. Lambda Cold Starts
- Increase memory allocation
- Enable Provisioned Concurrency

3. DynamoDB Capacity
```bash
aws dynamodb update-table --table-name PlaybackEvents --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
