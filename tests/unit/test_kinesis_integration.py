"""
Unit tests for Kinesis integration functionality.
"""

import json
import pytest
from datetime import datetime
from aws.send_to_kinesis import KinesisEventPublisher
from events.simulate_playback_events import PlaybackEvent

@pytest.fixture
def kinesis_publisher(kinesis_client):
    """Create a KinesisEventPublisher instance for testing."""
    return KinesisEventPublisher(
        stream_name="TestStream",
        region="us-east-1",
        client=kinesis_client
    )

@pytest.fixture
def sample_event():
    """Create a sample PlaybackEvent for testing."""
    return PlaybackEvent(
        event_id="test123",
        user_id="user123",
        book_id="book123",
        event_type="START_PLAYBACK",
        timestamp=datetime.now(),
        position=0,
        chapter=1,
        metadata={
            "device_type": "mobile",
            "app_version": "1.0.0",
            "network_type": "wifi"
        }
    )

def test_kinesis_publisher_initialization(kinesis_publisher):
    """Test KinesisEventPublisher initialization."""
    assert kinesis_publisher.stream_name == "TestStream"
    assert kinesis_publisher.region == "us-east-1"
    assert kinesis_publisher.client is not None

def test_create_stream(kinesis_client):
    """Test stream creation."""
    # Arrange
    stream_name = "TestStream"
    
    # Act
    kinesis_client.create_stream(
        StreamName=stream_name,
        ShardCount=1
    )
    
    # Assert
    response = kinesis_client.describe_stream(StreamName=stream_name)
    assert response["StreamDescription"]["StreamName"] == stream_name
    assert response["StreamDescription"]["StreamStatus"] in ["CREATING", "ACTIVE"]

def test_send_single_event(kinesis_publisher, sample_event):
    """Test sending a single event to Kinesis."""
    # Act
    response = kinesis_publisher.send_event(sample_event)
    
    # Assert
    assert "ShardId" in response
    assert "SequenceNumber" in response

def test_send_batch_events(kinesis_publisher, sample_event):
    """Test sending multiple events in batch to Kinesis."""
    # Arrange
    events = [sample_event for _ in range(5)]
    
    # Act
    responses = kinesis_publisher.send_events_batch(events)
    
    # Assert
    assert len(responses) == 5
    for response in responses:
        assert "ShardId" in response
        assert "SequenceNumber" in response

def test_event_serialization(sample_event):
    """Test event serialization for Kinesis."""
    # Arrange
    expected_keys = {
        "event_id", "user_id", "book_id", "event_type",
        "timestamp", "position", "chapter", "metadata"
    }
    
    # Act
    event_dict = sample_event.to_dict()
    event_json = json.dumps(event_dict)
    
    # Assert
    assert all(key in event_dict for key in expected_keys)
    assert isinstance(event_json, str)
    
    # Verify JSON can be deserialized
    deserialized = json.loads(event_json)
    assert deserialized["event_id"] == sample_event.event_id
    assert deserialized["event_type"] == sample_event.event_type

def test_stream_not_found_error(kinesis_client):
    """Test handling of non-existent stream."""
    # Arrange
    publisher = KinesisEventPublisher(
        stream_name="NonExistentStream",
        region="us-east-1",
        client=kinesis_client
    )
    
    # Act & Assert
    with pytest.raises(Exception):
        publisher.send_event(sample_event)

def test_invalid_partition_key(kinesis_publisher, sample_event):
    """Test handling of invalid partition key."""
    # Arrange
    sample_event.user_id = ""  # Invalid partition key
    
    # Act & Assert
    with pytest.raises(ValueError):
        kinesis_publisher.send_event(sample_event)

def test_batch_size_limit(kinesis_publisher, sample_event):
    """Test handling of batch size limits."""
    # Arrange
    events = [sample_event for _ in range(1000)]  # Exceeds maximum batch size
    
    # Act & Assert
    with pytest.raises(ValueError):
        kinesis_publisher.send_events_batch(events)

def test_retry_on_throttling(kinesis_publisher, sample_event, mocker):
    """Test retry behavior on throttling."""
    # Arrange
    mock_put = mocker.patch.object(
        kinesis_publisher.client,
        'put_record',
        side_effect=[
            {'Error': {'Code': 'ProvisionedThroughputExceededException'}},
            {'ShardId': 'shard-1', 'SequenceNumber': '123'}
        ]
    )
    
    # Act
    response = kinesis_publisher.send_event(sample_event)
    
    # Assert
    assert mock_put.call_count == 2
    assert "ShardId" in response
    assert "SequenceNumber" in response 