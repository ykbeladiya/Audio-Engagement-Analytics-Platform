"""
Unit tests for event simulation functionality.
"""

import pytest
from datetime import datetime, timedelta
from events.simulate_playback_events import (
    AudioBook, User, PlaybackEvent, EventGenerator
)

@pytest.fixture
def event_generator():
    """Create an event generator for testing."""
    start_date = datetime.now()
    end_date = start_date + timedelta(days=1)
    return EventGenerator(num_users=5, num_books=5, start_date=start_date, end_date=end_date)

def test_audiobook_creation():
    """Test creating an audiobook."""
    book = AudioBook(
        book_id="test_book",
        title="Test Book",
        author="Test Author",
        duration=3600,
        chapters=10,
        genre="Fiction"
    )
    assert book.book_id == "test_book"
    assert book.title == "Test Book"
    assert book.duration == 3600
    assert book.chapters == 10
    assert book.genre == "Fiction"

def test_user_creation():
    """Test creating a user."""
    preferences = {
        "preferred_speed": 1.0,
        "preferred_genres": ["Fiction", "Mystery"]
    }
    user = User(user_id="test_user", preferences=preferences)
    assert user.user_id == "test_user"
    assert user.preferences == preferences

def test_event_generator_initialization(event_generator):
    """Test event generator initialization."""
    assert len(event_generator.users) == 5
    assert len(event_generator.books) == 5

def test_generate_single_event(event_generator):
    """Test generating a single event."""
    event = event_generator.generate_event()
    assert isinstance(event, PlaybackEvent)
    assert event.event_type in event_generator.VALID_EVENT_TYPES
    assert event.timestamp >= event_generator.start_date
    assert event.timestamp <= event_generator.end_date

def test_generate_multiple_events(event_generator):
    """Test generating multiple events."""
    events = event_generator.generate_events(10)
    assert len(events) == 10
    assert all(isinstance(e, PlaybackEvent) for e in events)

def test_user_session_generation(event_generator):
    """Test generating a user session."""
    user = event_generator.users[0]
    book = event_generator.books[0]
    events = event_generator.generate_user_session(user, book)
    
    assert len(events) >= 3  # At least start, one event, and end
    assert events[0].event_type == "START_PLAYBACK"
    assert events[-1].event_type == "END_PLAYBACK"
    
    # Check chronological order
    for i in range(1, len(events)):
        assert events[i].timestamp > events[i-1].timestamp

def test_invalid_event_type():
    """Test handling of invalid event types."""
    # Arrange
    timestamp = datetime.now()
    
    # Act & Assert
    with pytest.raises(ValueError):
        PlaybackEvent(
            event_id="test_event",
            user_id="test_user",
            book_id="test_book",
            event_type="INVALID_TYPE",  # Invalid event type
            timestamp=timestamp,
            position=0,
            chapter=1,
            metadata={
                "device_type": "mobile",
                "app_version": "1.0.0",
                "network_type": "wifi"
            }
        )

def test_event_to_dict(event_generator):
    """Test converting an event to dictionary."""
    event = event_generator.generate_event()
    event_dict = event.to_dict()
    
    assert isinstance(event_dict, dict)
    assert "event_id" in event_dict
    assert "user_id" in event_dict
    assert "book_id" in event_dict
    assert "event_type" in event_dict
    assert "timestamp" in event_dict
    assert "position" in event_dict
    assert "chapter" in event_dict
    assert "metadata" in event_dict

def test_playback_event_creation():
    """Test PlaybackEvent instance creation and validation."""
    # Arrange
    timestamp = datetime.now()
    
    # Act
    event = PlaybackEvent(
        event_id="evt123",
        user_id="user123",
        book_id="book123",
        event_type="START_PLAYBACK",
        timestamp=timestamp,
        position=0,
        chapter=1,
        metadata={
            "device_type": "mobile",
            "app_version": "1.0.0",
            "network_type": "wifi"
        }
    )

    # Assert
    assert event.event_id == "evt123"
    assert event.event_type == "START_PLAYBACK"
    assert event.timestamp == timestamp
    assert event.metadata["device_type"] == "mobile"

def test_event_generation(event_generator):
    """Test event generation functionality."""
    # Act
    events = list(event_generator.generate_events(100))

    # Assert
    assert len(events) == 100
    for event in events:
        assert isinstance(event, PlaybackEvent)
        assert event.user_id in [user.user_id for user in event_generator.users]
        assert event.book_id in [book.book_id for book in event_generator.books]
        assert event.timestamp >= event_generator.start_date
        assert event.timestamp <= event_generator.end_date

def test_event_sequence_validity(event_generator):
    """Test that generated events follow logical sequence."""
    # Arrange
    events = list(event_generator.generate_events(50))
    user_sessions = {}

    # Act & Assert
    for event in events:
        if event.user_id not in user_sessions:
            user_sessions[event.user_id] = []
        user_sessions[event.user_id].append(event)

    for user_id, session_events in user_sessions.items():
        sorted_events = sorted(session_events, key=lambda x: x.timestamp)
        for i in range(len(sorted_events) - 1):
            current_event = sorted_events[i]
            next_event = sorted_events[i + 1]

            # Validate event sequence logic
            if current_event.event_type == "START_PLAYBACK":
                assert next_event.event_type in ["PAUSE", "SEEK", "END_PLAYBACK"]
            elif current_event.event_type == "PAUSE":
                assert next_event.event_type in ["RESUME", "END_PLAYBACK"]
            elif current_event.event_type == "RESUME":
                assert next_event.event_type in ["PAUSE", "SEEK", "END_PLAYBACK"]
            elif current_event.event_type == "SEEK":
                assert next_event.event_type in ["PAUSE", "END_PLAYBACK"]

def test_metadata_consistency(event_generator):
    """Test consistency of event metadata."""
    # Act
    events = list(event_generator.generate_events(50))

    # Assert
    for event in events:
        assert "device_type" in event.metadata
        assert "app_version" in event.metadata
        assert "network_type" in event.metadata
        assert isinstance(event.metadata["app_version"], str)

def test_invalid_event_type():
    """Test handling of invalid event types."""
    # Arrange
    timestamp = datetime.now()
    
    # Act & Assert
    with pytest.raises(ValueError):
        PlaybackEvent(
            event_id="evt123",
            user_id="user123",
            book_id="book123",
            event_type="INVALID_EVENT",
            timestamp=timestamp,
            position=0,
            chapter=1,
            metadata={}
        ) 