#!/usr/bin/env python3
"""
Script to generate simulated audiobook playback events.
Events are written to a JSON file and optionally streamed to a local server.
"""

import json
import random
import datetime
import time
from pathlib import Path
from typing import Dict, List, Optional
import uuid
from dataclasses import dataclass, asdict
import http.server
import socketserver
import threading
from queue import Queue
import logging
from datetime import timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AudioBook:
    """Represents an audiobook with its metadata."""
    book_id: str
    title: str
    author: str
    duration: int  # in seconds
    chapters: int
    genre: str

    def to_dict(self) -> Dict:
        """Convert the audiobook to a dictionary."""
        return asdict(self)

@dataclass
class User:
    """Represents a user with their preferences."""
    user_id: str
    preferences: Dict

    def to_dict(self) -> Dict:
        """Convert the user to a dictionary."""
        return asdict(self)

@dataclass
class PlaybackEvent:
    """Represents a playback event."""
    event_id: str
    user_id: str
    book_id: str
    event_type: str
    timestamp: datetime
    position: int  # in seconds
    chapter: int
    metadata: Dict

    def __post_init__(self):
        """Validate event type after initialization."""
        valid_event_types = ["START_PLAYBACK", "PAUSE", "RESUME", "SEEK", "END_PLAYBACK"]
        if self.event_type not in valid_event_types:
            raise ValueError(f"Invalid event type: {self.event_type}. Must be one of {valid_event_types}")

    def to_dict(self) -> Dict:
        """Convert the event to a dictionary."""
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "book_id": self.book_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "position": self.position,
            "chapter": self.chapter,
            "metadata": self.metadata
        }

class EventGenerator:
    """Generates simulated audiobook playback events."""
    VALID_EVENT_TYPES = ["START_PLAYBACK", "PAUSE", "RESUME", "SEEK", "END_PLAYBACK"]
    
    def __init__(self, num_users: int, num_books: int, start_date: datetime, end_date: datetime):
        """Initialize the event generator."""
        self.num_users = num_users
        self.num_books = num_books
        self.start_date = start_date
        self.end_date = end_date
        self.users = self._create_users()
        self.books = self._create_books()

    def _create_users(self) -> List[User]:
        """Create a list of simulated users."""
        users = []
        for _ in range(self.num_users):
            user = User(
                user_id=f"user_{uuid.uuid4().hex[:8]}",
                preferences={
                    "preferred_speed": random.choice([0.5, 0.75, 1.0, 1.25, 1.5, 2.0]),
                    "preferred_genres": random.sample(["Fiction", "Mystery", "Science", "History", "Biography"], k=2)
                }
            )
            users.append(user)
        return users

    def _create_books(self) -> List[AudioBook]:
        """Create a list of simulated audiobooks."""
        books = []
        genres = ["Fiction", "Mystery", "Science", "History", "Biography"]
        for _ in range(self.num_books):
            book = AudioBook(
                book_id=f"book_{uuid.uuid4().hex[:8]}",
                title=f"Book {uuid.uuid4().hex[:8]}",
                author=f"Author {uuid.uuid4().hex[:8]}",
                duration=random.randint(3600, 36000),  # 1-10 hours
                chapters=random.randint(5, 30),
                genre=random.choice(genres)
            )
            books.append(book)
        return books

    def generate_event(self, user: Optional[User] = None, book: Optional[AudioBook] = None) -> PlaybackEvent:
        """Generate a single playback event."""
        if user is None:
            user = random.choice(self.users)
        if book is None:
            book = random.choice(self.books)

        event_type = random.choice(self.VALID_EVENT_TYPES)
        timestamp = self.start_date + timedelta(
            seconds=random.randint(0, int((self.end_date - self.start_date).total_seconds()))
        )
        
        return PlaybackEvent(
            event_id=f"event_{uuid.uuid4().hex}",
            user_id=user.user_id,
            book_id=book.book_id,
            event_type=event_type,
            timestamp=timestamp,
            position=random.randint(0, book.duration),
            chapter=random.randint(1, book.chapters),
            metadata={
                "device_type": random.choice(["mobile", "tablet", "desktop"]),
                "app_version": "1.0.0",
                "network_type": random.choice(["wifi", "cellular", "ethernet"])
            }
        )

    def generate_events(self, num_events: int) -> List[PlaybackEvent]:
        """Generate multiple playback events."""
        events = []
        while len(events) < num_events:
            user = random.choice(self.users)
            book = random.choice(self.books)
            session_events = self.generate_user_session(user, book)
            # Only add events if we have space for the entire session
            if len(events) + len(session_events) <= num_events:
                events.extend(session_events)
            else:
                # If we can't fit the entire session, try with a different user/book
                continue
        return events[:num_events]

    def generate_user_session(self, user: User, book: AudioBook) -> List[PlaybackEvent]:
        """Generate a sequence of events representing a user's listening session."""
        events = []
        current_position = 0
        current_chapter = 1
        current_time = self.start_date
        
        # Start playback
        events.append(PlaybackEvent(
            event_id=f"event_{uuid.uuid4().hex}",
            user_id=user.user_id,
            book_id=book.book_id,
            event_type="START_PLAYBACK",
            timestamp=current_time,
            position=current_position,
            chapter=current_chapter,
            metadata={
                "device_type": random.choice(["mobile", "tablet", "desktop"]),
                "app_version": "1.0.0",
                "network_type": random.choice(["wifi", "cellular", "ethernet"])
            }
        ))
        
        # Generate random events during session
        session_duration = random.randint(300, 3600)  # 5-60 minutes
        last_event_type = "START_PLAYBACK"
        num_events = random.randint(2, 5)  # Generate at least 2 events (plus START_PLAYBACK)
        
        for _ in range(num_events - 1):  # -1 because we already have START_PLAYBACK
            # Determine valid next event types based on last event
            if last_event_type == "START_PLAYBACK":
                next_event_type = random.choice(["PAUSE", "SEEK"])
            elif last_event_type == "PAUSE":
                next_event_type = "RESUME"
            elif last_event_type == "RESUME":
                next_event_type = random.choice(["PAUSE", "SEEK"])
            elif last_event_type == "SEEK":
                next_event_type = "PAUSE"
            else:  # END_PLAYBACK
                break
            
            current_time += timedelta(seconds=random.randint(30, 300))
            if next_event_type != "SEEK":
                current_position += random.randint(30, 300)
            else:
                current_position = random.randint(0, book.duration)
            
            if current_position > book.duration:
                next_event_type = "END_PLAYBACK"
                current_position = book.duration
                
            current_chapter = min(book.chapters, 1 + current_position // (book.duration // book.chapters))
            
            events.append(PlaybackEvent(
                event_id=f"event_{uuid.uuid4().hex}",
                user_id=user.user_id,
                book_id=book.book_id,
                event_type=next_event_type,
                timestamp=current_time,
                position=current_position,
                chapter=current_chapter,
                metadata={
                    "device_type": random.choice(["mobile", "tablet", "desktop"]),
                    "app_version": "1.0.0",
                    "network_type": random.choice(["wifi", "cellular", "ethernet"])
                }
            ))
            
            last_event_type = next_event_type
        
        # Always end with END_PLAYBACK
        current_time += timedelta(seconds=random.randint(30, 300))
        events.append(PlaybackEvent(
            event_id=f"event_{uuid.uuid4().hex}",
            user_id=user.user_id,
            book_id=book.book_id,
            event_type="END_PLAYBACK",
            timestamp=current_time,
            position=current_position,
            chapter=current_chapter,
            metadata={
                "device_type": random.choice(["mobile", "tablet", "desktop"]),
                "app_version": "1.0.0",
                "network_type": random.choice(["wifi", "cellular", "ethernet"])
            }
        ))
        
        return events

    def save_events(self, events: List[PlaybackEvent], output_file: str):
        """Save events to a JSON file."""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump([event.to_dict() for event in events], f, indent=2)
        
        logger.info(f"Saved {len(events)} events to {output_file}")

class EventStreamServer:
    """Simple HTTP server to stream events."""
    
    def __init__(self, port: int = 8000):
        self.port = port
        
    def start(self, event_queue: Queue):
        """Start the server in a separate thread."""
        class Handler(http.server.SimpleHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                while True:
                    event = event_queue.get()
                    self.wfile.write(json.dumps(event.to_dict()).encode() + b'\n')
                    self.wfile.flush()
        
        with socketserver.TCPServer(("", self.port), Handler) as httpd:
            logger.info(f"Serving at port {self.port}")
            httpd.serve_forever()

def main():
    # Initialize event generator
    generator = EventGenerator(num_users=50, num_books=100, start_date=datetime.datetime.now(), end_date=datetime.datetime.now() + datetime.timedelta(days=1))
    
    # Generate and save events
    events = generator.generate_events(num_events=1000)
    generator.save_events(events, "events/data/playback_events.json")
    
    # Optionally start streaming server
    stream_events = False  # Set to True to enable streaming
    if stream_events:
        server = EventStreamServer()
        server_thread = threading.Thread(target=server.start, args=(generator.event_queue,))
        server_thread.daemon = True
        server_thread.start()
        
        # Stream events
        for event in events:
            generator.event_queue.put(event)
            time.sleep(0.1)  # Delay between events

if __name__ == "__main__":
    main() 