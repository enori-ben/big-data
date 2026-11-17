
import json
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC = "campus-events"

DEPARTMENTS = [
    "Computer Science",
    "Mathematics",
    "Physics"
]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

base = datetime.now(timezone.utc).replace(microsecond=0)

events = [
    ("S101", "check-in", "Computer Science", 2),
    ("S204", "check-in", "Mathematics", 8),
    ("S101", "open-course", "Computer Science", 15),
    ("S305", "check-in", "Physics", 22),
    ("S411", "check-in", "Computer Science", 35),
    ("S204", "check-in", "Mathematics", 48),
    ("S512", "check-in", "Physics", 12),
]

try:
    for event_id, (student, event_type, dept, seconds) in enumerate(events, 1):

        event_time = base + timedelta(seconds=seconds)

        event = {
            "eventId": event_id,
            "studentId": student,
            "eventType": event_type,
            "department": dept,
            "eventTime": event_time.isoformat()
        }

        producer.send(TOPIC, value=event)

        print("Sent:", event)

        time.sleep(1)

    producer.flush()

finally:
    producer.close()
