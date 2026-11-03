import json
import random
import time
import sys
import six
from datetime import datetime, timezone

sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaProducer

TOPIC = "campus-sensors"
BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

try:
    print("Starting Campus Sensor Producer...")
    print(f"Sending events to topic: {TOPIC}\n")

    for event_id in range(1, 101):
        event = {
    "event_id": event_id,
    "sensor_id": f"sensor-{random.randint(1, 4):02d}",
    "campus_building": f"building-{random.choice(['A', 'B', 'C'])}",
    "temperature_c": round(random.uniform(18, 42), 2),
    "humidity_pct": round(random.uniform(25, 85), 2),
    "timestamp": datetime.now(timezone.utc).isoformat()
}

        producer.send(TOPIC, value=event)
        print("Sent:", event)

        time.sleep(1)

    producer.flush()
    print("\nAll 100 events have been sent successfully.")

except Exception as error:
    print("Producer error:", error)

finally:
    producer.close()
    print("Producer closed.")
