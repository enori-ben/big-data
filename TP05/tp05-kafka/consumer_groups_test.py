
import json
import sys
import six

sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaConsumer

TOPIC = "campus-sensors"
BOOTSTRAP_SERVERS = "localhost:9092"

if len(sys.argv) < 2:
    print("Usage: python consumer_groups_test.py GROUP_ID")
    sys.exit(1)

group_id = sys.argv[1]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=group_id,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print(f"Consumer started | Group: {group_id}")
print(f"Listening to topic: {TOPIC}\n")

try:
    for message in consumer:
        event = message.value

        print(
            f"Group={group_id} | "
            f"Partition={message.partition} | "
            f"Offset={message.offset} | "
            f"Sensor={event['sensor_id']} | "
            f"Temperature={event['temperature_c']}°C"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
