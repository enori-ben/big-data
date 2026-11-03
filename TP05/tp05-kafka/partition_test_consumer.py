import json
import sys
import six

sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaConsumer

TOPIC = "campus-sensors-test"
GROUP_ID = "single-partition-group"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print(f"Consumer started | Group: {GROUP_ID}")
print("Listening to campus-sensors-test...\n")

try:
    for message in consumer:
        print(
            f"Consumer received | "
            f"Partition={message.partition} | "
            f"Offset={message.offset} | "
            f"Sensor={message.value['sensor_id']} | "
            f"Temperature={message.value['temperature_c']}°C"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()