
import json
import sys
import six

sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaConsumer

TOPIC = "campus-sensors"
BOOTSTRAP_SERVERS = "localhost:9092"
TARGET_BUILDING = "building-A"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-building-filter",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

total_temperature = 0
event_count = 0

print(f"Listening to topic: {TOPIC}")
print(f"Filtering events from: {TARGET_BUILDING}")
print("Calculating running average temperature...\n")

try:
    for message in consumer:
        event = message.value

        # Get campus building
        building = event.get("campus_building", "unknown")

        # Filter events: building-A only
        if building != TARGET_BUILDING:
            continue

        temperature = event["temperature_c"]
        humidity = event["humidity_pct"]

        event_count += 1
        total_temperature += temperature

        average_temperature = total_temperature / event_count

        print(
            f'{event["sensor_id"]} | '
            f'Building={building} | '
            f'T={temperature}°C | '
            f'RH={humidity}% | '
            f'Running Average={average_temperature:.2f}°C'
        )

        if temperature >= 35:
            print("ALERT: High temperature detected!")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

except Exception as error:
    print("Consumer error:", error)

finally:
    consumer.close()
    print("Consumer closed.")
