import json
import time
import random
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from faker import Faker

fake = Faker()

TOPIC = "raw_telemetry"
KAFKA_BOOTSTRAP = "localhost:9092"   # Producer runs on your Mac, so localhost works.

def generate_record():
    # Trip & car identifiers
    trip_id = str(uuid.uuid4())
    car_id = f"CAR-{random.randint(100, 999)}"

    # Coordinates
    latitude = float(fake.latitude())
    longitude = float(fake.longitude())

    # Event timestamp (this matches `event_timestamp` in schema)
    event_timestamp = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")

    # Telemetry values
    speed_kmph = round(random.uniform(0, 160), 2)
    fuel_level = round(random.uniform(0, 100), 2)
    engine_temp_c = round(random.uniform(60, 130), 2)

    # Trip start info
    trip_start_time = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    trip_start_latitude = latitude + random.uniform(-0.01, 0.01)
    trip_start_longitude = longitude + random.uniform(-0.01, 0.01)
    trip_start_date = datetime.utcnow().date().isoformat()

    message_key = f"{car_id}-{trip_id}"

    payload = {
        "trip_id": trip_id,
        "car_id": car_id,
        "latitude": latitude,
        "longitude": longitude,
        "event_timestamp": event_timestamp,
        "speed_kmph": speed_kmph,
        "fuel_level": fuel_level,
        "engine_temp_c": engine_temp_c,
        "trip_start_time": trip_start_time,
        "trip_start_latitude": trip_start_latitude,
        "trip_start_longitude": trip_start_longitude,
        "trip_start_date": trip_start_date,
        "message_key": message_key
    }
    return message_key, payload

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5
    )

    print(f"Producing to topic: {TOPIC} on {KAFKA_BOOTSTRAP}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            key, payload = generate_record()

            # Occasionally send bad data on purpose (to test corrupt_records later)
            if random.random() < 0.05:
                producer.send(TOPIC, key=key, value="THIS_IS_NOT_JSON")
                print(f"[CORRUPT SENT] key={key}")
            else:
                producer.send(TOPIC, key=key, value=payload)
                print(f"[SENT] key={key} speed={payload['speed_kmph']} fuel={payload['fuel_level']} temp={payload['engine_temp_c']}")

            producer.flush()
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()