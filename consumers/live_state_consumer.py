import json
from confluent_kafka import Consumer
import redis

KAFKA_BOOSTRAP_SERVER = "localhost:9092"
TOPIC = "citibike-station-status"
CONSUMER_GROUP = "live-state-writer-group"

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOSTRAP_SERVER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset":"earliest"
})

consumer.subscribe ([TOPIC])

def main():
    print("Starting live-state consumer -> writing current state to Redis")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            station = json.loads(msg.value().decode("utf-8"))
            station_id = station["station_id"]

            redis_key = f"station:{station_id}"
            r.set(redis_key,json.dumps(station))

    except KeyboardInterrupt:
        print("Shutting down live-state consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()