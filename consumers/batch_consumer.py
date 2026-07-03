import json
import os
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "citibike-station-status"
CONSUMER_GROUP = "batch-writer-group"
DATA_DIR = "data"
FLUSH_INTERVAL_SECONDS = 30 #write a file every 30s of collected messages

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest" #if consumer group has never read from the topic, starts from beginning so nothing is missed
})

consumer.subscribe([TOPIC])

def get_output_path():
    now = datetime.now(timezone.utc)
    dt_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    folder = os.path.join(DATA_DIR, f"dt={dt_str}",f"hour={hour_str}")
    os.makedirs(folder, exist_ok=True)
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    return os.path.join(folder,f"batch_{timestamp}.json")

def write_batch(messages):
    if not messages:
        return 

    path = get_output_path()
    with open(path, "w") as f:
        for msg in messages:
            f.write(json.dumps(msg)+ "\n")
    print(f"wrote {len(messages)} messages to {path}")

def main():
    print(f"starting batch consumer --> writing to '{DATA_DIR}/'")

    buffer = []
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0) #checks for message waiting up to 1 sec

            if msg is None:
                pass
            elif msg.error():
                print(f"consumer error: {msg.error()}")
            else:
                value  = json.loads(msg.value().decode("utf-8"))
                buffer.append(value) 
            '''
                buffer logic, waits to collect 30 secs worth of messages before writing
                trade little latency for much more efficient files downstream
            '''
            if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS: 
                write_batch(buffer)
                buffer = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print("Shutting down, flishing remaining messages...")
        write_batch(buffer)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

