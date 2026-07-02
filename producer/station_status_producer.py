import json
import time
import requests
from confluent_kafka import Producer

STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = 'citibike-station-status'
POLL_INTERVAL_SECONDS = 15

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")

def fetch_station_status():
    response = requests.get(STATION_STATUS_URL, timeout=10)
    response.raise_for_status()
    return response.json()["data"]["stations"]

def produce_station_statuses(stations):
    for station in stations:
        key = station["station_id"]
        value = json.dumps(station)
        producer.produce(
            topic=TOPIC,
            key=key,
            value=value,
            callback=delivery_report
        )
    producer.flush()

def main():
    print(f"Statrting CITI Bike producer -> topic '{TOPIC}'")
    while True:
        try:
            stations = fetch_station_status()
            produce_station_statuses(stations)
            print(f"Produced {len(stations)} station updates")
        except Exception as e:
            print(f"Error Fetching/Producing: {e}")
        
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()