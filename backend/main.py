import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

r = redis.Redis(host="localhost",port=6379,decode_responses=True)

with open("data/station_info.json") as f:
    STATION_INFO = json.load(f)

@app.get("/stations")
def get_stations():
    results=[]
    for key in r.scan_iter("station:*"):
        status = json.loads(r.get(key))
        station_id = status["station_id"]
        info = STATION_INFO.get(station_id)

        if not info:
            continue

        results.append({
            "station_id": station_id,
            "name": info["name"],
            "lat": info["lat"],
            "lon": info["lon"],
            "bikes_available": status.get("num_bikes_available",0), # getting live feed data from redis
            "ebikes_available": status.get("num_ebikes_available",0),
            "docks_available": status.get("num_docks_available",0),
        })
    return results