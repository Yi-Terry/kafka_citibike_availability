import json
import io
import os
import boto3
import pandas as pd
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
REDIS_HOST  = os.environ.get("REDIS_HOST", "localhost")
r = redis.Redis(host=REDIS_HOST,port=6379,decode_responses=True)
S3_BUCKET = os.environ["S3_BUCKET"]
s3 = boto3.client("s3")

with open("data/station_info.json") as f:
    STATION_INFO = json.load(f)

def load_latest_summary():
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="summaries/")
        if "Contents" not in response:
            return []
        latest = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)[0]
        obj = s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])
        content = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))

        return df.set_index("station_id").to_dict(orient="index")
    except Exception as e:
        print(f"Could not load summary: {e}")
        return {}

STATION_SUMMARY  = load_latest_summary()
print(f"Loaded summary data for {len(STATION_SUMMARY)} stations")

@app.get("/stations")
def get_stations():
    results=[]
    for key in r.scan_iter("station:*"):
        status = json.loads(r.get(key))
        station_id = status["station_id"]
        info = STATION_INFO.get(station_id)

        if not info:
            continue
        
        summary = STATION_SUMMARY.get(station_id, {})

        results.append({
            "station_id": station_id,
            "name": info["name"],
            "lat": info["lat"],
            "lon": info["lon"],
            "bikes_available": status.get("num_bikes_available",0), # getting live feed data from redis
            "ebikes_available": status.get("num_ebikes_available",0),
            "docks_available": status.get("num_docks_available",0),
            "avg_bikes_available": round(summary.get("avg_bikes_available", 0),1),
            "min_bikes_available": summary.get("min_bikes_available",0),
            "max_bikes_available": summary.get("max_bikes_available",0),
            "readings": summary.get("readings",0),
        })
    return results

@app.get("/summary/refresh")
def refresh_summary():
    global STATION_SUMMARY
    STATION_SUMMARY = load_latest_summary()
    return {"loaded": len(STATION_SUMMARY)}