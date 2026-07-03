import json
import requests

STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

def main():
    response = requests.get(STATION_INFO_URL, timeout = 10)
    response.raise_for_status()
    stations = response.json()["data"]["stations"]

    lookup = {
        s["station_id"]:{
            "name":s["name"],
            "lat":s["lat"],
            "lon":s["lon"]
        }
        for s in stations
    }

    with open("data/station_info.json","w") as f:
            json.dump(lookup,f)

            print(f"SAVED info for {len(lookup)} stations to data/station_info.json")

if __name__ =="__main__":
      main()