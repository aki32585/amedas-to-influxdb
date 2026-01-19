import requests

from dotenv import load_dotenv
import os
load_dotenv()

import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from apscheduler.schedulers.background import BlockingScheduler

token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
bucket = os.getenv("INFLUXDB_BUCKET")

write_api = write_client.write_api(write_options=SYNCHRONOUS)

def get_amds_data(no):
    url = f'https://api.cultivationdata.net/amds?no={no}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

def main():
    no = os.getenv("AMDS_NO")
    try:
        data = get_amds_data(no)
        print("Get data from API:")
        print(data)
        point = (
    Point("haneda")
    .field("temp", data['temp'][0])
    .field("precipitation10m", data['precipitation10m'][0])
    .field("wind", data['wind'][0])
  )
        write_api.write(bucket=bucket, org=org, record=point)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', minutes = int(os.getenv("SCHEDULE_MINUTES") or 5))
    print(f"Scheduler started. Running every {os.getenv('SCHEDULE_MINUTES')} minutes.")
    main()
    scheduler.start()