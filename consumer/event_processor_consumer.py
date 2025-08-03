from confluent_kafka import Consumer
import json
import os
from google.cloud import bigquery
from datetime import datetime
from pathlib import Path
import redis
import time
import requests
import sys

# Auth to GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bq_key.json"



# Init BigQuery client
bq_client = bigquery.Client()
dataset_id = "engagement"
table_id = "eventss"
consumer = Consumer({
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'latest'  
})

topic = "audience_eventss"
consumer.subscribe([topic])

print(f"Listening for messages on topic '{topic}'...")
sys.stdout.flush()

def send_to_external_system(data):
    url = "https://httpbin.org/post"  #  replace with real API if needed
    try:
        response = requests.post(url, json=data, timeout=5)
        response.raise_for_status()
        print(f" Sent event {data['event_id']} to external system. Status: {response.status_code}")
        sys.stdout.flush()
    except requests.exceptions.RequestException as e:
        print(f" Failed to send event {data['event_id']}: {e}")
        sys.stdout.flush()
        
        

redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
def send_to_redis(row):
    content_type = row["content_type"]
    event_ts = row["event_ts"].timestamp()  # تأكد إنه datetime
    device = row["device"]
    # نضيف المحتوى إلى Sorted Set
    redis_client.zadd("engagement_ranking", {content_type: event_ts})
    redis_client.zincrby("most_devices_used",1,device)
    now = time.time()
    print(f"now: {now}, event_ts:{event_ts}")
    sys.stdout.flush()
    

    latency = now - event_ts

    print(f"⏱️ Redis latency = {latency:.2f} seconds")
    sys.stdout.flush()

    # نحذف الأحداث الأقدم من 10 دقائق
    ten_minutes_ago = event_ts - 600
    redis_client.zremrangebyscore("engagement_ranking", 0, ten_minutes_ago)
    get_top_engagements()
    time.sleep(0.1)
    
    
    
    
def get_top_engagements(limit=10):
    top = redis_client.zrevrange("engagement_ranking",0,limit - 1,withscores=True)
    top_device = redis_client.zrevrange("most_devices_used",0,2,True)
    if not top :
        print("No data found")
        sys.stdout.flush()
        return
    elif not top_device:
        print("No data found")
        sys.stdout.flush()
        return
    print(f"top 3 device used")
    sys.stdout.flush()
    for rank , (device,score) in enumerate(top_device,1):
        print(f"{rank}. {device} counts {score}")
        sys.stdout.flush()
    
    print(f"\ntop {limit} content items in the last 10 mins")
    sys.stdout.flush()
    for rank,(content_type,score) in enumerate(top,1):
        engagement_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(score))
        print(f"{rank}. {content_type} (last engagement: {engagement_time})")
        sys.stdout.flush()

def write_to_bq(rows):## needs billed account
    table_ref= bq_client.dataset(dataset_id).table(table_id)
    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
         print("❌ BigQuery insert errors:", errors)
         sys.stdout.flush()
    else:
        print(f" Inserted {len(rows)} row(s) into BigQuery.")
        sys.stdout.flush()
print("📥 Listening to Kafka topic and writing to external system...")
sys.stdout.flush()
try:
    while True:
        msg = consumer.poll(0.5)  # ينتظر رسالة لمدة ثانية
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            sys.stdout.flush()
        value = msg.value().decode('utf-8')
        try:
            event = json.loads(value)

            # Convert string to datetime object
            if 'event_ts' in event:
                event['event_ts'] = datetime.fromisoformat(event['event_ts'].replace(" ", "T"))

            send_to_redis(event)

            # Send to external system using string version
            event_for_api = dict(event)
            event_for_api['event_ts'] = event_for_api['event_ts'].isoformat()
            #write_to_bq([event_for_api])
            send_to_external_system(event_for_api)

        except Exception as e:
            print("⚠️ Failed to insert row:", e)
            sys.stdout.flush()
except KeyboardInterrupt:
    print("🛑 Stopped by user.")
    sys.stdout.flush()
finally:
    consumer.close()
