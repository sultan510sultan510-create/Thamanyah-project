import psycopg2.extras
import json
import time
from confluent_kafka import Producer
import redis
import os
import datetime
from pathlib import Path
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--backfill", action="store_true", help="Run in backfill mode")
parser.add_argument("--since", type=str, help="Start timestamp for backfill (e.g., '2024-08-01T00:00:00')")
args = parser.parse_args()

STATE_FILE = "last_processed.txt"
# used to track processed IDs
PROCESSED_KEY = "processed_event_ids"

def load_last_ts():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return float(f.read())
    return None

def save_last_ts(timestamp):
    with open(STATE_FILE, "w") as f:
        f.write(str(timestamp))

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="engagement_db",
    user="user",
    password="pass"
)
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
            sys.stdout.flush()
            
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            sys.stdout.flush()
            
topic ="audience_eventss"


producer = Producer({'bootstrap.servers':'kafka:29092',
                    'acks':'all'})


cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

 

def read_from_db():
    
    while True:
        if args.backfill:
            if args.since:
                last_processed_ts = datetime.datetime.fromisoformat(args.since).timestamp()
            else:
                last_processed_ts = 0  # من البداية
            print(f"🕒 Backfill mode ON. Since timestamp: {last_processed_ts}")
            sys.stdout.flush()
        else:
            last_processed_ts = load_last_ts()
            print(f"🚀 Live mode. Last processed timestamp: {last_processed_ts}")
            sys.stdout.flush()

        
        print(f"last_processt_ts : {last_processed_ts}")
        sys.stdout.flush()
        if last_processed_ts:
            cursor.execute("""
                SELECT ee.id as event_id,
ee.content_id,
ee.user_id,
ee.event_type ,
ee.event_ts ,
ee.duration_ms ,
ee.device ,
ee.raw_payload ,
c.content_type ,
c.length_seconds
                FROM engagement_events ee
                JOIN content c ON ee.content_id = c.id
                WHERE ee.event_ts > to_timestamp(%s)
                ORDER BY ee.event_ts ASC
                LIMIT 100;
            """, (last_processed_ts,))
        else:
            cursor.execute("""
                SELECT ee.id as event_id,
ee.content_id,
ee.user_id,
ee.event_type ,
ee.event_ts ,
ee.duration_ms ,
ee.device ,
ee.raw_payload ,
c.content_type ,
c.length_seconds
                FROM engagement_events ee
                JOIN content c ON ee.content_id = c.id
                ORDER BY ee.event_ts ASC
                LIMIT 100;
            """)
        data = cursor.fetchall()
        batch_rows = []
        for rows in data:
            row = dict(rows)
            event_id = str(row["event_id"])

            # ✅ منع التكرار
            if redis_client.sismember(PROCESSED_KEY, event_id):
                continue  # skip
            if row["duration_ms"] is None or row["length_seconds"] is None:
                row["engagement_seconds"] = None
                row["engagement_pct"] = None
            else:
                a = (row["duration_ms"]/1000.0)
                b = row["length_seconds"]
                row["engagement_seconds"] = round(a, 2)
                row["engagement_pct"] = round(a / b, 2)
            
            producer.produce(
                topic,
                key = event_id,
                value = json.dumps(row,default=str),
                callback = delivery_callback
            )
            redis_client.sadd(PROCESSED_KEY, event_id)
            print(f"DEBUG row = {row}")
            sys.stdout.flush()
            
            
            if not args.backfill:
                save_last_ts(row["event_ts"].timestamp())
            if isinstance(row["event_ts"], datetime.datetime):
                row["event_ts"] = row["event_ts"].isoformat()
            row["duration_ms"] = int(row["duration_ms"]) if row["duration_ms"] is not None else None
            row["engagement_pct"] = float(row["engagement_pct"]) if row["engagement_pct"] is not None else None
            row["engagement_seconds"] = float(row["engagement_seconds"]) if row["engagement_seconds"] is not None else None

            batch_rows.append(row)
        if batch_rows:
            save_batch_to_json(batch_rows)
        producer.flush()
            
        
        # ⏱️ انتظر ثواني قبل الجولة التالية
        time.sleep(0.5)
        print(f"🔁 Cycle complete. Waiting for new events...\n")
        sys.stdout.flush()
        

    
def save_batch_to_json(batch):
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    folder = Path("bigquery_batches")
    folder.mkdir(exist_ok=True)
    filepath = folder / f"event_{now}.json"
    with open(filepath, "w") as f:
        for row in batch:
            f.write(json.dumps(row) + "\n")
    print(f" Saved batch to {filepath}")
    sys.stdout.flush()
    
    


read_from_db()


