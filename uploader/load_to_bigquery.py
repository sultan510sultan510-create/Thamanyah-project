#========================this code is not working without billed account in GCP=======================================================================================
# from confluent_kafka import Consumer
# from google.cloud import bigquery
# import json
# import os
# from datetime import datetime
# # Auth to GCP
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bq_key.json"



# # Init BigQuery client
# bq_client = bigquery.Client()
# dataset_id = "engagement"
# table_id = "events"


# consumer = Consumer({
#     'bootstrap.servers': 'kafka:29092',
#     'group.id': 'my-consumer-group',
#     'auto.offset.reset': 'earliest'  # عشان تقرأ من البداية
# })

# topic = "audience_events"
# consumer.subscribe([topic])

# print(f"Listening for messages on topic '{topic}'...")


# def write_to_bq(rows):
#     table_ref= bq_client.dataset(dataset_id).table(table_id)
#     errors = bq_client.insert_rows_json(table_ref, rows)
#     if errors:
#          print("❌ BigQuery insert errors:", errors)
#     else:
#         print(f" Inserted {len(rows)} row(s) into BigQuery.")
# print("📥 Listening to Kafka topic and writing to BigQuery...")
# try:
#     while True:
#         msg = consumer.poll(1.0)  # ينتظر رسالة لمدة ثانية
#         if msg is None:
#             continue
#         if msg.error():
#             print("❌ Error:", msg.error())
#         value = msg.value().decode('utf-8')
#         try:
#             event = json.loads(value)
#             # Ensure datetime format for BQ
#             if 'event_ts' in event:
#                 event['event_ts'] = event['event_ts'].replace(" ", "T")

#             write_to_bq([event])

#         except Exception as e:
#             print("⚠️ Failed to insert row:", e)
# except KeyboardInterrupt:
#     print("🛑 Stopped by user.")
# finally:
#     consumer.close()
#===============================================================================================================
# from google.cloud import bigquery
# from pathlib import Path
# import os
# import time
# import sys
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bq_key.json"
# client = bigquery.Client()
# table_id = "storage-project-419518.engagement.eventss"  # update with your project/dataset/table

# def upload_json_files():
#     folder = Path("bigquery_batches")
#     for file in folder.glob("*.json"):
#         print(f"🚀 Uploading {file.name}")
#         sys.stdout.flush()
#         with open(file, "rb") as f:
#             job = client.load_table_from_file(
#                 f,
#                 table_id,
#                 job_config=bigquery.LoadJobConfig(
#                     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
#                     autodetect=True,
#                     write_disposition="WRITE_APPEND"
#                 ),
#             )
#         job.result()
#         print(f"Uploaded: {file.name}")
#         sys.stdout.flush()
#         file.unlink()  # delete after upload

import json
from pathlib import Path
from google.cloud import bigquery
import sys
import time

client = bigquery.Client()
table_id = "storage-project-419518.engagement.eventss"  

def is_valid_json_line(line):
    try:
        data = json.loads(line)
        return isinstance(data, dict) and len(data) > 0
    except json.JSONDecodeError:
        return False

def upload_json_files():
    folder = Path("bigquery_batches")
    folder.mkdir(parents=True, exist_ok=True)  # يضمن أن المجلد موجود

    for file in folder.glob("*.json"):
        print(f"🚀 Checking {file.name} for valid JSON...")
        sys.stdout.flush()

        with open(file, "r") as f:
            valid_lines = [line for line in f if is_valid_json_line(line)]

        if not valid_lines:
            print(f"❌ Skipping {file.name} (no valid JSON lines)")
            sys.stdout.flush()
            file.unlink()
            continue

        clean_file = file.with_name(f"clean_{file.name}")
        with open(clean_file, "w") as f:
            f.writelines(valid_lines)

        print(f"📤 Uploading clean version of {file.name} to BigQuery...")
        sys.stdout.flush()
        with open(clean_file, "rb") as f:
            job = client.load_table_from_file(
                f,
                table_id,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    autodetect=True,
                    write_disposition="WRITE_APPEND"
                ),
            )
        job.result()

        print(f"✅ Uploaded: {file.name}")
        sys.stdout.flush()

        # حذف الملفات بعد الرفع
        clean_file.unlink()
        file.unlink()

if __name__ == "__main__":
    while True:
        upload_json_files()
        time.sleep(5)