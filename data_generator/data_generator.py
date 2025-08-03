import psycopg2
import random
import time
import uuid
from datetime import datetime, timedelta,timezone
import sys

# إعدادات الاتصال بقاعدة البيانات
conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="engagement_db",
    user="user",
    password="pass"
)
cursor = conn.cursor()

# محتوى تجريبي لملء جدول content (مرة وحدة)
def seed_content():
    cursor.execute("SELECT COUNT(*) FROM content;")
    count = cursor.fetchone()[0]
    if count > 0:
        print("✅ content table already has data.")
        sys.stdout.flush()
        return

    content_types = ['podcast', 'newsletter', 'video']
    for i in range(10):
        content_id = str(uuid.uuid4())
        slug = f"content-{i+1}"
        title = f"Title {i+1}"
        content_type = random.choice(content_types)
        length_seconds = random.randint(60, 1800)
        publish_ts = datetime.utcnow() - timedelta(days=random.randint(1, 30))

        cursor.execute("""
            INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (content_id, slug, title, content_type, length_seconds, publish_ts))

    conn.commit()
    print("✅ content table seeded.")
    sys.stdout.flush()

# توليد حدث تفاعل عشوائي
def generate_engagement_event():
    cursor.execute("SELECT id, length_seconds FROM content ORDER BY RANDOM() LIMIT 1;")
    row = cursor.fetchone()
    if not row:
        print("❌ No content available.")
        sys.stdout.flush()
        return

    content_id, length_seconds = row
    user_id = str(uuid.uuid4())
    event_type = random.choice(['play', 'pause', 'finish', 'click'])
    event_ts = datetime.now(timezone.utc)
    duration_ms = random.randint(1000, min(length_seconds * 1000, 10000)) if event_type != 'click' else None
    device = random.choice(['ios', 'android', 'web‑safari', 'web‑chrome'])
    raw_payload = {'session_id': str(uuid.uuid4())}

    cursor.execute("""
        INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        content_id, user_id, event_type, event_ts, duration_ms, device, json.dumps(raw_payload)
    ))

    conn.commit()
    print(f"✅ Inserted {event_type} event for content {content_id}")

# التشغيل الرئيسي
if __name__ == "__main__":
    import json
    seed_content()
    print("🚀 Generating engagement events...")
    sys.stdout.flush()
    while True:
        generate_engagement_event()
        time.sleep(random.randint(2, 5))
