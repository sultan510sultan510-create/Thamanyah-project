FROM python:3.11-slim

WORKDIR /app

COPY event_processor_producer.py .

RUN pip install --no-cache-dir psycopg2-binary confluent-kafka redis

CMD ["python", "event_processor_producer.py"]
