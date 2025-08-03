FROM python:3.11-slim

WORKDIR /app

COPY event_processor_consumer.py .

RUN pip install --no-cache-dir confluent-kafka google-cloud-bigquery redis requests

CMD ["python", "event_processor_consumer.py"]
