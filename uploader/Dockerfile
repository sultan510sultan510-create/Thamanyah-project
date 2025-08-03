FROM python:3.11-slim
WORKDIR /app
COPY load_to_bigquery.py .
RUN pip install google-cloud-bigquery
CMD ["python", "load_to_bigquery.py"]