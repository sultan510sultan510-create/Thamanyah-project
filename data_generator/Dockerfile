FROM python:3.11-slim

WORKDIR /app

COPY data_generator.py .

RUN pip install --no-cache-dir psycopg2-binary 

CMD ["python", "data_generator.py"]
