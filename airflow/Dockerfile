FROM apache/airflow:3.1.5-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY config/requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt