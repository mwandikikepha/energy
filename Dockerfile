FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY . .

RUN uv pip install --system \
    "fastapi[standard]" \
    "uvicorn[standard]" \
    "jinja2" \
    "python-multipart" \
    "requests" \
    "beautifulsoup4" \
    "lxml" \
    "pymongo" \
    "python-dotenv" \
    "pydantic" \
    "pydantic-settings" \
    "httpx" \
    "apache-airflow==2.9.0"

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow_home
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/airflow/dags
ENV AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
ENV AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30

EXPOSE 8000

CMD ["sh", "-c", "\
    airflow db migrate && \
    airflow scheduler & \
    uvicorn api.main:app --host 0.0.0.0 --port 8000 \
"]
