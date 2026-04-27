FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
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
    "httpx"

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Default command is the API — the cron service overrides this in Railway settings
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
