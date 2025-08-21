FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# system libs; build-essential helps wheels that need compiling on some regions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# fly’s HTTP service maps to 8080 by default
CMD ["uvicorn", "index:app", "--host", "0.0.0.0", "--port", "8080"]
