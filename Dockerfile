# Pipeline image: Python 3.13 (same major version as the development environment)
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies first so this layer is cached until requirements.txt changes
COPY requirements.txt .
RUN pip install -r requirements.txt

# Code and configuration. Data, state, metadata and outputs are mounted by docker-compose.yml.
COPY src ./src
COPY config ./config
COPY sql ./sql

CMD ["python", "-m", "src.cli", "pipeline"]
