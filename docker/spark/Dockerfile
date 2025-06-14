# syntax=docker/dockerfile:1
FROM python:3.10-slim-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspace
CMD ["sleep", "infinity"]
