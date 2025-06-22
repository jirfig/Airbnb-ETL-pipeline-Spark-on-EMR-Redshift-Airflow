# syntax=docker/dockerfile:1
FROM python:3.10-slim-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# Create a generic user that will be replaced by the host user
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN groupadd -g ${GROUP_ID} appuser && \
    useradd -u ${USER_ID} -g appuser -m -s /bin/bash appuser

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspace
RUN chown -R appuser:appuser /workspace

USER appuser
CMD ["sleep", "infinity"]
