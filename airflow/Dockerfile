# airflow/Dockerfile
# ───────────────────
# Custom Airflow image with OpenJDK 17 (required for PySpark) and all
# NexCart pipeline Python dependencies pre-installed.
#
# Built automatically by docker-compose — no manual build needed.

FROM apache/airflow:2.9.1-python3.11

# ── Install Java as root ───────────────────────────────────────────────────────
USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Install Python packages as airflow user ────────────────────────────────────
USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
