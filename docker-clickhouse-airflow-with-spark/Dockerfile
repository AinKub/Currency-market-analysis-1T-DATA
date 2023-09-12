FROM apache/airflow:2.7.0

USER root

RUN apt update && \
    apt install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark