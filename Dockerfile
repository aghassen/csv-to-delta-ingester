FROM python:3.8-slim-buster

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /opt/applications

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY config/ /opt/applications/config
COPY src/ /opt/applications/src
COPY data/ /opt/applications/data

CMD ["spark-submit", "--master", "local[2]", \
     "--packages", "io.delta:delta-core_2.12:2.1.0", \
     "--conf", "spark.eventLog.enabled=true", \
     "--conf", "spark.eventLog.dir=file:/tmp/spark-events", \
     "--conf", "spark.history.fs.logDirectory=file:/tmp/spark-events", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "src/main.py"]
