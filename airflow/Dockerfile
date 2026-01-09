FROM apache/airflow:2.8.1-python3.8

USER root

ARG SPARK_VERSION=3.4.2
ARG HADOOP_VERSION=3

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark (MATCHES cluster)
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

