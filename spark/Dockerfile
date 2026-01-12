FROM apache/spark:3.4.2

# Use Spark's default Python 3.8
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

ARG SPARK_VERSION=3.4.2
ARG HADOOP_VERSION=3

RUN curl -L -o /opt/spark/jars/elasticsearch-spark-30_2.12-7.16.1.jar \
 https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/7.16.1/elasticsearch-spark-30_2.12-7.16.1.jar