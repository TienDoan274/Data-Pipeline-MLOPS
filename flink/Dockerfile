# flink/Dockerfile (WITH KAFKA CONNECTOR)
FROM flink:1.18-scala_2.12-java11

USER root

# Install Python and dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir \
        apache-flink==1.18.0 \
        requests==2.31.0 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download Flink Kafka connector
RUN wget -P /opt/flink/lib/ \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

USER flink
WORKDIR /opt/flink