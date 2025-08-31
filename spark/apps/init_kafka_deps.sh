#!/bin/bash

# Script to download Kafka and Avro dependencies for Spark
# Run this inside the Spark container

echo "Downloading Spark Kafka and Avro dependencies..."

# Create jars directory if it doesn't exist
mkdir -p /opt/bitnami/spark/jars

# Function to download with curl
download_jar() {
    local url=$1
    local filename=$(basename $url)
    echo "Downloading $filename..."
    curl -L -o "/opt/bitnami/spark/jars/$filename" "$url"
}

# Download Kafka dependencies
echo "Downloading Kafka SQL dependencies..."
download_jar "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"

# Download Avro dependencies
echo "Downloading Avro dependencies..."
download_jar "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar"

echo "Dependencies downloaded successfully!"
echo "You can now run Spark jobs with Kafka and Avro support."