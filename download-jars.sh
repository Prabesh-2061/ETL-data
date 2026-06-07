#!/bin/bash
mkdir -p spark-jars

echo "Downloading PostgreSQL JDBC driver..."
curl -L -o spark-jars/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

echo "Downloading Hadoop AWS..."
curl -L -o spark-jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

echo "Downloading AWS Java SDK Bundle..."
curl -L -o spark-jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

echo "Done! JARs downloaded to spark-jars/"