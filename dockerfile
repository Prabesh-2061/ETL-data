FROM apache/airflow:3.2.1

# Installing all providers and packages
RUN pip install \
    apache-airflow-providers-postgres \
    psycopg2-binary \
    requests \
    apache-airflow-providers-apache-spark 
    

USER root

# Installing Java and procps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setting environment variables
ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV SPARK_MASTER_HOST="spark-master"
ENV SPARK_MASTER_PORT="7077"

RUN mkdir -p ${SPARK_HOME}

# Archive URL for spark 3.5.1 with Hadoop 3
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 && \
    rm -rf spark-3.5.1-bin-hadoop3.tgz

RUN chown -R airflow ${SPARK_HOME}

COPY spark-jars/postgresql.jar ${SPARK_HOME}/jars/postgresql.jar
COPY spark-jars/hadoop-aws-3.3.4.jar ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar
COPY spark-jars/aws-java-sdk-bundle-1.12.262.jar ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar

USER airflow