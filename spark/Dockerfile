FROM bitnami/spark:3.4.1

# Switch to root user
USER root

# Install curl
RUN install_packages curl

# Download Nessie and Iceberg JARs
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.2.23.jar \
    https://jdbc.postgresql.org/download/postgresql-42.2.23.jar && \
    curl -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.3_2.12-1.3.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.3.0/iceberg-spark-runtime-3.3_2.12-1.3.0.jar && \
    curl -o /opt/bitnami/spark/jars/nessie-spark-extensions-3.3_2.12-0.60.0.jar \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions-3.3_2.12/0.60.0/nessie-spark-extensions-3.3_2.12-0.60.0.jar


    # Download Hadoop AWS and AWS SDK JARs
RUN curl -o /opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
curl -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar


RUN pip install boto3 

# Change ownership of the jars directory
RUN chown -R 1001:1001 /opt/bitnami/spark/jars/

# Switch back to non-root user
USER 1001

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark

# Expose necessary ports (if needed)
EXPOSE 8080 7077
