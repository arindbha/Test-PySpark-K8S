FROM apache/spark:4.0.0-python3

USER root

# Install Python dependencies for Spark jobs
COPY spark_jobs/requirements.txt /opt/spark/jobs/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/jobs/requirements.txt

# Copy Spark job code
COPY spark_jobs/ /opt/spark/jobs/

# Download connector JARs
ENV JARS_DIR=/opt/spark/jars
RUN curl -fsSL -o ${JARS_DIR}/spark-bigquery-with-dependencies.jar \
    "https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.36.1.jar" && \
    curl -fsSL -o ${JARS_DIR}/gcs-connector.jar \
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"

USER spark

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["driver"]
