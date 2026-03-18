FROM python:3.11-slim

# Install JDK 17 for spark-submit binary
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Spark distribution (for spark-submit)
ENV SPARK_VERSION=4.0.0
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    | tar -xz -C /opt && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}
ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml .
COPY fastapi_app/ fastapi_app/
COPY pipelines/ pipelines/
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "fastapi_app.main:app", "--host", "0.0.0.0", "--port", "8000"]
