FROM apache/spark:4.0.0-python3

USER root

COPY requirements.txt /opt/spark/work-dir/
RUN pip install --no-cache-dir -r /opt/spark/work-dir/requirements.txt

COPY spark_jobs/ /opt/spark/work-dir/spark_jobs/
COPY pyproject.toml /opt/spark/work-dir/

USER spark

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["driver"]
