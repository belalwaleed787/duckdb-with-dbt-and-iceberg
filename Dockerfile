# NYC taxi lakehouse: dbt + DuckDB writing Iceberg tables (Glue catalog) to S3.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --create-home --uid 10001 dbt

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Bake the DuckDB extensions into the image (into /home/dbt/.duckdb) so runs never download them.
USER dbt
RUN python -c "import duckdb; con = duckdb.connect(); [con.install_extension(e) for e in ('httpfs', 'aws', 'iceberg', 'avro')]"

USER root
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh && chmod 755 /usr/local/bin/entrypoint.sh
COPY --chown=dbt:dbt nyc_taxi /app/nyc_taxi

# Writable paths live under /tmp so the container also works with a read-only root filesystem.
ENV DBT_PROJECT_DIR=/app/nyc_taxi \
    DBT_PROFILES_DIR=/app/nyc_taxi \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DUCKDB_PATH=/tmp/dbt/nyc_taxi.duckdb \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DBT_USE_COLORS=false \
    PYTHONPATH=/app/nyc_taxi/plugins \
    AWS_REGION=us-east-1 \
    LAKEHOUSE_ROOT=s3://nyc-test-iceberg \
    LANDING_GLOB=s3://nyc-test-iceberg/landing/yellow_taxi/*.parquet \
    DBT_THREADS=4

USER dbt
WORKDIR /app/nyc_taxi
# Fail the image build if the project does not parse.
RUN dbt parse --quiet

ENTRYPOINT ["entrypoint.sh"]
CMD ["build"]
