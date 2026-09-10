FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# dbt gets its own venv, isolated from Airflow's Python env — dbt-core's
# pinned deps (Jinja2, Click, PyYAML) routinely conflict with Airflow's own,
# same reasoning as .venv-dbt in local dev.
COPY dbt/requirements-dbt.txt /opt/dbt-requirements.txt
RUN python3 -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir -r /opt/dbt-requirements.txt && \
    chown -R airflow:0 /opt/dbt-venv

USER airflow
