FROM apache/airflow:2.9.0-python3.10
ENV AIRFLOW_VERSION=2.9.0

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="${AIRFLOW_HOME}/dags:${PYTHONPATH}"