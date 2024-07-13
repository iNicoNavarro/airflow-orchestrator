#!/bin/bash

export AIRFLOW_HOME="$PWD"

rm airflow-webserver.*

# start airflow{
airflow db migrate

airflow users create \
    --firstname ${AIRFLOW__ADMIN_FIRST_NAME} \
    --lastname ${AIRFLOW__ADMIN_LAST_NAME} \
    --role Admin \
    --emai ${AIRFLOW__ADMIN_EMAIL} \
    --username ${AIRFLOW__ADMIN_USER}  \
    --password ${AIRFLOW__ADMIN_PASSWORD}

airflow webserver -p 3000