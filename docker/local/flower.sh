#!/bin/bash

export AIRFLOW_HOME="$PWD"

rm airflow-flower.*

airflow celery flower