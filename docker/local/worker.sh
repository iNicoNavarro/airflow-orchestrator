#!/bin/bash

export AIRFLOW_HOME="$PWD"

airflow celery worker