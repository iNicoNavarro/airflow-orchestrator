#!/bin/bash

export AIRFLOW_HOME="$PWD"

rm airflow-scheduler.*

airflow scheduler