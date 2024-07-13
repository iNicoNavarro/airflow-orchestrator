#!/bin/bash

export AIRFLOW_HOME="$PWD"

rm airflow-triggerer.*

airflow triggerer