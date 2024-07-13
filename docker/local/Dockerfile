FROM python:buster

RUN apt-get update
RUN apt-get install libssl-dev comerr-dev libsasl2-dev postgresql-client-common postgresql-client-* -y

WORKDIR airflow

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY docker/local/*.sh ./

RUN chmod +x scheduler.sh && chmod +x webserver.sh && chmod +x worker.sh && chmod +x triggerer.sh && chmod +x flower.sh
