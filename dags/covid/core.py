import json
from io import StringIO
from datetime import datetime
from pandas import read_csv
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from connections.constants import S3__WRITE__POSTGRES_MISC
from helpers.utils import convert_date


S3_BUCKET: str = 'datalabs-postgres-misc'
S3_BASE_PATH: str = 'covid/{execution_date}'
S3_BASE_PREPARATION_PATH: str = f'{S3_BASE_PATH}/{{type}}/'
DATE_FORMAT: str = '%Y-%m-%d'


def upload_s3(filename: str, data: str):
    hook = S3Hook(
        aws_conn_id=S3__WRITE__POSTGRES_MISC,
    )
    print(hook)

    hook.load_string(
        string_data=data,
        key=filename,
        bucket_name=S3_BUCKET
    )


def upload_csv_to_s3(execution_date: str, data: str, type:str):
    buffer_write = StringIO()
    df_data = read_csv(StringIO(data))
    print(df_data)

    df_data.to_csv(
        buffer_write,
        index=False
    )
    buffer_write.seek(0)

    file_path = S3_BASE_PREPARATION_PATH.format(
        execution_date=execution_date,
        type=type,
    ) + f'{type}_data.csv'

    upload_s3(
        file_path,
        buffer_write.getvalue()
    )
    
    return file_path 


def read_csv_from_s3(file_path: str):
    hook = S3Hook(
        aws_conn_id=S3__WRITE__POSTGRES_MISC,
    )
    file_obj = hook.get_key(
        key=file_path,
        bucket_name=S3_BUCKET
    )
    data = file_obj.get()['Body'].read().decode('utf-8')
    return read_csv(StringIO(data))


def prep_data(df):
    df.drop_duplicates(
        inplace=True
    )
    df['date'] = df['date'].apply(convert_date)
    df.dropna(
        subset=['date'],
        inplace=True
    )
    df['date'] = df['date'].apply(lambda x: x.strftime(DATE_FORMAT))
    return df