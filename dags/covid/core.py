from pandas import read_csv
import json
from io import StringIO
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from connections.constants import S3__WRITE__POSTGRES_MISC


S3_BUCKET: str = 'postgres-misc'
S3_BASE_PATH: str = 'covid/{execution_date}'
S3_BASE_PREPARATION_PATH: str = f'{S3_BASE_PATH}/{type}/'


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


def upload_csv_to_s3(execution_date: str, data: str, file_type:str):
    buffer_write = StringIO()
    df_data = read_csv(StringIO(data))
    print(df_data)

    df_data.to_csv(
        buffer_write,
        index=False
    )
    buffer_write.seek(0)

    filename = S3_BASE_PREPARATION_PATH.format(
        execution_date=execution_date,
        type=file_type,
    ) + '.csv'

    upload_s3(
        filename,
        buffer_write.getvalue()
    )
    
    return filename 