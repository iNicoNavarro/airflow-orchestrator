import requests
from datetime import datetime
from helpers.extras import DAG
from airflow.decorators import task
from connections.aws.postgres import PostgresDataProdHook, PostgresStagingHook
from covid.core import upload_csv_to_s3


GET_DATA__TASK_ID: str = 'get_data'
TO_S3__TASK__ID: str = 'upload_s3'
TO_POSTGRES__TASK_ID: str = 'to_postgres'

CREATE_STG__TASK_ID: str = 'create_stg'
UPDATE_FINAL_TABLE__TASK_ID: str = 'update_final_table'
INSERT_FINAL_TABLE__TASK_ID: str = 'insert_to_final_table'

STAGING_TABLE: str = 'ecovis_test.covid_us'
FINAL_TABLE:str = 'finance.report_bc'
DATE_FORMAT: str = '%Y-%m-%d'



@task(task_id=GET_DATA__TASK_ID)
def get_and_upload_raw():
    execution_date = datetime.now().strftime('%Y%m%d%H%M%S')
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.content.decode('utf8')

        upload_csv_to_s3(
            execution_date, 
            data, 
            file_type='raw'
        )

    return 'revisar_s3'

    # with PostgresDataProdHook().get_conn() as conn:
    #     with conn.cursor() as cursor:
    #         cursor.execute("SELECT 1")
    #         result = cursor.fetchall()
    #         print(f"Connection test result: {result}")
    #         conn.commit()
    # return result


with DAG(schedule='25 6 * * *', owners=['Nico']) as dag:
    get_and_upload_raw()