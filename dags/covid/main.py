import requests
from datetime import datetime
from helpers.extras import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from connections.aws.postgres import PostgresDataProdHook, PostgresStagingHook
from covid.core import upload_csv_to_s3, read_csv_from_s3, prep_data


GET_AND_UPLOAD_RAW__TASK_ID: str = 'get_and_uploadS3_raw'
TO_POSTGRES__TASK_ID: str = 'to_stg_table'

CREATE_STG__TASK_ID: str = 'create_stg'
UPDATE_INSERT_FINAL_TABLE__TASK_ID: str = 'update_insert_final_table'
DROP_TABLE__TASK_ID: str = 'drop_stg_table'

STAGING_TABLE: str = 'temp_tables.covid_us__stg'
FINAL_TABLE:str = 'ecovis_test.covid_us'


@task(task_id=CREATE_STG__TASK_ID)
def create_staging_table(current_dag: DAG, table):
    with PostgresDataProdHook().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute_drop_table(table)

            cursor.execute(
                current_dag.read_file(
                    'sources/create.sql'
                ).format(
                    staging_table=table
                )
            )
        conn.commit()


@task(task_id=GET_AND_UPLOAD_RAW__TASK_ID)
def get_and_upload_raw():
    execution_date = datetime.now().strftime('%Y%m%d%H%M%S')
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
    response = requests.get(url)
    if response.status_code == 200:
        raw_data = response.content.decode('utf-8')

        return upload_csv_to_s3(
            execution_date, 
            raw_data, 
            type='raw'
        )


@task(task_id=TO_POSTGRES__TASK_ID)
def insert_staging_table(file_path: str, table: str):
    df_data = read_csv_from_s3(file_path)
    df_cleaned = prep_data(df_data)
    with PostgresDataProdHook().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute_bulk_insert(
                list(
                    df_cleaned.itertuples(index=False, name=None)
                ),
                columns=df_data.columns.tolist(),
                table=table
            )

        conn.commit()   

@task(task_id=UPDATE_INSERT_FINAL_TABLE__TASK_ID)
def update_insert_data(current_dag: DAG, final_table: str, staging_table: str):
    with PostgresDataProdHook().get_conn() as conn:
        with conn.cursor() as cursor:
            for task in ['update', 'insert']:
                cursor.execute(
                    current_dag.read_file(
                        f'sources/{task}.sql'
                    ).format(
                        final_table=final_table,
                        staging_table=staging_table
                    )
                )
        conn.commit()

@task(task_id=DROP_TABLE__TASK_ID)
def drop_table(table: str):
    with PostgresDataProdHook().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute_drop_table(table)

        conn.commit()

with DAG(schedule='25 6 * * *', owners=['Nico']) as dag:
    raw_file_path = get_and_upload_raw()
    
    with TaskGroup('send_to_postgres') as group:
        create_staging_table(
            current_dag=dag,
            table=STAGING_TABLE
        ) >> insert_staging_table(
            file_path=raw_file_path,
            table=STAGING_TABLE
        ) >> update_insert_data(
            current_dag=dag,
            staging_table=STAGING_TABLE,
            final_table=FINAL_TABLE           
        ) >> drop_table(
            table=STAGING_TABLE
        )

    raw_file_path >> group