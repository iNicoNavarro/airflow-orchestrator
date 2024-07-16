from helpers.extras import DAG
from airflow.decorators import task
from connections.sql.postgres import PostgresDataProdHook, PostgresStagingHook


TEST__TASK_ID: str = 'Test'

@task(task_id=TEST__TASK_ID)
def extract_data():
    with PostgresDataProdHook().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.execute_bul
            result = cursor.fetchall()
            print(f"Connection test result: {result}")
            conn.commit()
    return result


with DAG(schedule='25 6 * * *', owners=['Nico']) as dag:
    extract_data()