from helpers.extras import DAG
from airflow.decorators import task
from connections.aws.postgres import PostgresDataProdHook



METRICS_PER_MONTH__TASK_ID: str = 'metrics_per_month_report'
TOP5_REPORT__TASK_ID: str = 'top5_reports'


MONTHLY_VIEW: str = 'ecovis_test.monthly_covid_summary'
TOP5_VIEW: str = 'ecovis_test.top_5_report'


@task(task_id=METRICS_PER_MONTH__TASK_ID)
def execute_monthly_report(current_dag: DAG):
    with PostgresDataProdHook().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                current_dag.read_file(
                    'sources/metrics_per_month.sql'
                ).format(
                    view_name=MONTHLY_VIEW
                )
            )
        conn.commit()


@task(task_id=TOP5_REPORT__TASK_ID)
def execute_top5_report(current_dag: DAG):
    with PostgresDataProdHook().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                current_dag.read_file(
                    'sources/top5.sql'
                ).format(
                    view_name=TOP5_VIEW                
                )
            )
        conn.commit()

with DAG(schedule='25 6 * * *', owners=['Nico']) as dag:
    execute_monthly_report(
        current_dag=dag
    ), 
    execute_top5_report(
        current_dag=dag
    )