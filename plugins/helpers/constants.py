AIRFLOW_DAG_PATH: str = '/airflow/dags/'

USER__NICOLAS_NAVARRO: str = 'Nicolas Navarro'

USER_EMAILS: dict = {
    USER__NICOLAS_NAVARRO: 'nicolasnavarro@foodology.com.co',
}

PRIORITY__CRITICAL: int = 10000
PRIORITY__HIGH: int = 1000
PRIORITY__MEDIUM: int = 100
PRIORITY__REGULAR: int = 10
PRIORITY__LOW: int = 1

PRIORITY__DEFAULT: int = PRIORITY__REGULAR

FREQUENT_POOL: str = 'frequent_tasks'
CRITICAL_POOL: str = 'critical_tasks'
