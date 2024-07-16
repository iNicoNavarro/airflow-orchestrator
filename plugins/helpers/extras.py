import os
import json
import inspect
import airflow
from datetime import datetime, timedelta
from airflow.utils.weight_rule import WeightRule
from helpers.constants import AIRFLOW_DAG_PATH, USER_EMAILS, USER__NICOLAS_NAVARRO, PRIORITY__DEFAULT


def get_main_filename() -> str:
    for item in inspect.stack():
        if AIRFLOW_DAG_PATH in item.filename:
            name = os.path.dirname(item.filename)

            return name[name.find(AIRFLOW_DAG_PATH) + len(AIRFLOW_DAG_PATH):]


def get_xcom(task_id: str, context: dict):
    return context['ti'].xcom_pull(
        task_ids=task_id
    )


def safe_json(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result is None:
            return result

        return json.loads(
            json.dumps(
                result,
                default=str
            ).replace(
                ': NaN',
                ': null'
            )
        )

    return wrapper


class DAG(airflow.DAG):
    DEFAULT_ARGS = {
        'retries': 5,
        'retry_delay': timedelta(minutes=2),
        'email_on_failure': True,
        'email': [
            USER_EMAILS[USER__NICOLAS_NAVARRO]
        ],
        'email_on_retry': False,
        'weight_rule': WeightRule.ABSOLUTE,
        'priority_weight': PRIORITY__DEFAULT
    }

    def __init__(
            self,
            dag_id: str = None,
            tags: list = None,
            catchup: bool = False,
            start_date: datetime = datetime(2022, 1, 1),
            default_args: dict = DEFAULT_ARGS,
            owners: list = None,
            **kwargs
    ):
        if dag_id is None:
            dag_id = self.build_dag_name()

        if tags is None:
            tags = self.build_tags()

        if owners is not None:
            default_args['email'] = list(
                set(
                    default_args['email'] + [
                        USER_EMAILS[owner]
                        for owner in owners
                        if owner in USER_EMAILS
                    ]
                )
            )

            if 'owner' in default_args:
                owners = owners + [default_args['owner']]

            owners = list(set(owners))
            default_args['owner'] = ", ".join(owners)

        super(DAG, self).__init__(
            dag_id=dag_id,
            tags=tags,
            catchup=catchup,
            start_date=start_date,
            default_args=default_args,
            doc_md=inspect.currentframe().f_back.f_locals['__doc__'],
            **kwargs
        )

    @staticmethod
    def build_dag_name():
        return get_main_filename().replace('/', '__')

    @staticmethod
    def build_tags():
        return get_main_filename().split('/')[:-1]

    @property
    def variable(self):
        return Variable.from_variable(
            self._dag_id
        )

    @staticmethod
    def read_file(path: str, *args, **kwargs) -> str:
        base_path = AIRFLOW_DAG_PATH + get_main_filename()
        path = os.path.join(base_path, path)

        with open(path, *args, **kwargs) as file:
            return file.read()


class Variable(dict):
    def __getitem__(self, item):
        subset = dict(self)

        for path in item.split('.'):
            subset = subset[path]

        return self.from_output(
            subset
        )

    def __getattr__(self, item):
        try:
            return self.from_output(
                self[item]
            )
        except KeyError as e:
            raise AttributeError(str(e))

    @classmethod
    def from_output(cls, data):
        if isinstance(data, dict):
            return Variable(data)

        return data

    @classmethod
    def from_variable(cls, name: str):
        from airflow.models import Variable

        try:
            variable = Variable.get(
                name,
                deserialize_json=True
            )

        except KeyError:
            return None

        return cls.from_output(
            variable
        )
