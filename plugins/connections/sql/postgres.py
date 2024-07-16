import datetime

from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import cursor, connection
from connections.sql.base import BaseCursor, BaseConnection, BaseHook
from connections.constants import POSTGRES__WRITE__DATA_PROD_ECOVIS,\
      POSTGRES__WRITE__STAGING


class PostgresCursor(BaseCursor, cursor):
    CURSOR_TYPES = {
        int: 'INT8',
        float: 'FLOAT8',
        datetime.datetime: 'TIMESTAMP',
        str: 'VARCHAR(1000)',
    }

    ESCAPE_CHARACTER = '"'

    @classmethod
    def get_type_info(cls, item: int):
        if item == 20:
            return 'INT'
        if item == 1043:
            return 'VAR_STRING'
        if item == 1114:
            return 'DATETIME'
        return item


class PostgresConnection(BaseConnection, connection):
    BASE_CURSOR = PostgresCursor

    @classmethod
    def from_connection(cls, connection):
        return connection

class DummyAirflowConnection(Connection):
    @property
    def extra_dejson(self) -> dict:
        extras = super().extra_dejson
        extras['connection_factory'] = PostgresConnection
        extras['cursor_factory'] = PostgresConnection.BASE_CURSOR
        return extras

class BasePostgresHook(BaseHook, PostgresHook):
    BASE_CONNECTION = PostgresConnection

    @classmethod
    def get_connection(cls, conn_id: str):
        conn = super().get_connection(conn_id)
        conn.__class__ = DummyAirflowConnection
        return conn


# Define specific hooks for each connection ID
class PostgresDataProdHook(BasePostgresHook):
    def __init__(self, **kwargs):
        super(PostgresDataProdHook, self).__init__(
            postgres_conn_id=POSTGRES__WRITE__DATA_PROD_ECOVIS, **kwargs)

class PostgresStagingHook(BasePostgresHook):
    def __init__(self, **kwargs):
        super(PostgresStagingHook, self).__init__(
            postgres_conn_id=POSTGRES__WRITE__STAGING, **kwargs)
