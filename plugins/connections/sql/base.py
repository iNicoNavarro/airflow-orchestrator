import logging
import datetime
from typing import Iterator, TypeVar

from connections.constants import DEFAULT_INSERT_BATCH_SIZE, MAX_QUERY_LOG_CHARS


BaseCursorType = TypeVar('BaseCursorType', bound='BaseCursor')
BaseConnectionType = TypeVar('BaseConnectionType', bound='BaseConnection')


class BaseCursor:
    CURSOR_TYPES: dict = {}

    CREATE_TABLE_QUERY_PATTERN: str = """
    CREATE TABLE {options} {table}(
        {definitions}
    );
    """
    CREATE_TABLE_OPTIONS: str = 'IF NOT EXISTS'

    INSERT_INTO_QUERY_PATTERN: str = """
    INSERT INTO {table} {columns}
    VALUES 
        {values};
    """

    DROP_QUERY_PATTERN: str = """
    DROP TABLE {options} {table};
    """

    DROP_TABLE_OPTIONS: str = 'IF EXISTS'

    ESCAPE_CHARACTER: str = ''

    DBAPI_PYTHON_TYPES: dict = {
        'DATETIME': datetime.datetime,
        'VAR_STRING': str,
        'LONGLONG': int,
        'BLOB': str,
        'NULL': str,
        'TINY': int,
        'INT': int,
        'NEWDECIMAL': float,
        'FLOAT': float
    }

    def __init__(self, *args, **kwargs):
        super(BaseCursor, self).__init__(*args, **kwargs)

    def get_types(self) -> list:
        return [
            self.DBAPI_PYTHON_TYPES.get(self.get_type_info(item[1]), item[1]) for item in self.description
        ]

    @classmethod
    def get_type_info(cls, item: int):
        raise NotImplementedError(f"Method get_types must be implemented for '{cls.__name__}'")

    def get_columns(self) -> list:
        return [
            item[0] for item in self.description
        ]

    def get_metadata(self) -> dict:
        return dict(
            types=self.get_types(),
            columns=self.get_columns()
        )

    def read(self, n_rows: int = None) -> dict:
        if n_rows is None:
            data = self.fetchall()
        else:
            data = self.fetchmany(n_rows)

        data = list(data)

        logging.info(f"Retrieved {len(data)} rows in a batch of {n_rows}")

        return dict(
            self.get_metadata(),
            **{
                'data': data
            }
        )

    def read_batches(self, batch_size: int = DEFAULT_INSERT_BATCH_SIZE) -> Iterator[dict]:
        while True:
            results = self.read(batch_size)
            if results['data']:
                yield results
            else:
                break

    def read_df(self):
        import numpy as np
        import pandas as pd

        return pd.DataFrame(
            self.fetchall(),
            columns=self.get_columns()
        ).fillna(
            np.nan
        ).replace(
            [np.nan],
            [None]
        )

    @classmethod
    def convert_types(cls, types: list) -> list:
        new_types = []
        for item in types:
            try:
                new_types.append(cls.CURSOR_TYPES[item])
            except KeyError:
                import numpy as np
                import pandas as pd

                if item == np.dtype('datetime64[ns]'):
                    new_types.append(
                        'TIMESTAMP'
                    )

                elif 'datetime64' in str(item):
                    new_types.append(
                        'TIMESTAMP'
                    )

                elif item == np.dtype('O'):
                    new_types.append(
                        'VARCHAR(30000)'
                    )

                elif item == np.dtype('bool'):
                    new_types.append(
                        'BOOL'
                    )

                elif item == np.int64:
                    new_types.append(
                        'INT'
                    )

                elif item == np.float64:
                    new_types.append(
                        'FLOAT'
                    )

                elif item == pd.Int64Dtype():
                    new_types.append(
                        'INT'
                    )

                else:
                    raise KeyError(f"The following type has no cursor match: '{item}'")

        return new_types

    @classmethod
    def escape_variable(cls, variable: str) -> str:
        return f'{cls.ESCAPE_CHARACTER}{variable}{cls.ESCAPE_CHARACTER}'

    @classmethod
    def escape_table(cls, name: str) -> str:
        return cls.escape_variable(name).replace('.', f'{cls.ESCAPE_CHARACTER}.{cls.ESCAPE_CHARACTER}')

    @classmethod
    def build_create_query(
            cls,
            columns: list,
            types: list,
            table: str = '{table}',
            options: str = CREATE_TABLE_OPTIONS,
            query_pattern: str = CREATE_TABLE_QUERY_PATTERN
    ) -> str:
        new_types = cls.convert_types(types)

        type_pairs = [
            f'{cls.escape_variable(col)} {typ}'
            for col, typ in zip(columns, new_types)
        ]

        return query_pattern.format(
            options=options,
            table=cls.escape_table(table),
            definitions=",\n".join(type_pairs)
        )

    @classmethod
    def escape_value(cls, value) -> str:
        try:
            if (value is None) or (value != value):
                return 'NULL'
        except TypeError:
            import pandas as pd

            if pd.isna(value):
                return 'NULL'

        if isinstance(value, bool):
            return str(value)
        else:
            value = str(value).replace("'", "''").replace('\\', '')  # TODO: technical debt how to better handle?
            return f"'{value}'"

    @classmethod
    def build_insert_query(
            cls,
            data: list,
            columns: list,
            table: str = '{table}',
            query_pattern: str = INSERT_INTO_QUERY_PATTERN
    ) -> str:
        if columns:
            columns = '(' + ','.join(
                map(cls.escape_variable, columns)
            ) + ')'
        else:
            columns = ''

        values = [
            '(' + ','.join(
                map(cls.escape_value, values)
            ) + ')'
            for values in data
        ]

        return query_pattern.format(
            table=cls.escape_table(table),
            columns=columns,
            values=',\n'.join(
                values
            )
        )

    def execute_create(self, columns: list, types: list, **kwargs):
        self.execute(
            self.build_create_query(
                columns=columns,
                types=types,
                **kwargs
            )
        )

    def execute_bulk_insert(self, data: list, columns: list, batch_size: int = DEFAULT_INSERT_BATCH_SIZE, **kwargs):
        from math import ceil

        total_rows = len(data)
        n_batches = int(ceil(total_rows / batch_size))

        logging.info(f"Preparing to write {total_rows} in {n_batches} batches")

        for i in range(n_batches):
            self.execute(
                self.build_insert_query(
                    data=data[i * batch_size:(i + 1) * batch_size],
                    columns=columns,
                    **kwargs
                )
            )

    @classmethod
    def build_drop_table(cls, table, options: str = DROP_TABLE_OPTIONS) -> str:
        return cls.DROP_QUERY_PATTERN.format(
            table=cls.escape_table(table),
            options=options
        )

    def execute_drop_table(self, table, **kwargs):
        logging.info(f"Executing drop on table '{table}'")

        return self.execute(
            self.build_drop_table(
                table=table,
                **kwargs
            )
        )

    @classmethod
    def from_cursor(cls, cursor) -> BaseCursorType:
        cursor.__class__ = cls
        return cursor

    def execute(self, *args, **kwargs):
        logging.info(
            args[0][:MAX_QUERY_LOG_CHARS]
        )

        try:
            return super(BaseCursor, self).execute(*args, **kwargs)
        except Exception as e:
            logging.info(args[0])
            raise e


class BaseConnection:
    BASE_CURSOR: BaseCursor = BaseCursor

    def __init__(self, *args, **kwargs):
        super(BaseConnection, self).__init__(*args, **kwargs)

    def cursor(self) -> BaseCursor:
        return self.BASE_CURSOR.from_cursor(
            super(BaseConnection, self).cursor()
        )

    @classmethod
    def from_connection(cls, connection) -> BaseConnectionType:
        connection.__class__ = cls
        return connection


class BaseHook:
    BASE_CONNECTION: BaseConnection = BaseConnection

    def __init__(self, *args, **kwargs):
        super(BaseHook, self).__init__(*args, **kwargs)

    def get_conn(self, *args, **kwargs) -> BaseConnection:
        return self.BASE_CONNECTION.from_connection(
            super(BaseHook, self).get_conn(
                *args, **kwargs
            )
        )
