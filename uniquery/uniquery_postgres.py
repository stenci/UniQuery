import re

import psycopg2, psycopg2.extras
from typing import Dict, List, Type, Tuple, Any, Sequence, Iterable

from .uniquery import logger, UniQuerySessionBase, TransactionBase, UniQueryTableBase, UniQueryModelBase
from .exceptions import *
from .utils import ModelGeneratorBase


class ModelGenerator(ModelGeneratorBase):
    @classmethod
    def connect(cls, connection_string):
        connection = psycopg2.connect(connection_string)
        return connection

    @classmethod
    def get_cursor(cls, connection):
        return connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    @classmethod
    def get_table_list(cls, cur):
        cur.execute("""SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name""")
        return [row['table_name'] for row in cur.fetchall()]

    @classmethod
    def get_column_info(cls, cur, table_name):
        # postgres doesn't have a pk column like sqlite does, so we'll just
        # assume that the not nullable columns are primary keys.
        # Checking for is_nullable works with simple tables like the ones generated
        # in the test suite, but it's likely to require adjustment for more complex tables.
        cur.execute(f"""SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position""")
        return [{'name': col['column_name'],
                 'type': col['data_type'],
                 'pk':   col['is_nullable'] == 'NO'}
                for col in cur.fetchall()]

    @classmethod
    def get_foreign_keys(cls, cur, table_name):
        cur.execute(f"""SELECT kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
                     JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name
                     JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY'
              AND tc.table_name = '{table_name}'""")
        return cur.fetchall()

    @classmethod
    def python_column_type(cls, column_type):
        column_type = column_type.upper()

        if column_type in ['INTEGER', 'SMALLINT']: return 'int'
        if column_type == 'TIME': return 'int'

        if column_type in ['REAL', 'DOUBLE PRECISION']: return 'float'

        if column_type.startswith('VARCHAR') or column_type in ('CHARACTER VARYING', 'TEXT'): return 'str'

        if column_type in ['TIMESTAMP', 'TIMESTAMPTZ']: cls.require_import_datetime = True; return 'datetime'

        if column_type == 'BYTEA': return 'any'

        return f'Unexpected column type (see the function "python_column_type"): {column_type}'

    @classmethod
    def generate_models(cls, connection_string, py_file_name, tables=None, rename_attributes=None):
        super().generate_models_2(connection_string, py_file_name, 'postgres', tables, rename_attributes)


class UniQuerySession(UniQuerySessionBase):
    models: Dict[str, Type['UniQueryModel']] = {}
    PARAMETER_PLACE_HOLDER = '%s'
    AUTOINCREMENT_TYPE = 'integer'

    def __init__(self, db_config, log_sql=False):
        super().__init__(db_config, log_sql)
        self.connection: psycopg2.connection = None  # type: ignore

    def __enter__(self):
        self.connection = ModelGenerator.connect(self.db_config['connection_string'])
        return self

    def transaction(self, succeed_exceptions=None):
        return Transaction(self, succeed_exceptions)

    def placeholders_for_sqlglot(self, cmd):
        return re.sub(r"(?<!')%s(?!')", "?", cmd)


class Transaction(TransactionBase):
    def __init__(self, session: UniQuerySession, succeed_exceptions=None):
        super().__init__(session, succeed_exceptions)
        self._cursor: psycopg2.extensions.cursor

    def __enter__(self):
        self._cursor = self.session.connection.cursor()
        if self.session.connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INTRANS:
            self._savepoint_name = f'sp_{id(self)}'
            self.execute(f'SAVEPOINT {self._savepoint_name}')
        else:
            self.execute('BEGIN')
        return self

    def __exit__(self, exc_type, value, traceback):
        try:
            aborting = self._abort or (exc_type is not None and exc_type not in self._succeed_exceptions)
            if aborting:
                if self._savepoint_name:
                    self.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
                else:
                    self.execute('ROLLBACK')
            else:
                if self._savepoint_name:
                    self.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
                else:
                    self.execute('COMMIT')
        finally:
            if self._cursor:
                self._cursor.close()

    def commit(self):
        self.execute('COMMIT')

        cmd = f'BEGIN'
        self.execute(cmd)

    def store_lastrowid(self, rows):
        if rows and len(rows[0]) == 1:
            self.lastrowid = rows[0][0]


class UniQueryTable(UniQueryTableBase):
    pass


class UniQueryModel(UniQueryModelBase):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # register this class as a model in the session class
        UniQuerySession.models[cls.__name__] = cls

    def _insert_values(self, columns, values):
        values_place_holders = ', '.join([UniQuerySession.PARAMETER_PLACE_HOLDER] * len(columns))
        columns_list = '", "'.join(columns)
        return f"""INSERT INTO "{self._table.table_name}" ("{columns_list}")
                VALUES ({values_place_holders})
                RETURNING "{self._table.table_name}"."{self._table.model_class.Meta.primary_key}" """, values

    def _upsert_values(self, columns, values, primary_key_value):
        values_place_holders = ', '.join([UniQuerySession.PARAMETER_PLACE_HOLDER] * len(columns))
        primary_key_name = self._table.model_class.Meta.primary_key
        assignments = ', '.join(f'"{column_name}" = excluded."{column_name}"' for column_name in columns)
        columns_list = '", "'.join(columns)
        return f"""INSERT INTO "{self._table.table_name}" ("{columns_list}")
                VALUES ({values_place_holders})
                ON CONFLICT ("{primary_key_name}")
                DO UPDATE SET {assignments}
                WHERE "{self._table.table_name}"."{primary_key_name}" = {UniQuerySession.PARAMETER_PLACE_HOLDER}
                RETURNING "{self._table.table_name}"."{primary_key_name}" """, values + [primary_key_value]

    @classmethod
    def _select_by_primary_key(cls):
        return f'SELECT * FROM "{cls.Meta.table_name}" WHERE "{cls.Meta.primary_key}" = {UniQuerySession.PARAMETER_PLACE_HOLDER}'

    @classmethod
    def _insert_many(cls, table_name, column_names):
        return f"""INSERT INTO {table_name} 
                ({', '.join(f'"{c}"' for c in column_names)}) 
                VALUES ({', '.join(['%s'] * len(column_names))})"""
