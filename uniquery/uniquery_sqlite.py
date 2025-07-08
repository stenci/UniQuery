import sqlite3
from typing import Dict, List, Type, Tuple, Any, Sequence, Iterable

from .uniquery import logger, UniQuerySessionBase, TransactionBase, UniQueryTableBase, UniQueryModelBase
from .exceptions import *
from .utils import ModelGeneratorBase


class TransactionMode:
    NoTransaction = 'NO_TRANSACTION'
    Immediate = 'IMMEDIATE'
    Deferred = 'DEFERRED'
    Exclusive = 'EXCLUSIVE'


class ModelGenerator(ModelGeneratorBase):
    @classmethod
    def connect(cls, file_name):
        connection = sqlite3.connect(file_name)
        connection.row_factory = sqlite3.Row
        return connection

    @classmethod
    def get_cursor(cls, connection):
        return connection.cursor()

    @classmethod
    def get_table_list(cls, cur):
        cur.execute("""SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'""")
        return [row[0] for row in cur.fetchall()]

    @classmethod
    def get_column_info(cls, cur, table_name):
        cur.execute(f"PRAGMA table_info({table_name})")
        return [{'name': col['name'],
                 'type': col['type'],
                 'pk':   col['pk']}
                for col in cur.fetchall()]

    @classmethod
    def get_foreign_keys(cls, cur, table_name):
        cur.execute(f"PRAGMA foreign_key_list({table_name})")
        return [{'foreign_table_name':  fk['table'],
                 'column_name':         fk['from'],
                 'foreign_column_name': fk['to']}
                for fk in cur.fetchall()]

    @classmethod
    def python_column_type(cls, column_type):
        if column_type == 'INTEGER': return 'int'
        if column_type == 'SMALLINT': return 'int'
        if column_type == 'TIME': return 'int'

        if column_type == 'REAL': return 'float'

        if column_type.startswith('VARCHAR'): return 'str'
        if column_type == 'TEXT': return 'str'

        if column_type == 'DATETIME': cls.require_import_datetime = True; return 'datetime'

        if column_type == 'BLOB': return 'any'

        return f'Unexpected column type (see the function "python_column_type"): {column_type}'

    @classmethod
    def additional_imports(cls):
        return ['TransactionMode']

    @classmethod
    def generate_models(cls, connection_string, py_file_name, tables=None, rename_attributes=None):
        super().generate_models_2(connection_string, py_file_name, 'sqlite', tables, rename_attributes)


class UniQuerySession(UniQuerySessionBase):
    models: Dict[str, Type['UniQueryModel']] = {}
    PARAMETER_PLACE_HOLDER = '?'
    AUTOINCREMENT_TYPE = 'INTEGER'

    def __init__(self, db_config, log_sql=False):
        super().__init__(db_config, log_sql)
        self.connection: sqlite3.Connection = None  # type: ignore

    def __enter__(self):
        self.connection = ModelGenerator.connect(self.db_config['connection_string'])
        return self

    def transaction(self, succeed_exceptions=None, transaction_mode: TransactionMode = TransactionMode.Deferred):
        return Transaction(self, succeed_exceptions, transaction_mode)


class Transaction(TransactionBase):
    def __init__(self, session: UniQuerySession, succeed_exceptions=None, transaction_mode: TransactionMode = TransactionMode.Deferred):
        super().__init__(session, succeed_exceptions)
        self._cursor: sqlite3.Cursor
        self._transaction_mode = transaction_mode

    def __enter__(self):
        self._cursor = self.session.connection.cursor()
        if self._transaction_mode != TransactionMode.NoTransaction:
            if self.session.connection.in_transaction:
                self._savepoint_name = f'sp_{id(self)}'
                self.execute(f'SAVEPOINT {self._savepoint_name}')
            else:
                self.execute(f'BEGIN {self._transaction_mode}')
        return self

    def __exit__(self, exc_type, value, traceback):
        try:
            if self._transaction_mode == TransactionMode.NoTransaction:
                return

            aborting = self._abort or (exc_type is not None and exc_type not in self._succeed_exceptions)
            if aborting:
                if self._savepoint_name:
                    self.execute(f"ROLLBACK TO {self._savepoint_name}")
                else:
                    self.execute('ROLLBACK')
            else:
                if self._savepoint_name:
                    self.execute(f"RELEASE {self._savepoint_name}")
                else:
                    self.execute('COMMIT')
        finally:
            if self._cursor:
                self._cursor.close()

    def commit(self):
        self.execute('COMMIT')

        cmd = f'BEGIN {self._transaction_mode}'
        self.execute(cmd)

    def store_lastrowid(self, rows):
        self.lastrowid = self._cursor.lastrowid


class UniQueryTable(UniQueryTableBase):
    pass


class UniQueryModel(UniQueryModelBase):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # register this class as a model in the session class
        UniQuerySession.models[cls.__name__] = cls

    def _insert_values(self, columns, values):
        values_place_holders = ', '.join([UniQuerySession.PARAMETER_PLACE_HOLDER] * len(columns))
        return f"""INSERT INTO {self._table.table_name}('{"', '".join(columns)}')
                VALUES({values_place_holders})""", values

    def _upsert_values(self, columns, values, primary_key_value):
        values_place_holders = ', '.join([UniQuerySession.PARAMETER_PLACE_HOLDER] * len(columns))
        primary_key_name = self._table.model_class.Meta.primary_key
        assignments = ', '.join(f'{column_name} = {UniQuerySession.PARAMETER_PLACE_HOLDER}' for column_name in columns)
        return f"""INSERT INTO {self._table.table_name}('{"', '".join(columns)}')
                    VALUES({values_place_holders})
                    ON CONFLICT({primary_key_name}) 
                    DO UPDATE SET {assignments}
                    WHERE {primary_key_name} = {UniQuerySession.PARAMETER_PLACE_HOLDER}""", values + values + [primary_key_value]

    @classmethod
    def _select_by_primary_key(cls):
        return f"SELECT * FROM {cls.Meta.table_name} WHERE {cls.Meta.primary_key} = {UniQuerySession.PARAMETER_PLACE_HOLDER}"

    @classmethod
    def _insert_many(cls, table_name, column_names):
        return f"""INSERT INTO {table_name} 
                ({', '.join(f"'{c}'" for c in column_names)})
                VALUES ({', '.join(['?'] * len(column_names))})"""
