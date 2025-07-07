import logging
import re
from typing import Dict, List, Type, Tuple, Any, Sequence, Iterable

from sqlglot import parse_one
from sqlglot.expressions import Table, Column
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope

from exceptions import *
from string_utils import to_singular, to_camel_case, to_plural

logger = logging.getLogger('UniQuery')


class UniQuerySessionBase:
    models: Dict[str, Type['UniQueryModelBase']] = {}

    @property
    def PARAMETER_PLACE_HOLDER(self):
        raise NotImplementedError

    @property
    def AUTOINCREMENT_TYPE(self):
        raise NotImplementedError

    def __init__(self, db_config, log_sql=False):
        self.connection = None
        self.db_config = db_config
        self.log_sql = log_sql

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, exc_type, value, traceback):
        if self.connection:
            self.connection.close()

    def transaction(self, succeed_exceptions=None):
        raise NotImplementedError

    def placeholders_for_sqlglot(self, cmd):
        return cmd


class TransactionBase:
    def __init__(self, session: UniQuerySessionBase, succeed_exceptions=None):
        self.session = session
        self._cursor = None
        self._savepoint_name = None
        self._succeed_exceptions = tuple(succeed_exceptions or [])
        self._abort = False
        self.lastrowid = None

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, exc_type, value, traceback):
        raise NotImplementedError

    def create_record(self, uniquery_model: Type['UniQueryModelBase'], **kwargs: Any):
        """
        Create a new record of the specified model with the given keyword arguments.

        :param uniquery_model: The type of the record to create.
        :type uniquery_model: Type['UniQueryModel']
        :param kwargs: Additional arguments to initialize the record with.
        :type kwargs: dict
        :return: An instance of model created with the newly created record.
        :rtype: 'UniQueryModel'
        :raises UniQueryModelNotFoundError: If the specified model is not found in the models.

        The ``UniQueryModel`` object is created, but not saved to the database.

        If the primary key is autoincrement, then the id is ``None``, and it will
        get the correct value after it is saved to the database with ``.save()``
        and the id is generated.

        If the primary key is not autoincrement, then the id must be provided with
        the arguments.
        """
        for table in self.session.models.values():
            if table is uniquery_model:
                break
        else:
            raise UniQueryModelNotFoundError

        tables = self._get_tables_in_query(f"select * from '{table.Meta.table_name}'")
        table = tables[table.Meta.table_name]

        data = [None] * len(table.column_indexes)
        for index, name in table.column_indexes.items():
            data[index] = kwargs.get(name, None)

        instance = table.model_class(table, data)

        for k, v in kwargs.items():
            setattr(instance, k, v)

        return instance

    def delete_record(self, uniquery_model: Type['UniQueryModelBase'], primary_key_value: Any):
        """
        Delete a record of the specified type given the primary key value.

        :param uniquery_model: The type of the record to delete.
        :type uniquery_model: Type['UniQueryModel']
        :param primary_key_value: Primary key value.
        :type primary_key_value: Any
        :raises UniQueryModelNotFoundError: If the specified model is not found in the models.
        """
        for table in self.session.models.values():
            if table is uniquery_model:
                break
        else:
            raise UniQueryModelNotFoundError

        self.execute(f'DELETE FROM {table.Meta.table_name} WHERE {table.Meta.primary_key} = {self.session.PARAMETER_PLACE_HOLDER}',
                     (primary_key_value,))

    def abort(self):
        self._abort = True

    def commit(self):
        self.execute('COMMIT')

    def _get_columns_in_query(self, cmd, db_config):
        columns = []
        cmd2 = self.session.placeholders_for_sqlglot(cmd)
        ast = parse_one(cmd2, dialect=db_config['dialect'])
        ast = qualify(ast, schema=db_config['schema'])
        root = build_scope(ast)
        for expression in ast.expressions:
            if isinstance(expression.this, Column):
                column_name = expression.this.name
                column_alias = expression.this.alias
                source = root.sources[expression.this.table]
                if isinstance(source, Table):
                    table_name = source.name
                    table_alias = source.alias if table_name != source.alias else None
                else:
                    table_name = None
                    table_alias = None
            else:
                column_name = expression.alias
                column_alias = None
                table_name = None
                table_alias = None
            is_primary_key = table_name and db_config['primary_key_columns'][table_name] == column_name
            columns.append((table_name, table_alias, column_name, column_alias, is_primary_key))
        return columns

    def _get_tables_in_query(self, sql_cmd, models: List['UniQueryModelBase'] = None):
        """
        Create an UniQueryTable for each table involved in the query.

        If ``models`` is provided, then only those tables are returned (useful for example with CTE or other fancy queries).
        If ``models`` is provided, the column names selected in the query must match the provided table column names.
        """
        if models:
            columns = []
            for model in models:
                columns.extend(self._get_columns_in_query(f'SELECT * FROM {model.Meta.table_name}', self.session.db_config))
        else:
            columns = self._get_columns_in_query(sql_cmd, self.session.db_config)

        tables = {}
        for column in columns:
            table_name = column[0]
            table_alias = column[1]
            if table_name and table_name not in tables:
                tables[table_name] = UniQueryTableBase(table_name, table_alias, columns, self)

        return tables

    def execute(self, sql_cmd: str, parameters=None, get_dicts=False):
        """If ``get_dicts``, then return a list of dicts, otherwise a list of ``Row`` object created by the cursor object."""
        self._log_sql(sql_cmd, parameters)
        self._cursor.execute(sql_cmd, parameters or [])
        rows = self._cursor.fetchall() if self._cursor.description else []

        self.store_lastrowid(rows)

        if not get_dicts:
            return rows

        columns = self._get_columns_in_query(sql_cmd, self.session.db_config) if get_dicts else None
        return [{f'{(table_alias or table_name + ".") if (table_alias or table_name) else ""}{column_alias or column_name}': row[i]
                 for i, (table_name, table_alias, column_name, column_alias, is_primary_key)
                 in enumerate(columns)}
                for row in rows]

    def insert_many(self, table_name: str, column_names: Sequence[str], rows: Sequence[Tuple]):
        cmd = self.session.models[to_camel_case(to_singular(table_name))]._insert_many(table_name, column_names)
        self._log_sql(cmd, rows)
        self._cursor.executemany(cmd, rows)
        rows2 = self._cursor.fetchall() if self._cursor.description else []
        return rows2

    def delete_many(self, objects: Iterable['UniQueryModelBase']):
        table_names: Dict[str, List[UniQueryModelBase]] = {}
        for o in objects:
            table_name = o._table.table_name
            if table_name not in table_names:
                table_names[table_name] = []
            table_names[table_name].append(o)

        primary_key_columns = self.session.db_config['primary_key_columns']
        for table_name, objects in table_names.items():
            cmd = f'DELETE FROM {table_name} WHERE {primary_key_columns[table_name]} IN ({", ".join("?" * len(objects))})'
            self.execute(cmd, [o._id for o in objects])

    def query(self, query_result, sql_cmd, parameters=None, models: List[Type['UniQueryModelBase']] = None):
        tables = self._get_tables_in_query(sql_cmd, models)

        # execute the query
        rows = self.execute(sql_cmd, parameters)

        if models and rows:
            n_columns_in_query = sum(len(t.column_indexes) for t in tables.values())
            if len(rows[0]) != n_columns_in_query:
                raise WrongNumberOfColumnsInQuery(
                    f'The total number of columns in the provided list of models ({n_columns_in_query}) does not match the number of columns in the query result ({len(rows[0])})')

        # populate the tables with all the rows used by each table, creating instances without data, only the id
        for row in rows:
            for table in tables.values():
                table._add_row(row)

        # set the other field values on each row
        for table in tables.values():
            table._set_field_values()

        # create the one-to-many and many-to-one relations
        for table in tables.values():
            table._add_relation_lists()

        for table_one in tables.values():
            for table_many in tables.values():
                table_one._add_relations(table_many)

        # create the many-to-many relations
        for table_1 in tables.values():
            for table_2 in tables.values():
                table_1._add_relations_many_to_many(table_2)

        # add the final result to the QueryResult
        n_used_rows = 0
        for table in tables.values():
            setattr(query_result, table.model_class.Meta.plural, list(table.instances.values()))
            setattr(query_result, table.model_class.Meta.plural + '_dict', table.instances)
            n_used_rows += len(table.instances)

        assert n_used_rows >= len(rows), f'The query returned {len(rows)} rows, but only {n_used_rows} rows were used. Perhaps the query returned many duplicated rows?'

        # perform optional initialization by executing post_init on every UniQueryModel instance
        for table in tables.values():
            table._run_post_init()

    def _log_sql(self, cmd, parameters=None):
        if not self.session.log_sql:
            return

        # Remove lines with comments (not comments at the end of lines)
        cmd = re.sub(r'^\s*--.*?$', ' ', cmd, flags=re.MULTILINE)

        # Remove multiple spaces (even inside strings, which may be not desired)
        cmd = re.sub(r'\s+', ' ', cmd)

        if parameters:
            try:
                if isinstance(parameters, dict):  # Process dictionary-like parameters first
                    def replace_named_placeholder(match):
                        param_name = match.group(1)
                        if param_name not in parameters:
                            raise ValueError(f"Missing parameter: {param_name}")
                        value = parameters[param_name]

                        if isinstance(value, str):
                            return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"
                        elif value is None:
                            return "NULL"
                        else:
                            return str(value)

                    # Replace named placeholders in the format :parameter_name
                    formatted_cmd = re.sub(r':(\w+)', replace_named_placeholder, cmd)

                elif hasattr(parameters, '__iter__') and not isinstance(parameters, str):  # Handle list or iterable parameters
                    def replace_placeholder(match):
                        value = next(parameters_iter)

                        if isinstance(value, str):
                            return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"
                        elif value is None:
                            return "NULL"
                        else:
                            return str(value)

                    parameters_iter = iter(parameters)
                    formatted_cmd = re.sub(re.escape(self.session.PARAMETER_PLACE_HOLDER), replace_placeholder, cmd)

                    try:
                        next(parameters_iter)
                        raise ValueError('Iterator not fully consumed')
                    except StopIteration:
                        pass

                else:  # Raise an error if parameters is neither a dictionary nor iterable
                    raise TypeError("parameters must be a dictionary or an iterable (e.g., a list)")
            except TypeError:
                raise
            except:
                # something went wrong, perhaps an insert_many with the number of placeholder
                # not matching the number of values, so we fall back to unformatted output
                formatted_cmd = f'{cmd}, {parameters}'

            print(formatted_cmd)
        else:
            print(cmd)

    def store_lastrowid(self, rows):
        raise NotImplementedError


class UniQueryTableBase:
    def __init__(self, table_name, table_alias, columns, transaction: TransactionBase):
        self.table_name = table_name
        self.table_alias = table_alias
        self.transaction = transaction
        self.model_class: Type[UniQueryModelBase] = transaction.session.models[to_camel_case(to_singular(table_name))]
        self.instances: Dict[any, UniQueryModelBase] = {}
        self.name = self.model_class.Meta.singular

        self.column_indexes = {}
        primary_key_column = transaction.session.db_config['primary_key_columns'][self.table_name]
        self.primary_key_index = -1
        for i, (t_name, t_alias, column_name, column_alias, is_primary_key) in enumerate(columns):
            if t_name == table_name or t_name is None:
                self.column_indexes[i] = column_name
                if column_name == primary_key_column:
                    self.primary_key_index = i
        if self.primary_key_index == -1:
            raise MissingPrimaryKey(f'Column "{primary_key_column}" must be included in queries involving table "{table_name}"')

    def __getitem__(self, primary_key):
        return self.instances[primary_key]

    def get(self, primary_key, default=None):
        return self.instances.get(primary_key, default)

    def _add_row(self, data: tuple):
        model_instance = self.model_class(self, data)

        if model_instance._id is None:
            return  # this row has no id for this table, perhaps a LEFT JOIN query with no rows in this table

        if model_instance._id in self.instances:
            return  # this row has already been loaded (perhaps it appears multiple time in a join query)

        self.instances[model_instance._id] = model_instance

    def _set_field_values(self):
        for isntance in self.instances.values():
            isntance._set_field_values()

    def _run_post_init(self):
        if not hasattr(self.model_class, 'post_init'):
            return

        for instance in self.instances.values():
            # noinspection PyUnresolvedReferences
            instance.post_init()

    def _add_relation_lists(self):
        """Add empty lists for relation_many"""
        for relations_in_table in self.model_class.Meta.relations_many.values():
            for relation in relations_in_table:
                for instance in self.instances.values():
                    setattr(instance, relation.attribute_name, [])

        for relations in self.model_class.Meta.relations_many_many.values():
            for relation in relations:
                for instance in self.instances.values():
                    setattr(instance, relation.attribute_name, [])

    def _add_relations(self, table_many: 'UniQueryTableBase'):
        """Add relation_one to this table and relation_many to table_many"""
        relations_one: List[RelationOne] = self.model_class.Meta.relations_one.get(table_many.table_name)
        if not relations_one:
            return

        for relation_one in relations_one:
            for this_instance in self.instances.values():
                try:
                    id = getattr(this_instance, relation_one.from_column)
                except AttributeError:
                    raise MissingPrimaryKey(f'Column "{relation_one.attribute_name}" must be included in queries involving table "{self.table_name}"')

                table_many_instance = table_many.get(id, None)
                setattr(this_instance, relation_one.attribute_name, table_many_instance)

                if table_many_instance:
                    many_selves = getattr(table_many_instance, relation_one.other_attribute_name)
                    many_selves.append(this_instance)

    def _add_relations_many_to_many(self, other_table: 'UniQueryTableBase'):
        """Add direct relation between table one and table two, so the middle table can be skipped"""
        relations_many_many = self.model_class.Meta.relations_many_many.get(other_table.model_class.Meta.table_name, [])
        for relation in relations_many_many:
            if not relation:
                continue

            for this_instance in self.instances.values():
                other_instances = []
                relation_instances = getattr(this_instance, to_plural(relation.link_table), None)
                if not relation_instances:
                    continue  # relation field not populated yet because its table is not included in the query
                for relation_instance in relation_instances:
                    other_instances.append(getattr(relation_instance, other_table.name))
                setattr(this_instance, relation.attribute_name, other_instances)

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.name}>'


class UniQueryModelBase:
    MAX_LEN_REPR_ITEM = 20

    def __init__(self, table: UniQueryTableBase, values: Sequence):
        self._table = table
        self._values = values
        self._id = values[table.primary_key_index]

    class Meta:
        table_name: str
        primary_key: str
        singular: str
        plural: str
        camel: str
        columns: List[str]
        relations_one: Dict[str, List['RelationOne']]
        relations_many: Dict[str, List['RelationMany']]
        relations_many_many: Dict[str, List['RelationManyMany']]

    def _set_field_values(self):
        for index, name in self._table.column_indexes.items():
            setattr(self, name, self._values[index])

    def __repr__(self):
        # first show columns containing name, then containing desc and primary key, then other columns
        priority_1 = []
        priority_2 = []
        priority_3 = []
        for k in vars(self):
            if 'name' in k:
                priority_1.append(k)
            elif 'desc' in k or self.Meta.primary_key == k:
                priority_2.append(k)
            else:
                priority_3.append(k)

        values = []
        for attr_name in priority_1 + priority_2 + priority_3:
            if hasattr(self, attr_name):
                attr_value = getattr(self, attr_name)
                if (attr_value
                        and not attr_name.startswith('_')
                        and type(attr_value) is not dict
                        and type(attr_value) is not list
                        and not isinstance(attr_value, UniQueryModelBase)):
                    formatted_value = str(attr_value)
                    if len(values) < 6:
                        if len(formatted_value) > self.MAX_LEN_REPR_ITEM:
                            formatted_value = formatted_value[:self.MAX_LEN_REPR_ITEM - 2] + '...'
                        values.append(f'{attr_name}={formatted_value}')
                    else:
                        values.append(f'[...]')
                        break

        values = ', '.join(values)
        return f'<{self.__class__.__name__}: {values}>'

    def _insert_values(self, columns, values):
        raise NotImplementedError

    def _upsert_values(self, columns, values, primary_key_value):
        raise NotImplementedError

    @classmethod
    def _select_by_primary_key(cls):
        raise NotImplementedError

    @classmethod
    def _insert_many(cls, table_name, column_names):
        raise NotImplementedError

    def save(self):
        primary_key_name = self._table.model_class.Meta.primary_key
        primary_key_value = getattr(self, primary_key_name, None)

        columns = []
        values = []
        for column_name in self._table.model_class.Meta.columns:
            if hasattr(self, column_name):
                value = getattr(self, column_name)
                if column_name == primary_key_name and primary_key_value is None:
                    if self._table.transaction.session.db_config['schema'][self._table.table_name][column_name] != self._table.transaction.session.AUTOINCREMENT_TYPE:
                        raise MissingId(f'Impossible to create an instance of type "{self._table.table_name}" without id because the id is not autoincrement.')
                    continue
                columns.append(column_name)
                values.append(value)

        if primary_key_value is None:
            cmd, parameters = self._insert_values(columns, values)
        else:
            cmd, parameters = self._upsert_values(columns, values, primary_key_value)

        self._table.transaction.execute(cmd, parameters)

        if primary_key_value is None:
            setattr(self, primary_key_name, self._table.transaction.lastrowid)

    @classmethod
    def get_by_pk_value(cls, transaction: TransactionBase, value) -> 'UniQueryModelBase':
        class QueryResult:
            rows: List

        cmd = cls._select_by_primary_key()
        query_result = QueryResult()
        transaction.query(q := query_result, cmd, (value,))

        rows = getattr(q, cls.Meta.plural, None)
        return rows[0] if rows else None


class Relation:
    attribute_name: str
    in_class: str

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.attribute_name} {self.in_class}>'


class RelationOne(Relation):
    def __init__(self, attribute_name, other_attribute_name, in_class, from_column, to_table, to_column):
        self.attribute_name = attribute_name
        self.other_attribute_name = other_attribute_name
        self.in_class = in_class
        self.from_column = from_column
        self.to_table = to_table
        self.to_column = to_column


class RelationMany(Relation):
    def __init__(self, attribute_name, in_class, from_column, to_table):
        self.attribute_name = attribute_name
        self.in_class = in_class
        self.from_column = from_column
        self.to_table = to_table


class RelationManyMany(Relation):
    def __init__(self, attribute_name, in_class, from_column, to_table, to_column, link_table):
        self.attribute_name = attribute_name
        self.in_class = in_class
        self.from_column = from_column
        self.to_table = to_table
        self.to_column = to_column
        self.link_table = link_table
