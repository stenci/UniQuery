import json
import os
import re
from typing import List

from .exceptions import MultiplePrimaryKeys, RenamedAttributeNotFound
from .string_utils import to_singular, to_plural, to_camel_case


class ModelGeneratorBase:
    require_import_datetime = False

    @classmethod
    def connect(cls, file_name):
        raise NotImplementedError

    @classmethod
    def get_cursor(cls, connection):
        raise NotImplementedError

    @classmethod
    def get_table_list(cls, cur):
        raise NotImplementedError

    @classmethod
    def get_column_info(cls, cur, table_name):
        raise NotImplementedError

    @classmethod
    def get_foreign_keys(cls, cur, table_name):
        raise NotImplementedError

    @classmethod
    def python_column_type(cls, sqlite_column_type):
        raise NotImplementedError

    @classmethod
    def additional_imports(cls):
        return []

    @classmethod
    def get_db_full_schema(cls, connection_string, rename_attributes=None):
        rename_attributes = rename_attributes or {}

        with cls.connect(connection_string) as conn:
            cur = cls.get_cursor(conn)

            try:
                table_list = sorted(cls.get_table_list(cur), key=lambda name: to_singular(name))

                tables = {}
                for table_real_name in table_list:
                    column_descriptions = cls.get_column_info(cur, table_real_name)
                    singular = to_singular(table_real_name)  # in case the table names are plural
                    plural = to_plural(singular)
                    camel = to_camel_case(singular)
                    primary_keys = [col['name'] for col in column_descriptions if col['pk']]
                    if len(primary_keys) < 1:
                        raise MultiplePrimaryKeys(f'Table {table_real_name} has no primary keys.')
                    if len(primary_keys) > 1:
                        raise MultiplePrimaryKeys(f'Table {table_real_name} has multiple primary keys: {primary_keys}.')
                    tables[singular] = {
                        'table_name':          table_real_name,
                        'singular':            singular,
                        'plural':              plural,
                        'camel':               camel,
                        'primary_key':         primary_keys[0],
                        'columns':             {c['name']: {'name': c['name'], 'sql_type': c['type'], 'python_type': cls.python_column_type(c['type'])} for c in column_descriptions},
                        'relations_one':       {},
                        'relations_many':      {},
                        'relations_many_many': {},
                    }

                for table_many in tables.values():
                    foreign_keys = cls.get_foreign_keys(cur, table_many['table_name'])

                    # If the table only contains the primary key and 2 foreign keys, it is
                    # assumed to be a many-to-many relation table.
                    is_many_to_many_link_table = len(foreign_keys) == 2 and len(table_many['columns']) == 3

                    # Every foreign key generates one singular field name in the current table
                    # and one plural field name in the other table.
                    for foreign_key in foreign_keys:
                        table_many_fk_column_name = foreign_key['column_name']
                        table_one = tables[to_singular(foreign_key['foreign_table_name'])]
                        table_one_fk_column_name = foreign_key['foreign_column_name']

                        # Define all valid names for this attribute, from the shortest version to the longest.
                        # The longest is unique, the shorter ones may not be.
                        # Use this list later, when simplifying the attribute names to find the shortest unique one.
                        table_many_attribute_names = [
                            table_many_fk_column_name.replace(f"_{table_one['primary_key']}", ''),
                            table_many_fk_column_name,
                            table_one['singular'],
                            f"{table_one['singular']}__{table_many_fk_column_name}",
                            f"{table_one['singular']}__{table_many_fk_column_name}__{table_one_fk_column_name}",
                            f"{table_many['table_name']}__{table_many_fk_column_name}__{table_one['table_name']}__{table_one_fk_column_name}",
                        ]
                        table_one_attribute_names = [
                            table_many['plural'],
                            to_plural(table_many_fk_column_name),
                            ('links_to_' if is_many_to_many_link_table else '') + table_many['plural'],
                            f"{table_many['singular']}__{to_plural(table_many_fk_column_name)}",
                            f"{table_many['singular']}__{table_one_fk_column_name}",
                            f"{table_many['singular']}__{table_one_fk_column_name}__{table_many_fk_column_name}",
                            f"{table_one['table_name']}__{table_one_fk_column_name}__{table_many['table_name']}__{table_many_fk_column_name}",
                        ]

                        # Remove column names
                        table_many_attribute_names = [name for name in table_many_attribute_names if name not in table_many['columns']]
                        table_one_attribute_names = [name for name in table_one_attribute_names if name not in table_one['columns']]

                        # Remove duplicated (keeping the first occurrence)
                        table_many_attribute_names = [name for i, name in enumerate(table_many_attribute_names) if name not in table_many_attribute_names[:i]]
                        table_one_attribute_names = [name for i, name in enumerate(table_one_attribute_names) if name not in table_one_attribute_names[:i]]

                        # Add relation definition dictionaries to tables
                        if table_one['table_name'] not in table_many['relations_one']:
                            table_many['relations_one'][table_one['table_name']] = []

                        if table_many['table_name'] not in table_one['relations_many']:
                            table_one['relations_many'][table_many['table_name']] = []

                        # JOIN comment for 1:N and N:1
                        join_comment_many = f"1:N - JOIN {table_many['table_name']} ON {table_many['table_name']}.{table_many_fk_column_name} = {table_one['table_name']}.{table_one_fk_column_name}"
                        join_comment_one = f"N:1 - JOIN {table_one['table_name']} ON {table_many['table_name']}.{table_many_fk_column_name} = {table_one['table_name']}.{table_one_fk_column_name}"

                        relation_many = {
                            'attribute_names': table_one_attribute_names,
                            'type':            f"{table_many['camel']}",
                            'hint':            f"List['{table_many['camel']}']",
                            'to_table':        table_many['table_name'],
                            'from':            f"{foreign_key['column_name']}",
                            'join_comment':    join_comment_many,
                        }
                        table_one['relations_many'][table_many['table_name']].append(relation_many)

                        relation_one = {
                            'attribute_names': table_many_attribute_names,
                            'type':            f"'{table_one['camel']}'",
                            'from_column':     table_many_fk_column_name,
                            'to_table':        table_one['singular'],
                            'to_column':       foreign_key['foreign_column_name'],
                            'other_relation':  relation_many,
                            'join_comment':    join_comment_one,
                        }
                        table_many['relations_one'][table_one['table_name']].append(relation_one)

                    # Add direct links between two tables with a many-to-many link table.
                    if is_many_to_many_link_table:
                        table1 = tables[to_singular(foreign_keys[0]['foreign_table_name'])]
                        table2 = tables[to_singular(foreign_keys[1]['foreign_table_name'])]

                        for t1, fk1, t2, fk2 in [(table1, foreign_keys[0], table2, foreign_keys[1]), (table2, foreign_keys[1], table1, foreign_keys[0])]:
                            if t2['table_name'] not in t1['relations_many_many']:
                                t1['relations_many_many'][t2['table_name']] = []

                            table_name_plural = table_many['plural']
                            fk1_table_name_plural = to_plural(fk1['foreign_table_name'])
                            fk2_table_name_plural = to_plural(fk2['foreign_table_name'])
                            attribute_names = [
                                f"{fk2_table_name_plural}",
                                f"{table_name_plural}__{fk2_table_name_plural}",
                                f"{table_name_plural}__{to_plural(fk2['column_name'])}",
                                f"{table_name_plural}__{to_plural(fk1['column_name'])}__{to_plural(fk2['column_name'])}",
                                f"{t1['table_name']}__{fk1['column_name']}__{table_many['table_name']}__{t2['table_name']}__{fk2['column_name']}",
                            ]
                            # JOIN comment for N:M
                            join_comment_many_many = (
                                f"N:M - JOIN {table_many['table_name']} ON {t1['table_name']}.{fk1['foreign_column_name']} = {table_many['table_name']}.{fk1['column_name']} "
                                f"JOIN {t2['table_name']} ON {table_many['table_name']}.{fk2['column_name']} = {t2['table_name']}.{fk2['foreign_column_name']}"
                            )
                            t1['relations_many_many'][t2['table_name']].append({
                                'attribute_names': attribute_names,
                                'to_table':        t2['singular'],
                                'from':            fk1['column_name'],
                                'to':              fk2['column_name'],
                                'link_table':      table_many['table_name'],
                                'join_comment':    join_comment_many_many,
                            })
            finally:
                cur.close()

        # Shorten attribute names for relations when the whole name is not required, that is when
        # there is only one relation per table and specifying the column name is uselessly verbose
        renamed_attributes = set()
        for table_many in tables.values():
            all_attribute_names = [
                name
                for relation_set in ['relations_one', 'relations_many', 'relations_many_many']
                for table_relations in table_many[relation_set].values()
                for relation in table_relations
                for name in relation['attribute_names']
            ]
            for relation_set in ['relations_one', 'relations_many', 'relations_many_many']:
                for table_relations in table_many[relation_set].values():
                    for relation in table_relations:
                        longest_attribute_name = relation['attribute_names'][-1]
                        if longest_attribute_name in rename_attributes:
                            relation['attribute_name'] = rename_attributes[longest_attribute_name]
                            renamed_attributes.add(longest_attribute_name)
                        else:
                            for name in relation['attribute_names']:
                                if all_attribute_names.count(name) == 1 and name not in table_many['columns']:
                                    relation['attribute_name'] = name
                                    break
                            else:
                                assert False, 'Unable to find unique attribute names'

        not_renamed_attributes = set(rename_attributes.keys()) - renamed_attributes
        if len(not_renamed_attributes) == 1:
            attr = next(iter(not_renamed_attributes))
            raise RenamedAttributeNotFound(f'The attribute "{attr}" was specified for renaming but was not found.')
        elif not_renamed_attributes:
            quoted_attrs = ', '.join(f'"{attr}"' for attr in sorted(not_renamed_attributes))
            raise RenamedAttributeNotFound(f'The following attributes were specified for renaming but were not found: {quoted_attrs}')

        for table in tables.values():
            for relation_set in ['relations_one', 'relations_many', 'relations_many_many']:
                for table_relations in table[relation_set].values():
                    for relation in table_relations:
                        if 'other_relation' in relation:
                            relation['other_attribute_name'] = relation['other_relation']['attribute_name']

        return tables

    @classmethod
    def add_imports(cls, rows: List[str], manually_added_code, dialect):
        rows.append('# region manually added imports')
        if 'manually added imports' in manually_added_code:
            rows.extend(manually_added_code['manually added imports'])
            del manually_added_code['manually added imports']
        rows.append('# endregion')

        rows.append('from typing import Dict, Any, List, Tuple')
        rows.append('from uniquery.uniquery import RelationOne, RelationMany, RelationManyMany')
        if cls.additional_imports():
            rows.append(f'from uniquery.uniquery_{dialect} import UniQuerySession, UniQueryTable, UniQueryModel, Transaction, ' + ','.join(cls.additional_imports()))
        else:
            rows.append(f'from uniquery.uniquery_{dialect} import UniQuerySession, UniQueryTable, UniQueryModel, Transaction')
        if cls.require_import_datetime:
            rows.append('from datetime import datetime')
        rows.append(' ')

    @classmethod
    def add_model_definitions(cls, rows: List[str], tables, manually_added_code):
        for table in tables.values():
            table_camel = table['camel']
            rows.append(f'class {table_camel}(UniQueryModel):')

            rows.append('    # region model definition')
            for column in table['columns'].values():
                rows.append(f"    {column['name']}: {column['python_type']}")

            for table_relations in table['relations_one'].values():
                for relation in table_relations:
                    rows.append(f"    # {relation['join_comment']}")
                    rows.append(f"    # Rename attribute adding this to the rename_attributes argument of generate_models(): '{relation['attribute_names'][-1]}': 'new_name'")
                    rows.append(f"    {relation['attribute_name']}: {relation['type']}")

            for table_relations in table['relations_many'].values():
                for relation in table_relations:
                    rows.append(f"    # {relation['join_comment']}")
                    rows.append(f"    # Rename attribute adding this to the rename_attributes argument of generate_models(): '{relation['attribute_names'][-1]}': 'new_name'")
                    rows.append(f"    {relation['attribute_name']}: {relation['hint']}")

            for table_relations in table['relations_many_many'].values():
                for relation in table_relations:
                    to_table = tables[relation['to_table']]
                    rows.append(f"    # {relation['join_comment']}")
                    rows.append(f"    # Rename attribute adding this to the rename_attributes argument of generate_models(): '{relation['attribute_names'][-1]}': 'new_name'")
                    rows.append(f"    {relation['attribute_name']}: List['{to_table['camel']}']")

            rows.append(' ')
            rows.append('    class Meta:')
            rows.append(f"        table_name = '{table['table_name']}'")
            rows.append(f"        primary_key = '{table['primary_key']}'")
            rows.append(f"        singular = '{table['singular']}'")
            rows.append(f"        plural = '{table['plural']}'")
            rows.append(f"        camel = '{table_camel}'")
            rows.append(f"        columns = {list(table['columns'].keys())}")

            if table['relations_one']:
                rows.append('        relations_one = {')
                for table_name, table_relations in table['relations_one'].items():
                    rows.append(f"            '{table_name}': [")
                    for relation in table_relations:
                        rows.append(
                            f"                RelationOne('{relation['attribute_name']}', '{relation['other_attribute_name'] if 'other_attribute_name' in relation else 'xxx'}', {relation['type']}, '{relation['from_column']}', '{relation['to_table']}', '{relation['to_column']}'),")
                    rows.append('            ],')
                rows.append('        }')
            else:
                rows.append('        relations_one = {}')

            if table['relations_many']:
                rows.append('        relations_many = {')
                for table_name, table_relations in table['relations_many'].items():
                    rows.append(f"            '{table_name}': [")
                    for relation in table_relations:
                        rows.append(f"                RelationMany('{relation['attribute_name']}', '{relation['type']}', '{relation['from']}', '{relation['to_table']}'),")
                    rows.append('            ],')
                rows.append('        }')
            else:
                rows.append('        relations_many = {}')

            if table['relations_many_many']:
                rows.append('        relations_many_many = {')
                for table_name, table_relations in table['relations_many_many'].items():
                    rows.append(f"            '{table_name}': [")
                    for relation in table_relations:
                        rows.append(
                            f"                RelationManyMany('{relation['attribute_name']}', '{tables[relation['to_table']]['camel']}', '{relation['from']}', '{relation['to_table']}', '{relation['to']}', '{relation['link_table']}'),")
                    rows.append('            ],')
                rows.append('        }')
            else:
                rows.append('        relations_many_many = {}')

            rows.append(' ')
            args1 = ', '.join(f"{c['name']}: '{c['python_type']}' = None" for c in table['columns'].values())
            args2 = ', '.join(f"{c['name']}={c['name']}" for c in table['columns'].values())
            primary_key_name = table['primary_key']
            primary_key_type = table['columns'][primary_key_name]['python_type']
            rows.append(f'    @staticmethod')
            rows.append(f"    def create_record(transaction: Transaction, {args1}) -> '{table_camel}':")
            rows.append(f"        return transaction.create_record({table['camel']}, {args2})")
            rows.append(' ')
            rows.append(f"    def delete_record(self) -> None:")
            rows.append(f"        return self._table.transaction.delete_record({table['camel']}, primary_key_value=self.{primary_key_name})")

            rows.append('    # endregion')
            rows.append(' ')
            rows.append('    # region manually added class members')
            code = manually_added_code.get(table_camel, [])
            if code:
                rows.extend(code)
                del manually_added_code[table_camel]
            rows.append('    # endregion')
            rows.append(' ')

    @classmethod
    def add_query_result(cls, rows, tables):
        rows.append(f'class QueryResult:')

        for table_name, table in tables.items():
            rows.append(f"    {table['plural']}: List['{table['camel']}']")

        rows.append(' ')

        for table_name, table in tables.items():
            rows.append(f"    {table['plural']}_dict: Dict[Any, '{table['camel']}']")

        rows.append(' ')

        rows.append('    def __repr__(self):')
        rows.append('        items = [')
        rows.append("            f'{prp} {len(getattr(self, prp, []))}' for prp in [")
        for table_name, table in tables.items():
            rows.append(f"                '{table['plural']}',")
        rows.append('            ]')
        rows.append('            if len(getattr(self, prp, []))')
        rows.append('        ]')
        rows.append("        items_txt = ', '.join(items) or 'empty'")
        rows.append("        return f'<QueryResult {items_txt}>'")

        rows.append(' ')

    @classmethod
    def add_db_config(cls, rows: List[str], tables, connection_string, dialect):
        schema = {table['table_name']: {column['name']: column['sql_type']
                                        for column in table['columns'].values()}
                  for table in tables.values()}

        primary_key_columns = {table['table_name']: table['primary_key']
                               for table in tables.values()}

        db_config = {
            'schema':              schema,
            'primary_key_columns': primary_key_columns,
            'connection_string':   connection_string,
            'dialect':             dialect,
        }

        db_config_rows = json.dumps(db_config, indent=4).split('\n')
        db_config_rows[0] = 'db_config = ' + db_config_rows[0]

        rows.extend(db_config_rows)
        rows.append(' ')

    @classmethod
    def add_bottom_comments(cls, rows, tables, py_file_name, dialect):
        file_name = py_file_name[:-3].replace('/', '.').replace('\\', '.')
        models = ', '.join(f"{table['camel']}" for table in tables.values())
        rows.append(f'# from {file_name} import {models}, QueryResult, db_config')
        if cls.additional_imports():
            rows.append(f'# from uniquery_{dialect} import UniQuerySession, Transaction, ' + ','.join(cls.additional_imports()))
        else:
            rows.append(f'# from uniquery_{dialect} import UniQuerySession, Transaction')

    @classmethod
    def insert_missing_code(cls, rows, manually_added_code):
        if not manually_added_code:
            return

        new_rows = ['']
        new_rows.append('# region other code manually added')
        for region, code in manually_added_code.items():
            if region != 'other code manually added':
                new_rows.append(f'# code from missing class {region}')
            new_rows.extend(code)
        new_rows.append('# endregion')
        new_rows.append(' ')

        i = rows.index('# endregion') + 1
        rows[i:i] = new_rows

    @classmethod
    def get_manually_added_code(cls, py_file_name):
        if not os.path.exists(py_file_name):
            return {}

        code = {}
        collecting_code = False
        with open(py_file_name, 'r') as f:
            for line in f:
                match = re.match(r'class (.*)\(UniQueryModel\):', line)
                if match:
                    current_class = match.group(1)
                    collected_code = []
                    code[current_class] = collected_code

                elif line.startswith('# region manually added imports'):
                    collecting_code = True
                    collected_code = []
                    code['manually added imports'] = collected_code

                elif line.startswith('# region other code manually added'):
                    collecting_code = True
                    collected_code = []
                    code['other code manually added'] = collected_code

                elif line.strip() == '# region manually added class members':
                    collecting_code = True

                elif line.strip() == '# endregion':
                    collecting_code = False

                elif collecting_code:
                    collected_code.append(line.strip('\n'))

        code = {k: v for k, v in code.items() if v}  # remove empty lists

        return code

    @classmethod
    def generate_models(cls, connection_string, py_file_name, tables=None, rename_attributes=None):
        raise NotImplementedError

    @classmethod
    def generate_models_2(cls, connection_string, py_file_name, dialect, tables=None, rename_attributes=None):
        assert py_file_name.endswith('.py')

        cls.require_import_datetime = False
        if not tables:
            tables = cls.get_db_full_schema(connection_string, rename_attributes)

        manually_added_code = cls.get_manually_added_code(py_file_name)

        rows = []
        cls.add_imports(rows, manually_added_code, dialect)
        cls.add_model_definitions(rows, tables, manually_added_code)
        cls.add_query_result(rows, tables)
        cls.add_db_config(rows, tables, connection_string, dialect)
        cls.add_bottom_comments(rows, tables, py_file_name, dialect)
        cls.insert_missing_code(rows, manually_added_code)

        with open(py_file_name, 'w') as f:
            f.write('\n'.join(rows))
