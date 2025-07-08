import os
import shutil
import sqlite3
import sys
import time
from pprint import pformat

import pytest

from uniquery.exceptions import RenamedAttributeNotFound
from uniquery.uniquery_sqlite import UniQuerySession, TransactionMode, MissingId, ModelGenerator, logger

# The module tmp_sqlite.car_dealership_models does not exist yet. It is
# generated together with the test database by the first test and
# deleted when the test tears down.
#
# The test that generates database and module must be defined
# first, because the order of execution is the same as the order
# of definition.

connection_string = 'tmp_sqlite/car_dealership.db'
models_file_name = 'tmp_sqlite/car_dealership_models.py'


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown():
    assert os.path.isfile(
        __file__), 'File "test_car_dealership_sqlite.py" does not exist in the current folder. Make sure the tests are executed in the "tests" folder, because the "test/tmp_sqlite" folder is required and will be generated during setup and removed during tear down.'
    if os.path.isdir('tmp_sqlite'):
        shutil.rmtree('tmp_sqlite')
    os.makedirs('tmp_sqlite')

    sys.path.insert(0, '.')

    yield

    # Unfortunately, deleting the file at this stage sometimes does not work. Even the code below may
    # fail. Apparently, using context managers or explicitly closing the cursor— which are the correct
    # and documented ways to release resources — does not always release them. Therefore, we attempt to
    # delete the file and simply ignore any failure.
    #
    # with sqlite3.connect('file.db') as conn:
    #     try:
    #         cur = conn.cursor()
    #         cur.execute("CREATE TABLE cars (make TEXT, model TEXT, year  INTEGER)")
    #         cur.execute("INSERT INTO cars(make, model, year) VALUES('Toyota', 'Camry', 2018)")
    #         cur.connection.commit()
    #     finally:
    #         if cur:
    #             cur.close()
    #
    # os.unlink('file.db')

    # Comment out the next lines to prevent the temporary folder deletion and inspect the generated module
    try:
        shutil.rmtree('tmp_sqlite')
    except PermissionError:
        print('Impossible to delete the temporary database file because it is still locked (Python / sqlite / Windows error??)')


def test_create_database_and_data():
    with sqlite3.connect(connection_string) as conn:
        try:
            cur = conn.cursor()
            create_database(cur)
            add_data(cur)
        finally:
            if cur:
                cur.close()

    # test the exception by trying to generate the module using the wrong rename_attributes
    with pytest.raises(RenamedAttributeNotFound) as excinfo:
        ModelGenerator.generate_models(
            connection_string, models_file_name,
            rename_attributes={
                'non_existing_attribute_1': 'hello',
                'non_existing_attribute_2': 'hello',
            }
        )
    assert 'non_existing_attribute' in str(excinfo.value)

    # generate the correct module
    ModelGenerator.generate_models(
        connection_string, models_file_name,
        rename_attributes={
            'parts__part_number__bom_link__child':            'parents_links',
            'parts__part_number__bom_link__parent':           'children_links',
            'cars__car_id__clients_cars__clients__client_id': 'clients_renamed',
        }
    )
    inject_manual_code()


def test_save():
    from tmp_sqlite.car_dealership_models import QueryResult, db_config
    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(q1 := QueryResult(),
                     """SELECT *
                        FROM invoice
                                 JOIN salesreps ON invoice.salesrep_id = salesreps.id
                        WHERE salesreps.id = 'salesrep2'
                        ORDER BY invoice.id""")
            logger.info(pformat(q1.invoices))
            logger.info(pformat(q1.invoices[0].salesrep.name))
            q1.invoices[0].amount += 1
            q1.invoices[0].save()
            tr.commit()
            logger.info(pformat(q1.salesreps))
            logger.info(pformat(q1.salesreps[0].invoices))
            logger.info(pformat(q1.salesreps_dict['salesrep2']))

            tr.query(q2 := QueryResult(),
                     """SELECT *
                        FROM invoice
                                 JOIN salesreps ON invoice.salesrep_id = salesreps.id
                        WHERE salesreps.id = 'salesrep2'
                        ORDER BY invoice.id""")

            assert q1.invoices[0].amount == q2.invoices[0].amount


def test_many_to_many():
    from tmp_sqlite.car_dealership_models import QueryResult, db_config
    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(q := QueryResult(),
                     """SELECT *
                        FROM clients FULL OUTER JOIN clients_cars
                        ON clients.id = clients_cars.client_id
                            FULL OUTER JOIN cars ON clients_cars.car_id = cars.id""")

    print('All cars')
    for car in q.cars:
        print(f'  Car: {car.make}')
        for client in car.clients_renamed:
            print(f'    Client: {client.name}')

    print('All clients')
    for client in q.clients:
        print(f'  Client: {client.name}')
        for car in client.cars:
            print(f'    Car: {car.make}')

    assert {c.make for c in q.clients_dict['Client2'].cars} == {'Volvo', 'Honda'}
    assert len(q.cars_dict['Car1'].clients_renamed) == 1
    assert len(q.cars_dict['Car2'].clients_renamed) == 0
    assert len(q.clients_dict['Client1'].cars) == 1
    assert len(q.clients_dict['Client2'].cars) == 2
    assert len(q.clients_dict['Client3'].cars) == 0


def test_join_query():
    from tmp_sqlite.car_dealership_models import QueryResult, db_config
    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(q := QueryResult(),
                     """SELECT 1 AS one, 'two' AS two, salesreps.*, invoice.*
                        FROM salesreps
                                 JOIN invoice ON invoice.salesrep_id = salesreps.id
                        ORDER BY invoice.id""")

            print('All salesreps')
            for salesrep in q.salesreps:
                print(f'  salesrep: {salesrep}')
                for invoice in salesrep.invoices:
                    print(f'    Invoice: {invoice}')

            print('All invoices')
            for invoice in q.invoices:
                print(f'  Invoice: {invoice}')
                print(f'    salesrep: {invoice.salesrep}')

    assert q.salesreps_dict['salesrep2'].invoices[2].amount == 500


def test_concurrent_transactions():
    # changes made by connection1 are visible by connection2 after they have been committed
    from tmp_sqlite.car_dealership_models import QueryResult, db_config
    with (UniQuerySession(db_config, True) as session1,
          UniQuerySession(db_config, True) as session2):
        tr1 = session1.transaction()
        tr1.__enter__()
        tr1.query(q1 := QueryResult(), "SELECT * FROM invoice WHERE id = 'Invoice1'")
        q1.invoices[0].amount += 1
        q1.invoices[0].save()

        tr2 = session2.transaction(TransactionMode.NoTransaction)
        tr2.__enter__()
        tr2.query(q2 := QueryResult(), "SELECT * FROM invoice WHERE id = 'Invoice1'")
        assert q2.invoices[0].amount < q1.invoices[0].amount
        tr2.__exit__(None, None, None)

        tr1.commit()

        tr2 = session2.transaction(TransactionMode.NoTransaction)
        tr2.__enter__()
        tr2.query(q2 := QueryResult(), "SELECT * FROM invoice WHERE id = 'Invoice1'")
        assert q2.invoices[0].amount == q1.invoices[0].amount
        tr2.abort()
        tr2.__exit__(None, None, None)

        tr1.__exit__(None, None, None)

        session1.__exit__(None, None, None)
        session2.__exit__(None, None, None)


def test_create_and_delete():
    from tmp_sqlite.car_dealership_models import QueryResult, db_config, Car, Table1
    # add one row with id manually set to 3
    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            row = Table1.create_record(tr, text='abc', int=123)
            assert getattr(row, 'id', None) is None
            row.id = 3
            row.save()
            assert row.id == 3

        # add three rows with autoincremented id
        for _ in range(3):
            with session.transaction() as tr:
                row = Table1.create_record(tr, text='def', int=456)
                assert getattr(row, 'id', None) is None
                row.save()
                assert row.id and row.id > 3

        with session.transaction() as tr:
            car = Car.create_record(tr, make='Fiat', year=1965)
            assert getattr(car, 'id', None) is None
            try:
                car.save()
            except MissingId:
                pass
            else:
                raise AssertionError("Expected exception MissingRowId not raised.")
            new_id = f'Car{time.time()}'
            car.id = new_id
            car.save()
            assert getattr(car, 'id', None) == new_id

        with session.transaction() as tr:
            tr.query(q := QueryResult(), f"SELECT * FROM cars WHERE id = '{new_id}'")
            assert q.cars[0].year == 1965
            q.cars[0].delete_record()

        with session.transaction() as tr:
            tr.query(q := QueryResult(), f"SELECT * FROM cars WHERE id = '{new_id}'")
            assert len(q.cars) == 0

        with session.transaction() as tr:
            tr.execute('DELETE FROM cars WHERE year > 8000')

        with session.transaction() as tr:
            rows = tr.insert_many('cars', ('id', 'make', 'year'),
                                  [
                                      ('C1', 'Fiat', 8001),
                                      ('C2', 'Fiat', 8002),
                                      ('C3', 'Fiat', 8003),
                                      ('C4', 'Fiat', 8004),
                                      ('C5', 'Fiat', 8005),
                                  ])
            assert rows == []

        with session.transaction() as tr:
            tr.query(q := QueryResult(), f"SELECT * FROM cars WHERE year > 8000")
            assert len(q.cars) == 5

        with session.transaction() as tr:
            tr.execute('DELETE FROM cars WHERE year > 8000')

        with session.transaction() as tr:
            tr.query(q := QueryResult(), f"SELECT * FROM cars WHERE year > 8000")
            assert len(q.cars) == 0


def test_get_by_pk_value():
    from tmp_sqlite.car_dealership_models import db_config, Car
    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            car = Car.get_by_pk_value(tr, 'Car1')
            assert car._id == 'Car1'

            car = Car.get_by_pk_value(tr, 'nope')
            assert not car


def test_where_in():
    from tmp_sqlite.car_dealership_models import QueryResult, db_config
    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(
                q := QueryResult(),
                'SELECT * FROM cars WHERE cars.id IN (?, ?, ?, ?)',
                ('Car1', 'Car3', 'Car5', 'Car6')
            )
            assert len(q.cars) == 3


def test_empty_relations():
    from tmp_sqlite.car_dealership_models import QueryResult, db_config
    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(q := QueryResult(),
                     """SELECT *
                        FROM cars
                                 LEFT JOIN clients_cars ON cars.id = clients_cars.car_id
                                 LEFT JOIN clients ON clients_cars.client_id = clients.id""")
            for car in q.cars:
                print(car, car.clients_renamed)
            for client in q.clients:
                print(client, client.cars)


def test_bom():
    from tmp_sqlite.car_dealership_models import QueryResult, db_config

    def print_children(rows, part, parent_path='', qty=''):
        path = f'{parent_path}/{part.part_number}'
        rows.append(f'{path}  {qty}  {part}')
        for child_link in part.children_links:
            print_children(rows, child_link.part__child, path, child_link.qty)

    with UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(q := QueryResult(),
                     """SELECT *
                        FROM parts
                                 LEFT JOIN bom_link ON parts.part_number = bom_link.child""")

    rows = []
    for part in q.parts:
        if part.children_links:
            rows.append('===')
            print_children(rows, part)
    print('\n'.join(rows))
    assert rows == [
        '===',
        '/A1    <Part: part_number=A1, description=Root 1>',
        '/A1/B1  1  <Part: part_number=B1, description=Assembly 1>',
        '/A1/B1/C1  2  <Part: part_number=C1, description=Subassembly 1>',
        '/A1/B1/C1/D1  3  <Part: part_number=D1, description=Part 1>',
        '/A1/B1/C1/D2  4  <Part: part_number=D2, description=Part 2>',
        '/A1/B1/D1  3  <Part: part_number=D1, description=Part 1>',
        '/A1/B2  2  <Part: part_number=B2, description=Assembly 2>',
        '/A1/B2/C1  1  <Part: part_number=C1, description=Subassembly 1>',
        '/A1/B2/C1/D1  3  <Part: part_number=D1, description=Part 1>',
        '/A1/B2/C1/D2  4  <Part: part_number=D2, description=Part 2>',
        '/A1/B2/C2  2  <Part: part_number=C2, description=Subassembly 2>',
        '/A1/B2/C2/D1  5  <Part: part_number=D1, description=Part 1>',
        '/A1/B2/C2/D3  6  <Part: part_number=D3, description=Part 3>',
        '===',
        '/A2    <Part: part_number=A2, description=Root 2>',
        '/A2/B2  3  <Part: part_number=B2, description=Assembly 2>',
        '/A2/B2/C1  1  <Part: part_number=C1, description=Subassembly 1>',
        '/A2/B2/C1/D1  3  <Part: part_number=D1, description=Part 1>',
        '/A2/B2/C1/D2  4  <Part: part_number=D2, description=Part 2>',
        '/A2/B2/C2  2  <Part: part_number=C2, description=Subassembly 2>',
        '/A2/B2/C2/D1  5  <Part: part_number=D1, description=Part 1>',
        '/A2/B2/C2/D3  6  <Part: part_number=D3, description=Part 3>',
        '/A2/D1  5  <Part: part_number=D1, description=Part 1>',
        '===',
        '/B1    <Part: part_number=B1, description=Assembly 1>',
        '/B1/C1  2  <Part: part_number=C1, description=Subassembly 1>',
        '/B1/C1/D1  3  <Part: part_number=D1, description=Part 1>',
        '/B1/C1/D2  4  <Part: part_number=D2, description=Part 2>',
        '/B1/D1  3  <Part: part_number=D1, description=Part 1>',
        '===',
        '/B2    <Part: part_number=B2, description=Assembly 2>',
        '/B2/C1  1  <Part: part_number=C1, description=Subassembly 1>',
        '/B2/C1/D1  3  <Part: part_number=D1, description=Part 1>',
        '/B2/C1/D2  4  <Part: part_number=D2, description=Part 2>',
        '/B2/C2  2  <Part: part_number=C2, description=Subassembly 2>',
        '/B2/C2/D1  5  <Part: part_number=D1, description=Part 1>',
        '/B2/C2/D3  6  <Part: part_number=D3, description=Part 3>',
        '===',
        '/C1    <Part: part_number=C1, description=Subassembly 1>',
        '/C1/D1  3  <Part: part_number=D1, description=Part 1>',
        '/C1/D2  4  <Part: part_number=D2, description=Part 2>',
        '===',
        '/C2    <Part: part_number=C2, description=Subassembly 2>',
        '/C2/D1  5  <Part: part_number=D1, description=Part 1>',
        '/C2/D3  6  <Part: part_number=D3, description=Part 3>',
    ]


def create_database(cur: sqlite3.Cursor):
    cur.execute("""
                CREATE TABLE cars
                (
                    id    TEXT PRIMARY KEY,
                    make  TEXT,
                    model TEXT,
                    year  INTEGER
                )
                """)

    cur.execute("""
                CREATE TABLE clients
                (
                    id   TEXT PRIMARY KEY,
                    name TEXT
                )
                """)

    cur.execute("""
                CREATE TABLE clients_cars
                (
                    client_id INTEGER,
                    car_id    INTEGER,
                    id        TEXT PRIMARY KEY,
                    FOREIGN KEY (client_id) REFERENCES clients (id),
                    FOREIGN KEY (car_id) REFERENCES cars (id)
                )
                """)

    cur.execute("""
                CREATE TABLE repairs
                (
                    id          TEXT PRIMARY KEY,
                    description TEXT,
                    car_id      INTEGER,
                    FOREIGN KEY (car_id) REFERENCES cars (id)
                )
                """)

    cur.execute("""
                CREATE TABLE salesreps
                (
                    id   TEXT PRIMARY KEY,
                    name TEXT
                )
                """)

    # intentionally singular, just to test both plural and singular table names
    cur.execute("""
                CREATE TABLE invoice
                (
                    id          TEXT PRIMARY KEY,
                    amount      REAL,
                    car_id      INTEGER,
                    repair_id   INTEGER,
                    salesrep_id INTEGER,
                    FOREIGN KEY (car_id) REFERENCES cars (id),
                    FOREIGN KEY (repair_id) REFERENCES repairs (id),
                    FOREIGN KEY (salesrep_id) REFERENCES salesreps (id)
                )
                """)

    # another table with numerical autoincrement primary key
    cur.execute("""
                CREATE TABLE table1
                (
                    id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    text TEXT,
                    int  INTEGER
                )
                """)

    cur.execute("""
                CREATE TABLE parts
                (
                    part_number TEXT PRIMARY KEY,
                    description TEXT
                )
                """)

    cur.execute("""
                CREATE TABLE bom_link
                (
                    id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent TEXT,
                    child  TEXT,
                    qty    INTEGER,
                    FOREIGN KEY (parent) REFERENCES parts (part_number),
                    FOREIGN KEY (child) REFERENCES parts (part_number)
                )
                """)

    cur.execute("""
                CREATE TABLE company
                (
                    id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT
                )
                """)

    cur.execute("""
                CREATE TABLE part_seller
                (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    part_number TEXT,
                    company_id  TEXT,
                    price       REAL,
                    FOREIGN KEY (company_id) REFERENCES company (id),
                    FOREIGN KEY (part_number) REFERENCES parts (part_number)
                )
                """)

    cur.execute("""
                CREATE TABLE part_maker
                (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    part_number TEXT,
                    company_id  TEXT,
                    price       REAL,
                    FOREIGN KEY (company_id) REFERENCES company (id),
                    FOREIGN KEY (part_number) REFERENCES parts (part_number)
                )
                """)


def add_data(cur: sqlite3.Cursor):
    cur.execute("INSERT INTO cars(id, make, model, year) VALUES('Car1', 'Toyota', 'Camry', 2018)")
    cur.execute("INSERT INTO cars(id, make, model, year) VALUES('Car2', 'Honda', 'Accord', 2019)")
    cur.execute("INSERT INTO cars(id, make, model, year) VALUES('Car3', 'Honda', 'Accord', 2020)")
    cur.execute("INSERT INTO cars(id, make, model, year) VALUES('Car4', 'Chevy', 'Bolt', 2017)")
    cur.execute("INSERT INTO cars(id, make, model, year) VALUES('Car5', 'Volvo', 'CX60', 2015)")

    cur.execute("INSERT INTO clients(id, name) VALUES('Client1', 'Client One')")
    cur.execute("INSERT INTO clients(id, name) VALUES('Client2', 'Client Two')")
    cur.execute("INSERT INTO clients(id, name) VALUES('Client3', 'Client Three')")

    cur.execute("INSERT INTO clients_cars(id, client_id, car_id) VALUES('Client1Car1', 'Client1', 'Car1')")
    cur.execute("INSERT INTO clients_cars(id, client_id, car_id) VALUES('Client2Car3', 'Client2', 'Car3')")
    cur.execute("INSERT INTO clients_cars(id, client_id, car_id) VALUES('Client2Car5', 'Client2', 'Car5')")

    cur.execute("INSERT INTO repairs(id, description, car_id) VALUES('Repair1', 'brake repair', 'Car5')")
    cur.execute("INSERT INTO repairs(id, description, car_id) VALUES('Repair2', 'oil change', 'Car5')")
    cur.execute("INSERT INTO repairs(id, description, car_id) VALUES('Repair3', 'oil change', 'Car3')")
    cur.execute("INSERT INTO repairs(id, description, car_id) VALUES('Repair4', 'fix', 'Car2')")

    cur.execute("INSERT INTO salesreps(id, name) VALUES('salesrep1', 'Bob')")
    cur.execute("INSERT INTO salesreps(id, name) VALUES('salesrep2', 'Tom')")

    cur.execute("INSERT INTO invoice(id, amount, car_id, salesrep_id) VALUES('Invoice1', 30000.00, 'Car1', 'salesrep1')")
    cur.execute("INSERT INTO invoice(id, amount, car_id, salesrep_id) VALUES('Invoice2', 33000.00, 'Car2', 'salesrep2')")
    cur.execute("INSERT INTO invoice(id, amount, repair_id, salesrep_id) VALUES('Invoice3', 200.00, 'Repair1', 'salesrep2')")
    cur.execute("INSERT INTO invoice(id, amount, repair_id, salesrep_id) VALUES('Invoice4', 500.00, 'Repair2', 'salesrep2')")

    cur.execute("INSERT INTO parts(part_number, description) VALUES('A1', 'Root 1')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('A2', 'Root 2')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('B1', 'Assembly 1')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('B2', 'Assembly 2')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('C1', 'Subassembly 1')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('C2', 'Subassembly 2')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('D1', 'Part 1')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('D2', 'Part 2')")
    cur.execute("INSERT INTO parts(part_number, description) VALUES('D3', 'Part 3')")

    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('A1', 'B1', 1)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('A1', 'B2', 2)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('A2', 'B2', 3)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('A2', 'D1', 5)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('B1', 'C1', 2)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('B1', 'D1', 3)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('B2', 'C1', 1)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('B2', 'C2', 2)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('C1', 'D1', 3)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('C1', 'D2', 4)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('C2', 'D1', 5)")
    cur.execute("INSERT INTO bom_link(parent, child, qty) VALUES('C2', 'D3', 6)")

    cur.execute("INSERT INTO company(id, name) VALUES(1, 'Maker 1')")
    cur.execute("INSERT INTO company(id, name) VALUES(2, 'Maker 2')")
    cur.execute("INSERT INTO company(id, name) VALUES(3, 'Distributor 1')")
    cur.execute("INSERT INTO company(id, name) VALUES(4, 'Distributor 2')")
    cur.execute("INSERT INTO company(id, name) VALUES(5, 'Maker and Distributor 1')")
    cur.execute("INSERT INTO company(id, name) VALUES(6, 'Maker and Distributor 2')")

    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('A1', 1, 50)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('B1', 1, 20)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('C1', 1, 5)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('C2', 1, 2)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D1', 1, 5)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D2', 1, 2)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('C1', 2, 4)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('C2', 2, 1)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D1', 2, 4)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D2', 2, 1)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D3', 2, 3)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('C2', 5, 2)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D1', 5, 5)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D2', 5, 2)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('C1', 6, 4)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('C2', 6, 1)")
    cur.execute("INSERT INTO part_maker(part_number, company_id, price) VALUES('D1', 6, 4)")

    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('A1', 3, 60)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('A2', 3, 70)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('B1', 3, 20)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('B2', 3, 25)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('A1', 4, 62)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('A2', 4, 72)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('B1', 4, 23)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('B2', 4, 23)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('A1', 5, 64)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('A2', 5, 75)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('B1', 6, 21)")
    cur.execute("INSERT INTO part_seller(part_number, company_id, price) VALUES('B2', 6, 21)")

    cur.connection.commit()


def inject_manual_code():
    with open(models_file_name, 'r') as f:
        lines = [line.rstrip() for line in f.readlines()]

    part_class_line_index = lines.index("class Part(UniQueryModel):")
    region_line_index = lines.index("    # region manually added class members", part_class_line_index)

    new_lines = [
        "    # there is nothing really to inject",
    ]
    lines[region_line_index + 1:region_line_index + 1] = new_lines

    with open(models_file_name, 'w') as f:
        f.write('\n'.join(lines))
