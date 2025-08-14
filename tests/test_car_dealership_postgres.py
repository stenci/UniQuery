import os
import shutil
import sys
import time
from pprint import pformat

import psycopg2
import pytest
from dotenv import load_dotenv

from uniquery.exceptions import RenamedAttributeNotFound
from uniquery.uniquery_postgres import UniQuerySession, ModelGenerator, MissingId, logger

# The module tmp_postgres.car_dealership_models does not exist yet. It is
# generated together with the test database by the first test and
# deleted when the test tears down.
#
# The test that generates database and module must be defined
# first, because the order of execution is the same as the order
# of definition.

load_dotenv()

USER = os.getenv("local_postgres_user")
PASSWORD = os.getenv("local_postgres_password")
HOST = os.getenv("local_postgres_host")
PORT = os.getenv("local_postgres_port")
DBNAME = os.getenv("local_postgres_dbname")

connection_string = f"user={USER} password={PASSWORD} host={HOST} port={PORT} dbname={DBNAME}"
models_file_name = 'tmp_postgres/car_dealership_models.py'


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown():
    assert os.path.isfile(
        __file__), 'File "test_car_dealership_postgres.py" does not exist in the current folder. Make sure the tests are executed in the "tests" folder, because the "test/tmp_postgres" folder is required and will be generated during setup and removed during tear down.'
    if os.path.isdir('tmp_postgres'):
        shutil.rmtree('tmp_postgres')
    os.makedirs('tmp_postgres', exist_ok=True)

    sys.path.insert(0, '.')

    yield

    # Comment out the next line to prevent the temporary folder deletion and inspect the generated module
    shutil.rmtree('tmp_postgres')


def test_create_database_and_data():
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cur:
            create_database(cur)
            add_data(cur)

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
    from tmp_postgres.car_dealership_models import QueryResult, db_config
    with  UniQuerySession(db_config, True) as session:
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
    from tmp_postgres.car_dealership_models import QueryResult, db_config
    with  UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(q := QueryResult(),
                     """SELECT *
                        FROM cars
                                 JOIN clients_cars ON cars.id = clients_cars.car_id
                                 JOIN clients ON clients_cars.client_id = clients.id""")

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


def test_join_query():
    from tmp_postgres.car_dealership_models import QueryResult, db_config
    with  UniQuerySession(db_config, True) as session:
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
    from tmp_postgres.car_dealership_models import QueryResult, db_config
    with  (UniQuerySession(db_config, True) as session1,
           UniQuerySession(db_config, True) as session2):
        tr1 = session1.transaction()
        tr1.__enter__()
        tr1.query(q1 := QueryResult(), "SELECT * FROM invoice WHERE id = 'Invoice1'")
        q1.invoices[0].amount += 1
        q1.invoices[0].save()

        tr2 = session2.transaction()
        tr2.__enter__()
        tr2.query(q2 := QueryResult(), "SELECT * FROM invoice WHERE id = 'Invoice1'")
        assert q2.invoices[0].amount < q1.invoices[0].amount
        tr2.__exit__(None, None, None)

        tr1.commit()

        tr2 = session2.transaction()
        tr2.__enter__()
        tr2.query(q2 := QueryResult(), "SELECT * FROM invoice WHERE id = 'Invoice1'")
        assert q2.invoices[0].amount == q1.invoices[0].amount
        tr2.abort()
        tr2.__exit__(None, None, None)

        tr1.__exit__(None, None, None)

        session1.__exit__(None, None, None)
        session2.__exit__(None, None, None)


def test_create_and_delete():
    from tmp_postgres.car_dealership_models import QueryResult, db_config, Car, Table1
    with  UniQuerySession(db_config, True) as session:
        # add one row with id manually set to 3
        with session.transaction() as tr:
            row = Table1.create_record(tr, text='abc', int=123)
            assert getattr(row, 'id', None) is None
            row.id = 3
            row.save()
            assert row.id == 3

        # add three rows with autoincremented id, and expect a failure on the 3rd one
        with pytest.raises(psycopg2.errors.UniqueViolation):
            for n in range(1, 4):
                with session.transaction() as tr:
                    row = Table1.create_record(tr, text='def', int=456)
                    assert getattr(row, 'id', None) is None
                    row.save()
                    print(row.id)
                    assert row.id and row.id == n
        assert n == 3

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
    from tmp_postgres.car_dealership_models import db_config, Car
    with  UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            car = Car.get_by_pk_value(tr, 'Car1')
            assert car._id == 'Car1'

            car = Car.get_by_pk_value(tr, 'nope')
            assert not car


def test_where_in():
    from tmp_postgres.car_dealership_models import QueryResult, db_config
    with  UniQuerySession(db_config, True) as session:
        with session.transaction() as tr:
            tr.query(
                q := QueryResult(),
                'SELECT * FROM cars WHERE cars.id IN (%s, %s, %s, %s)',
                ('Car1', 'Car3', 'Car5', 'Car6')
            )
            assert len(q.cars) == 3


def test_empty_relations():
    from tmp_postgres.car_dealership_models import QueryResult, db_config
    with  UniQuerySession(db_config, True) as session:
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


def create_database(cur: psycopg2.extensions.cursor):
    cur.execute("""
                DROP TABLE IF EXISTS cars CASCADE;
                DROP TABLE IF EXISTS clients CASCADE;
                DROP TABLE IF EXISTS clients_cars CASCADE;
                DROP TABLE IF EXISTS repairs CASCADE;
                DROP TABLE IF EXISTS salesreps CASCADE;
                DROP TABLE IF EXISTS invoice CASCADE;
                DROP TABLE IF EXISTS table1 CASCADE;
                DROP TABLE IF EXISTS parts CASCADE;
                DROP TABLE IF EXISTS bom_link CASCADE;
                DROP TABLE IF EXISTS company CASCADE;
                DROP TABLE IF EXISTS part_seller CASCADE;
                DROP TABLE IF EXISTS part_maker CASCADE;
                """)

    cur.execute("""
                CREATE TABLE cars
                (
                    id    VARCHAR PRIMARY KEY,
                    make  VARCHAR,
                    model VARCHAR,
                    year  INTEGER
                )
                """)

    cur.execute("""
                CREATE TABLE clients
                (
                    id   VARCHAR PRIMARY KEY,
                    name VARCHAR
                )
                """)

    cur.execute("""
                CREATE TABLE clients_cars
                (
                    client_id VARCHAR,
                    car_id    VARCHAR,
                    id        VARCHAR PRIMARY KEY,
                    FOREIGN KEY (client_id) REFERENCES clients (id),
                    FOREIGN KEY (car_id) REFERENCES cars (id)
                )
                """)

    cur.execute("""
                CREATE TABLE repairs
                (
                    id          VARCHAR PRIMARY KEY,
                    description VARCHAR,
                    car_id      VARCHAR,
                    FOREIGN KEY (car_id) REFERENCES cars (id)
                )
                """)

    cur.execute("""
                CREATE TABLE salesreps
                (
                    id   VARCHAR PRIMARY KEY,
                    name VARCHAR
                )
                """)

    cur.execute("""
                CREATE TABLE invoice
                (
                    id          VARCHAR PRIMARY KEY,
                    amount      REAL,
                    car_id      VARCHAR,
                    repair_id   VARCHAR,
                    salesrep_id VARCHAR,
                    FOREIGN KEY (car_id) REFERENCES cars (id),
                    FOREIGN KEY (repair_id) REFERENCES repairs (id),
                    FOREIGN KEY (salesrep_id) REFERENCES salesreps (id)
                )
                """)

    cur.execute("""
                CREATE TABLE table1
                (
                    id   SERIAL PRIMARY KEY,
                    text VARCHAR,
                    int  INTEGER
                )
                """)

    cur.execute("""
                CREATE TABLE parts
                (
                    part_number VARCHAR PRIMARY KEY,
                    description VARCHAR
                )
                """)

    cur.execute("""
                CREATE TABLE bom_link
                (
                    id     SERIAL PRIMARY KEY,
                    parent VARCHAR,
                    child  VARCHAR,
                    qty    INTEGER,
                    FOREIGN KEY (parent) REFERENCES parts (part_number),
                    FOREIGN KEY (child) REFERENCES parts (part_number)
                )
                """)

    cur.execute("""
                CREATE TABLE company
                (
                    id   SERIAL PRIMARY KEY,
                    name VARCHAR
                )
                """)

    cur.execute("""
                CREATE TABLE part_seller
                (
                    id          SERIAL PRIMARY KEY,
                    part_number VARCHAR,
                    company_id  INTEGER,
                    price       REAL,
                    FOREIGN KEY (company_id) REFERENCES company (id),
                    FOREIGN KEY (part_number) REFERENCES parts (part_number)
                )
                """)

    cur.execute("""
                CREATE TABLE part_maker
                (
                    id          SERIAL PRIMARY KEY,
                    part_number VARCHAR,
                    company_id  INTEGER,
                    price       REAL,
                    FOREIGN KEY (company_id) REFERENCES company (id),
                    FOREIGN KEY (part_number) REFERENCES parts (part_number)
                )
                """)



def add_data(cur: psycopg2.extensions.cursor):
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
