# UniQuery

**Raw SQL in. Typed Python Objects out. The only ORM that just does ORM.**

## Table of Contents

- [Overview](#overview)
- [Introduction](#introduction)
- [Traditional ORMs vs UniQuery](#traditional-orms-vs-uniquery)
- [Installation](#installation)
- [Getting Started](#getting-started)
- [Features](#features)
- [Architecture Details](#architecture-details)
- [License](#license)

## Overview

Here’s what UniQuery is about: raw SQL in, typed Python objects out — with relationships handled automatically.

```python
from my_models import db_config, UniQuerySession, QueryResult

with UniQuerySession(db_config) as session:
    with session.transaction() as tr:
        tr.query(q := QueryResult(), """
            SELECT *
            FROM clients
            FULL OUTER JOIN clients_cars ON clients.id = clients_cars.client_id
            FULL OUTER JOIN cars ON clients_cars.car_id = cars.id
        """)

print(q.cars[0].clients[0].name)
print(q.clients[0].cars[0].make)
print(q.cars_dict['Car1'].make)
```

- Write raw SQL with a `JOIN`
- UniQuery automatically populates `q.cars` and `q.clients`
- Each `Car` object gets its related `Client` objects as `.clients`
- Each `Client` object includes a `.cars` list of related cars

## Introduction

UniQuery is not a traditional ORM — it doesn’t create tables, manage migrations, or build queries for you.

Instead, it’s designed for developers who prefer writing raw SQL and want Python objects back — with relationships handled automatically.

Most traditional ORMs hide SQL behind layers of abstraction, which leads to limitations, hidden performance problems, and extra learning curves. UniQuery instead assumes you already know SQL and builds a thin, powerful interface that adds convenience without taking control away.

Database design is best handled using tools like **DataGrip** or **Azure Data Studio** — not an ORM. I don’t want to use an ORM to define my schema. I want to use the right tools for that. An ORM is supposed to mean _Object Relational Mapping_, and that’s exactly what I need: a library that, after I run the SQL I wrote myself (with the right tools), gives me back lists and dictionaries of properly typed Python objects, connected through their relationships, not something that limits my SQL, slows down my queries, or gives me weak type hints.

**UniQuery** is a Python library that provides a unified interface for working with relational databases by automatically generating data model classes from your database schema and enabling intuitive querying and data manipulation. It supports multiple database backends (currently SQLite and PostgreSQL) and uses introspection to create Python classes for each database table. With UniQuery, you can write standard SQL queries and receive the results as Python objects with their inter-table relationships automatically handled.

## Traditional ORMs vs UniQuery

| Feature               | Traditional ORMs                                                                                 | UniQuery                                                                                       |
| --------------------- | ------------------------------------------------------------------------------------------------ |------------------------------------------------------------------------------------------------|
| **Schema Definition** | You define the classes, and they generate the database schema.                                   | You start with an existing database, and generate the classes from it using `generate_models`. |
| **Drawbacks**         | Trial and error to get schema right; advanced DB features often inaccessible due to abstraction. | No abstraction: use all advanced DB features directly.                                         |
| **Querying**          | Must use the ORM's query builder — often unintuitive and limiting for complex queries.           | Write pure SQL in your DB dialect — any query your DB supports is valid.                       |
| **Type Hinting**      | Often incomplete or incorrect due to dynamic typing and query builder complexity.                | Fully automatic and accurate via `generate_models`.                                            |
| **Schema Sync**       | Easy to get out of sync by editing classes directly.                                             | Update schema → rerun `generate_models` → classes are synced (custom methods preserved).       |
| **1+N Problem**       | Requires manual optimization or advanced ORM knowledge to avoid.                                 | Make one SQL query, UniQuery normalizes everything automatically in one round trip.            |
| **Migrations**        | Painful: learn ORM-specific migration DSL and keep classes backward-compatible.                  | Just run your SQL migrations and regenerate models. Done.                                      |

---

## Installation

Install for SQLite only:
```bash
pip install uniquery
```

Install for Postgres:
```bash
pip install uniquery[postgres]
```

Install via pip from GitHub:
```bash
pip install git+https://github.com/stenci/UniQuery.git
```

## Getting Started

Using UniQuery involves a simple 4+1 step process:

### Step 1: Create Your Database (Using Your Preferred Tools)

Design and create your database schema using tools like **DataGrip**, **Azure Data Studio**, or plain SQL. Ensure that every table — including link tables used for many-to-many relations — has a **primary key**. UniQuery requires this for proper model generation and relationship mapping.

### Step 2: Generate Python Models

Run the `generate_models()` function to introspect the database and generate a Python module with ORM model classes and schema metadata:

```python
from uniquery.uniquery_sqlite import ModelGenerator
ModelGenerator.generate_models("path/to/your_database.sqlite", "my_models.py")
```

This creates:
- One `UniQueryModel` subclass per table
- A `QueryResult` container class
- A `db_config` dictionary with connection/schema info
- Clearly marked **editable regions** in the file where you can safely add custom methods

### Step 3: Customize Models (Optional but Recommended)

Open the generated `my_models.py` and add helper methods or computed properties inside the editable regions (`# region custom code` / `# endregion`). These edits are preserved when regenerating models.

### Step 4: Connect, Query, and Use ORM Models

You can now connect to the database and run SQL queries. UniQuery maps the results to model instances and populates relationships automatically.

```python
from my_models import UniQuerySession, QueryResult, db_config

with UniQuerySession(db_config) as session:
    with session.transaction() as tr:
        tr.query(q := QueryResult(), "SELECT * FROM cars")

print(q.cars[0].make)
```

- `q.cars` is a list of `Car` instances.
- `q.cars_dict` maps primary keys (e.g. car IDs) to `Car` objects.
- Relationships like `.clients` on each car will only be populated if your query joins the `clients` table — see the [Overview](#overview) for an example.

### Step 5 (Optional): Regenerate Models After Schema Changes

If your database structure changes, simply rerun:

```python
ModelGenerator.generate_models("path/to/your_database.sqlite", "my_models.py")
```

UniQuery will update the model definitions while preserving all your customizations in the editable regions.

---

## Features

* **Automatic Model Generation:** UniQuery can introspect an existing database and generate Python model classes for each table. The `ModelGenerator` utility creates a models file with classes (subclasses of `UniQueryModel`) representing tables, including their fields and relationships.
  * Note: Every table in the database must have a primary key, including link tables used for many-to-many relationships.

* **Multi-Database Support:** The library is designed to work with different SQL databases. It includes implementations for SQLite and PostgreSQL, each with appropriate handling of connections, SQL syntax, and placeholders.

* **Unified Query Interface:** Write regular SQL queries (including JOINs and complex selects) and let UniQuery handle the rest. When you execute a query through UniQuery, it automatically builds Python objects for each table involved in the query and organizes them into a `QueryResult` container. If your query involves multiple tables, UniQuery populates the corresponding lists in the `QueryResult` (e.g. `query_result.orders` and `query_result.customers` for an orders-customers join) and sets up the relationships between objects. For example, in a scenario with a many-to-many relation between *clients* and *cars*, each `Car` instance will have a `.clients` list of related clients, and each `Client` instance a `.cars` list – all populated automatically from the query results.

* **Relationship Handling:** UniQuery infers one-to-many, many-to-one, and many-to-many relationships from foreign keys in your schema and makes navigating these relations easy. After a query, related records can be accessed as attributes on the model instances. For instance, if an **Invoice** table has a foreign key to **SalesRep**, querying invoices will allow each invoice object to have a `.salesrep` attribute, and each salesrep object will have an `.invoices` list of Invoice objects. Many-to-many link tables are recognized (if a table consists solely of two foreign keys plus a primary key) and the related objects on both sides are connected through convenient list attributes.

* **Naming Conventions:** UniQuery uses a consistent pattern:
  - Model classes are singular (e.g. `Client`, `CarModel`)
  - On the one side of a relationship, attributes are singular (e.g. `client.salesrep`)
  - On the many side, attributes are plural (e.g. `salesrep.clients`)
  - Many-to-many relations use plural lists on both sides
  This helps make code predictable and type-safe. Attribute names are generated using singularization/pluralization with support for common irregular forms.

* **Choosing the Right JOIN**: In many cases, a `LEFT OUTER JOIN` or even a `FULL OUTER JOIN` is not only acceptable but ideal. It ensures that UniQuery can construct complete object lists for both sides of the join, even when one side has no corresponding match. This allows, for instance, a `Client` object with no `Car` entries (or vice versa) to still be included in the result, with its `.cars` or `.clients` list simply being empty. Since UniQuery populates the relationships based on joined data, outer joins are often the most natural way to retrieve and link all involved objects at once.

* **Transaction Management:** UniQuery provides a high-level transaction API via context managers. You work with a `UniQuerySession` (database connection context) and open transactions using `session.transaction()`. Within a transaction, you can execute queries and make changes. The transaction can be committed or rolled back as needed. The API supports nested transactions with savepoints for databases that allow it (so you can safely handle partial rollbacks). Simply exiting the transaction context (or calling `abort()` on it) will roll back uncommitted changes, whereas calling `commit()` will persist them and keep the transaction open for further operations.

* **Data Manipulation (CRUD Operations):** The model objects returned by UniQuery are active records – you can create, update, or delete using intuitive methods:

  * *Create:* Use the class method `YourModel.create_record(transaction, **fields)` to instantiate a new object within an active transaction. After setting any necessary fields, call `.save()` on the instance to insert it into the database.
  * *Read:* Fetch data by writing SQL queries with `transaction.query()`, or use the convenience class method `YourModel.get_by_pk_value(transaction, primary_key)` to retrieve a single record by primary key.
  * *Update:* Modify the attributes of an object and call its `.save()` method to update the corresponding row in the database. UniQuery will perform an INSERT or UPDATE as appropriate (it handles upserts where supported).
  * *Delete:* Call an object's `.delete_record()` method to remove that record from the database. This will execute a DELETE statement for you under the hood.

* **Convenient Query Results:** The `QueryResult` object holds results for each table involved in a query and enables easy access via lists and dictionaries. For example, after a join query, you can access `query_result.TableA` (list of TableA objects) and `query_result.TableB` (list of TableB objects), and also use `query_result.TableA_dict[id_value]` to get a specific object by its primary key.

* **Logging and Debugging:** If you enable SQL logging (by passing `log_sql=True` when creating a session), UniQuery will print out the SQL commands being executed. These statements are fully rendered with parameters already applied, making them immediately executable in your SQL shell or database console. This is especially useful for debugging, profiling, or understanding how your queries are constructed and executed.

* **Transaction Success Exceptions:** In some frameworks like **CherryPy**, control flow exceptions such as `HTTPRedirect` are used to indicate success. You can configure UniQuery to treat these as successful — not causing a rollback — by passing them to `succeed_exceptions`.
  ```python
  from cherrypy import HTTPRedirect
  
  with session.transaction(succeed_exceptions=(HTTPRedirect,)) as tr:
      ...
      raise HTTPRedirect("/done")  # This will NOT cause a rollback
  ```

#### Query Requirements and Limitations

When using `Transaction.query`, the SQL query must return columns from one or more actual tables in the schema. If you explicitly list columns for a table (instead of using `SELECT *`), you must include that table’s primary key among the returned columns so that UniQuery can map the results correctly. Queries must not return calculated or derived columns (e.g., expressions, functions, or aliases not tied directly to schema fields). If you need to access all result columns — including calculated values — use the `execute` method with `get_dicts=True` instead.

_**TODO**: Add support for calculated columns and aggregate queries in `Transaction.query`._

### Architecture Details
UniQuery’s architecture is built around a set of core classes that abstract database operations and mapping:

* **Model Classes:** For each database table, UniQuery uses a generated subclass of `UniQueryModel` to represent that table. These classes contain a nested `Meta` class with metadata about the table (like `table_name`, `primary_key`, list of `columns`, and relationship mappings). The fields of the table and relationships to other tables are represented as class attributes (for type hinting) and are populated at runtime. Each model class also gets utility methods like `create_record` and `delete_record` injected during generation.

* **Session and Transaction:** A `UniQuerySession` represents a connection to the database. There are separate session classes for each supported DB dialect (e.g., `uniquery_sqlite.UniQuerySession` and `uniquery_postgres.UniQuerySession`), both inheriting from a common `UniQuerySessionBase`. The session is used as a context manager to open/close the database connection. Within a session, you create a `Transaction` (also database-specific subclass) which manages a database cursor and the scope for executing queries or modifications. The transaction implements context manager methods `__enter__` and `__exit__` to begin and end (commit/rollback) the transaction. This design abstracts away the differences in how each DB handles transactions (for example, SQLite supports different modes like DEFERRED or IMMEDIATE, while PostgreSQL uses savepoints for nested transactions).
 
  UniQuery automatically handles nested transactions. When you start a new transaction inside an existing one, it creates a savepoint if the database supports it (e.g. SQLite or PostgreSQL). This lets you safely nest logic blocks without worrying about conflicts — inner transactions can roll back independently, while the outer transaction remains open until explicitly committed or aborted.

* **Query Processing:** When you call `transaction.query(query_result, sql, params)`, UniQuery parses the SQL query using the **sqlglot** library to determine which tables are involved and to properly handle SQL parameter placeholders. If UniQuery cannot determine the tables involved in a query (e.g. with UNIONs or CTEs), you can pass an explicit list of model classes using the `models` argument in `transaction.query(...)`. The order of models must match the order of columns returned by the SQL query. If there’s a mismatch, UniQuery will raise a `WrongNumberOfColumnsInQuery` exception.
 It then executes the query and iterates over the returned rows. For each table in the query, a `UniQueryTable` object is created internally to accumulate results. Each row from the cursor is processed and turned into model instances for the respective tables using the schema information from `db_config`. UniQuery sets each field value on these instances and then establishes relations:

  * One-to-many and many-to-one links are set by linking objects via their foreign keys (e.g., setting the `.customer` reference on an Order, and adding that Order to the `.orders` list on the corresponding Customer).
  * Many-to-many relationships are resolved by recognizing link tables and connecting the objects on either side (e.g., adding a `Car` to a `Client`’s `.cars` list and vice versa for a `clients_cars` association table).
    After mapping, the `QueryResult` object is populated with lists and dictionaries of the results for each table. All these steps happen behind the scenes in the `Transaction.query()` implementation.

* **Data Persistence:** The model instances carry a reference to the transaction they belong to (through their internal `_table` object). When you call `instance.save()`, UniQuery determines whether to perform an INSERT or an UPDATE. If the object is new (its primary key attribute was `None` before save), an INSERT is issued, and UniQuery captures the last inserted ID (if autoincrement) to update the object. If the object already exists (primary key is set), it performs an upsert (INSERT ... ON CONFLICT DO UPDATE for PostgreSQL or a REPLACE/UPDATE equivalent for SQLite). The deletion via `delete_record()` simply executes a DELETE statement for that primary key. By abstracting this logic, UniQuery allows you to persist changes to objects without writing SQL manually.

## License

Licensed under the [MIT License](./LICENSE).
