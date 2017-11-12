# Rawl
[![Build Status](https://travis-ci.org/mikeshultz/rawl.svg?branch=master)](https://travis-ci.org/mikeshultz/rawl) [![Coverage Status](https://coveralls.io/repos/github/mikeshultz/rawl/badge.svg?branch=master)](https://coveralls.io/github/mikeshultz/rawl?branch=master)

An odd raw sql abstraction library.  It might suck.

**NOTE**: This is not an ORM, nor intended to hide the database.  It's more or 
less a wrapper around [psycopg2](http://initd.org/psycopg/docs/).  It __will 
not__ create the database for you, either.  Nor should it!  __Proper database 
design can not be abstracted away.__  That said, with some care you can execute 
a set of queries to create your schema if needed.  See the tests for an 
example.

## Usage

### Simple Connection

The most rudimentary way to use Rawl is with `RawlConnection`.  It's basically 
just a wrapper for [psycopg2's connection](http://initd.org/psycopg/docs/connection.html).

    with RawlConnection("postgresql://myUser:myPass@db.example.com/my_database") as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * from my_table;")
        results = cursor.fetchall()

This isn't really the useful part of Rawl, so perhaps you'd rather build data
controllers or models.

### Models

Create model classes that derrive from `RawlBase`.  `RawlBase` provides some 
useful methods: 
 
 - `query` - Executes a query from provided SQL string template and parameters
 - `select` - Executes a query from provided SQL string template, columns, and 
    parameters
 - `process_columns` - Converts an iterable to a list of strings that represent
    column names.

These are also available, though not especially inteded to be used by the user 
unless necessary.
 - `_assemble_select` - Put together a compiled Psycopg2 SQL SELECT from an SQL
    string template and query parameters.
 - `_assemble_simple` - Put together a compiled Psycopg2 SQL statement from an 
    SQL string template and query parameters.
 - `_execute` - Executes an assembled SQL query

Here's a very simple example of a model:

    from enum import IntEnum
    from rawl import RawlBase


    class StateColumns(IntEnum):
        state_id = 0
        name = 1


    class StateModel(RawlBase):
        def __init__(self, dsn):
            # Generate column list from the Enum
            columns = [str(col).split('.')[1] for col in StateColumns]

            # Init the parent
            super(TheModel, self).__init__(dsn, columns=columns, table_name='state')

            # Do your own init stuff
            my_init_stuff()

        def all(self):
            """ Return all state records """
            return self.select("SELECT {0} FROM state;", self.columns)

        def get(self, pk):
            """ Return all state records """
            return self.select("SELECT {0} FROM state WHERE state_id = %s;", self.columns, pk)

        def get_name(self, pk):
            """ Return all state records """
            return self.select("SELECT {0} FROM state WHERE state_id = %s;", ['name'], pk)

    if __name__ == "__main__":
        states = StateModel("postgresql://myUser:myPass@myserver.example.com/my_db")
        for state in states.all():
            # Print using the Enum
            print(state[StateColumns.name])
            # Or an attribute
            print(state.state_id)

And of course you can add your own methods for various specialty queries or 
anything you want.

### Results

Unless otherwise altered by the implemented methods, results returned from any 
query are always a list of `RawlResult` objects.  You can treat these as if 
they were `dict`s, `object`s, or `list`s.  For instance:

    results = StateModel().all()
    for row in results:
        
        # These are all the same
        print(row.state_id)
        print(row['state_id'])
        print(row[0])

        # Or iterate through the columns
        for col in row:
            print(col)

`RawResult` should be suitable for serialization(pickling), but should you need 
to convert to a python type, use `RawResult.to_dict()` or `RawResult.to_list()`.

## Testing

Install the dependencies.

    pip install -r requirements.dev.txt

Run pytest with the following environmental variables.

 - `PG_DSN` - Database connection string for a non-test database.  If a single instance install, database `postgres` will do.
 - `RAWL_DSN` - Connection string for the test database.  **It will be dropped and recreated.**

The `-v` switch is for logging verbosity.  Add more `v`'s for a lower log level.  For example:

    PG_DSN="postgresql://myUser:myPassword@db.example.com:5432/postgres" RAWL_DSN="postgresql://myUser:myPassword@db.example.com:5432/rawl_test" pytest -s -vvvv