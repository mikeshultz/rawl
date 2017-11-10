# Rawl

An odd raw sql abstraction library.  It might suck.

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

These are also available, though not especially inteded to be used by the user 
unless necessary.

 - `_process_columns` - Converts an iterable to a list of strings that represent
    column names.
 - `_assemble_select` - Put together a compiled Psycopg2 SQL SELECT from an SQL
    string template and query parameters.
 - `_assemble_simple` - Put together a compiled Psycopg2 SQL statement from an 
    SQL string template and query parameters.
 - `_execute` - Executes an assembled SQL query

Here's a very simple example of a model:

    class StateColumns(IntEnum):
        state_id = 0
        name = 1


    class StateModel(RawlBase):
        def __init__(self, dsn):
            # Generate column list from the Enum
            columns = [str(col).split('.')[1] for col in StateColumns]

        def all(self):
            """ Return all state records """
            return self.select("SELECT {0} FROM state;", ['name'])

        def get(self, pk):
            """ Return all state records """
            return self.select("SELECT {0} FROM state WHERE state_id = %s;", ['name'], pk)

    if __name__ == "__main__":
        states = StateModel("postgresql://myUser:myPass@myserver.example.com/my_db")
        for state in states.all():
            print(state[StateColumns.name])

## Testing

Install the dependencies.

    pip install -r requirements.dev.txt

Run pytest with the following environmental variables.

 - `PG_DSN` - Database connection string for a non-test database.  If a single instance install, database `postgres` will do.
 - `RAWL_DSN` - Connection string for the test database.  **It will be dropped and recreated.**

The `-v` switch is for logging verbosity.  Add more `v`'s for a lower log level.  For example:

    PG_DSN="postgresql://myUser:myPassword@db.example.com:5432/postgres" RAWL_DSN="postgresql://myUser:myPassword@db.example.com:5432/rawl_test" pytest -s -vvvv