# Rawl

An odd raw sql abstraction library.  It might suck.

## Usage

TBD

## Testing

Install the dependencies.

    pip install -r requirements.dev.txt

Run pytest with the following environmental variables.

 - `PG_DSN` - Database connection string for a non-test database.  If a single instance install, database `postgres` will do.
 - `RAWL_DSN` - Connection string for the test database.  **It will be dropped and recreated.**

The `-v` switch is for logging verbosity.  Add more `v`'s for a lower log level.  For example:

    PG_DSN="postgresql://myUser:myPassword@db.example.com:5432/postgres" RAWL_DSN="postgresql://myUser:myPassword@db.example.com:5432/rawl_test" pytest -s -vvvv