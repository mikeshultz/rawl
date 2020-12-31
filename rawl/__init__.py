# -*- coding: utf-8 -*-
""" Rawl
https://github.com/mikeshultz/rawl

This module is a simple database abstraction trying to balance the usefulness of
an ORM with the lack of constraints and flexibility of rawl SQL.

Note:
    This is not an ORM, nor intended to hide the database. It's more or less a
    wrapper around psycopg2. It will not create the database for you, either.
    Nor should it! Proper database design can not be abstracted away. That said,
    with some care you can execute a set of queries to create your schema if
    needed. See the tests for an example.

Example:
    from rawl import RawlBase

    DSN = "postgresql://myUser:myPass@myserver.example.com/my_db"


    class StateModel(RawlBase):
        def __init__(self):
            # Init the parent
            super(StateModel, self).__init__(DSN, table_name='state', 
                columns=['state_id', 'name'])

        def get_name(self, pk):
            ''' My special method returning only a name for a state '''

            result = self.select("SELECT {0} FROM state WHERE state_id = %s;", 
                self.columns, pk)

            # Return first row with all columns
            return result[0].name

    if __name__ == "__main__":
        for state in StateModel().all(): 
            print(state.name)

License:
    Copyright (C) 2017 Mike Shultz

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import logging
import random
import warnings
from enum import IntEnum
from abc import ABC
from json import JSONEncoder
from datetime import datetime
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import (
    STATUS_IN_TRANSACTION,
    STATUS_BEGIN,
    STATUS_PREPARED,
    ISOLATION_LEVEL_READ_COMMITTED,
)

OPEN_TRANSACTION_STATES = (STATUS_IN_TRANSACTION, STATUS_BEGIN, STATUS_PREPARED)
POOL_MIN_CONN = 1
POOL_MAX_CONN = 25

log = logging.getLogger("rawl")


def pop_or_none(d, k):
    """ Pop a value from a dict or return None if not exists """
    try:
        return d.pop(k)
    except KeyError:
        return None


class RawlException(Exception):
    pass


class RawlConnection(object):
    """
    Connection handling for rawl

    Usage
    -----
    with RawlConnection("postgresql://user:pass@server/db") as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * from my_table;")
        results = cursor.fetchall()
    """

    pool = None

    def __init__(self, dsn_string, close_on_exit=True):

        log.debug("Connection init")

        self.dsn = dsn_string
        self.close_on_exit = close_on_exit

        # Create the pool if it doesn't exist already
        if RawlConnection.pool is None:
            RawlConnection.pool = ThreadedConnectionPool(
                POOL_MIN_CONN, POOL_MAX_CONN, self.dsn
            )
            log.debug("Created connection pool ({})".format(id(RawlConnection.pool)))
        else:
            log.debug("Reusing connection pool ({})".format(id(RawlConnection.pool)))

    def __enter__(self):
        conn = None

        try:
            conn = self.get_conn()
            return conn

        except Exception:
            log.exception("Connection failure")

        finally:
            self.put_conn(conn)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            self.entrance = False
        return True

    def get_conn(self):
        log.debug("Retrieving connection from pool for %s" % self.dsn)

        conn = RawlConnection.pool.getconn()
        if conn.status not in OPEN_TRANSACTION_STATES:
            conn.set_session(isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
        return conn

    def put_conn(self, conn):
        if self.close_on_exit:
            # Assume rolled back if uncommitted
            if conn.status in OPEN_TRANSACTION_STATES:
                conn.rollback()

            RawlConnection.pool.putconn(conn)


class RawlResult(object):
    """ Represents a row of results retreived from the DB """

    def __init__(self, columns, data_dict):
        self._data = data_dict
        self.columns = columns

    def __str__(self):
        return str(self._data)

    def __getattribute__(self, name):
        # Try for the local objects actual attributes first
        try:
            return object.__getattribute__(self, name)

        # Then resort to the data dict
        except AttributeError:

            if name in self._data:
                return self._data[name]
            else:
                raise AttributeError("%s is not available" % name)

    def __getstate__(self):
        return self._data

    def __setstate__(self, state):
        self._data = state

    def __getitem__(self, k):
        # If it's an int, use the int to lookup a column in the position of the
        # sequence provided.
        if type(k) == int:
            return dict.__getitem__(self._data, self.columns[k])
        # If it's a string, it's a dict lookup
        elif type(k) == str:
            return dict.__getitem__(self._data, k)
        # Anything else and we have no idea how to handle it.
        else:
            int_k = None
            try:
                int_k = int(k)
                return dict.__getitem__(self._data, self.columns[int_k])
            except IndexError:
                raise IndexError("Unknown index value %s" % k)

    def __setitem__(self, k, v):
        # If it's an int, use the int to lookup a column in the position of the
        # sequence provided.
        if type(k) == int:
            return dict.__setitem__(self._data, self.columns[k], v)
        # If it's a string, it's a dict lookup
        elif type(k) == str:
            return dict.__setitem__(self._data, k, v)
        # Anything else and we have no idea how to handle it.
        else:
            int_k = None
            try:
                int_k = int(k)
                return dict.__setitem__(self._data, self.columns[int_k], v)
            except IndexError:
                raise IndexError("Unknown index value %s" % k)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        things = self._data.values()
        for x in things:
            yield x

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def to_dict(self):
        return self._data

    def to_list(self):
        return list(self.values())


class RawlBase(ABC):
    """ And abstract class for creating models out of raw SQL queries """

    def __init__(self, dsn, columns, table_name, pk_name=None):
        self.dsn = dsn
        self.table = table_name
        self.columns = []
        self._connection_manager = RawlConnection(dsn)
        self._open_transaction = None
        self._open_cursor = None

        # Process the provided columns into a list
        self.process_columns(columns)

        # Use primary key provided
        if pk_name is not None:
            self.pk = pk_name
        # Otherwise, assume first column
        else:
            self.pk = columns[0]

    def _assemble_with_columns(self, sql_str, columns, *args, **kwargs):
        """
        Format a select statement with specific columns

        :sql_str:   An SQL string template
        :columns:   The columns to be selected and put into {0}
        :*args:     Arguments to use as query parameters.
        :returns:   Psycopg2 compiled query
        """

        # Handle any aliased columns we get (e.g. table_alias.column)
        qcols = []
        for col in columns:
            if "." in col:
                # Explodeded it
                wlist = col.split(".")

                # Reassemble into string and drop it into the list
                qcols.append(sql.SQL(".").join([sql.Identifier(x) for x in wlist]))
            else:
                qcols.append(sql.Identifier(col))

        query_string = sql.SQL(sql_str).format(
            sql.SQL(", ").join(qcols), *[sql.Literal(a) for a in args]
        )

        return query_string

    def _assemble_select(self, sql_str, columns, *args, **kwargs):
        """Alias for _assemble_with_columns"""
        warnings.warn(
            "_assemble_select has been depreciated for _assemble_with_columns. It will be removed in a future version.",
            DeprecationWarning,
        )
        return self._assemble_with_columns(sql_str, columns, *args, **kwargs)

    def _assemble_simple(self, sql_str, *args, **kwargs):
        """
        Format a select statement with specific columns

        :sql_str:   An SQL string template
        :*args:     Arguments to use as query parameters.
        :returns:   Psycopg2 compiled query
        """

        query_string = sql.SQL(sql_str).format(*[sql.Literal(a) for a in args])

        return query_string

    def _execute(self, query, commit=True, working_columns=None, read_only=False):
        """
        Execute a query with provided parameters

        Parameters
        :query:     SQL string with parameter placeholders
        :commit:    If True, the query will commit
        :returns:   List of rows
        """

        log.debug("RawlBase._execute()")

        result = []

        if working_columns is None:
            working_columns = self.columns

        conn = None
        if self._open_transaction:
            conn = self._open_transaction
        else:
            conn = self._connection_manager.get_conn()

        query_id = random.randrange(9999)

        curs = None
        if self._open_transaction:
            if not self._open_cursor:
                self._open_cursor = conn.cursor()
            curs = self._open_cursor
        else:
            curs = conn.cursor()

        try:
            log.debug("Executing(%s): %s" % (query_id, query.as_string(curs)))
        except Exception:
            log.exception("LOGGING EXCEPTION LOL")

        curs.execute(query)

        log.debug("Executed")

        if commit:
            log.debug("AUTOCOMMIT(%s)" % query_id)
            conn.commit()

        log.debug("curs.rowcount: %s" % curs.rowcount)

        """ According to the docs, curs.description "is None for operations
        that do not return rows"

        https://www.psycopg.org/docs/cursor.html#cursor.description
        """
        if curs.rowcount > 0 and curs.description is not None:
            # Process the results into a dict and stuff it in a RawlResult
            # object.  Then append that object to result
            for row in curs.fetchall():
                row_dict = {}
                for i, col in enumerate(working_columns):
                    try:
                        # For aliased columns, we need to get rid of the dot
                        col = col.replace(".", "_")
                        row_dict[col] = row[i]
                    except IndexError:
                        pass

                log.debug("Appending dict to result: %s" % row_dict)

                rr = RawlResult(working_columns, row_dict)
                result.append(rr)

        if not self._open_cursor:
            log.debug("Closing cursor")
            curs.close()

        if not self._open_transaction:
            log.debug("put_conn({})".format(id(conn)))
            self._connection_manager.put_conn(conn)

        return result

    def process_columns(self, columns):
        """
        Handle provided columns and if necessary, convert columns to a list for
        internal strage.

        :columns: A sequence of columns for the table. Can be list, comma
            -delimited string, or IntEnum.
        """
        if type(columns) == list:
            self.columns = columns
        elif type(columns) == str:
            self.columns = [c.strip() for c in columns.split()]
        elif type(columns) == IntEnum:
            self.columns = [str(c) for c in columns]
        else:
            raise RawlException("Unknown format for columns")

    def query(self, sql_string, *args, **kwargs):
        """
        Execute a DML query

        :sql_string:    An SQL string template
        :*args:         Arguments to be passed for query parameters.
        :commit:        Whether or not to commit the transaction after the query
        :returns:       Psycopg2 result
        """
        commit = pop_or_none(kwargs, "commit")
        columns = pop_or_none(kwargs, "columns")

        if commit is None and self._open_transaction is not None:
            commit = False

        query = self._assemble_simple(sql_string, *args, **kwargs)
        return self._execute(query, commit=commit, working_columns=columns)

    def select(self, sql_string, cols, *args, **kwargs):
        """
        Execute a SELECT statement

        :sql_string:    An SQL string template
        :columns:       A list of columns to be returned by the query
        :*args:         Arguments to be passed for query parameters.
        :returns:       Psycopg2 result
        """

        commit = pop_or_none(kwargs, "commit")
        working_columns = pop_or_none(kwargs, "columns")

        if commit is None and self._open_transaction is not None:
            commit = False

        query = self._assemble_with_columns(sql_string, cols, *args, *kwargs)

        return self._execute(query, working_columns=working_columns, commit=commit)

    def insert_dict(self, value_dict, commit=True):
        """
        Execute an INSERT statement using a python dict

        :value_dict:    A dictionary representing all the columns(keys) and
            values that should be part of the INSERT statement
        :commit:        Whether to automatically commit the transaction
        :returns:       Psycopg2 result
        """

        # Sanity check the value_dict
        for key in value_dict.keys():
            if key not in self.columns:
                raise ValueError("Column %s does not exist" % key)

        # These lists will make up the columns and values of the INSERT
        insert_cols = []
        value_set = []

        # Go through all the possible columns and look for that column in the
        # dict.  If available, we need to add it to our col/val sets
        for col in self.columns:
            if col in value_dict:
                # log.debug("Inserting with column %s" % col)
                insert_cols.append(col)
                value_set.append(value_dict[col])

        # Create SQL statement placeholders for the dynamic values
        placeholders = ", ".join(["{%s}" % x for x in range(1, len(value_set) + 1)])

        # TODO: Maybe don't trust table_name ane pk_name?  Shouldn't really be
        # user input, but who knows.
        query = self._assemble_with_columns(
            '''
            INSERT INTO "'''
            + self.table
            + """" ({0}) 
            VALUES ("""
            + placeholders
            + """) 
            RETURNING """
            + self.pk
            + """
            """,
            insert_cols,
            *value_set
        )

        result = self._execute(query, commit=commit)

        # Inca
        if len(result) > 0:
            # Return the pk if we can
            if hasattr(result[0], self.pk):
                return getattr(result[0], self.pk)
            # Otherwise, the full result
            else:
                return result[0]
        else:
            return None

    def get(self, pk):
        """
        Retreive a single record from the table.  Lots of reasons this might be
        best implemented in the model

        :pk:            The primary key ID for the record
        :returns:       List of single result
        """

        if type(pk) == str:
            # Probably an int, give it a shot
            try:
                pk = int(pk)
            except ValueError:
                pass

        return self.select(
            "SELECT {0} FROM " + self.table + " WHERE " + self.pk + " = {1};",
            self.columns,
            pk,
        )

    def all(self):
        """
        Retreive all single record from the table.  Should be implemented but not
        required.
        :returns:       List of results
        """

        return self.select("SELECT {0} FROM " + self.table + ";", self.columns)

    def start_transaction(self):
        """
        Initiate a connection  to use as a transaction
        """
        self._open_transaction = self._connection_manager.get_conn()
        return self._open_transaction

    def rollback(self):
        """
        Initiate a connection  to use as a transaction
        """
        if self._open_transaction:
            log.debug("rollback()")

            if self._open_cursor:
                self._open_cursor.close()

            self._open_transaction.rollback()
            self._connection_manager.put_conn(self._open_transaction)

            self._open_cursor = None
            self._open_transaction = None
        else:
            log.warning("Cannot rollback, no open transaction")

    def commit(self):
        """
        Commit an already open transaction
        """
        if self._open_transaction:
            log.debug("commit()")

            if self._open_cursor:
                self._open_cursor.close()

            self._open_transaction.commit()
            self._connection_manager.put_conn(self._open_transaction)

            self._open_cursor = None
            self._open_transaction = None
        else:
            log.warning("Cannot commit, no open transaction")


class RawlJSONEncoder(JSONEncoder):
    """
    A JSON encoder that can be used with json.dumps

    Usage
    -----
    json.dumps(cls=RawlJSONEncoder)
    """

    def default(self, o):
        if type(o) == datetime:
            return o.isoformat()
        elif type(o) == RawlResult:
            return o.to_dict()
        return super(RawlJSONEncoder, self).default(o)
