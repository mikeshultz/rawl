import logging
import random
import warnings
from enum import IntEnum
from abc import ABC
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import (
    ISOLATION_LEVEL_READ_COMMITTED, 
    TRANSACTION_STATUS_INTRANS
)

log = logging.getLogger('rawl')


class RawlException(Exception): pass


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

    def __init__(self, dsn_string):

        log.debug("Connection init")

        self.dsn = dsn_string
        self.pool = ThreadedConnectionPool(1, 25, self.dsn)

        self.conn = None

    def __enter__(self):
        try: 
            
            log.info("Connecting to %s" % self.dsn)

            self.conn = self.pool.getconn()
            self.conn.set_session(isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
            return self.conn

        except Exception:
            log.exception("Connection failure")

        finally: 
            # Assume rolled back if uncommitted
            if self.conn.get_transaction_status() == TRANSACTION_STATUS_INTRANS:
                self.conn.rollback()
            self.pool.putconn(self.conn)
            self.conn = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            self.entrance = False
        return True


class RawlResult(object):
    """ Represents a row of results retreived from the DB """

    def __init__(self, columns, data_dict):
        self._data = data_dict
        self.columns = columns

    def __getattribute__(self, name):
        # Try for the local objects actual attributes first
        try:
            return object.__getattribute__(self, name)

        # Then resort to the data dict
        except AttributeError:
            
            if name in self._data:
                return self._data[name]
            else:
                raise AttributeError("%s is not available")

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

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        #log.debug(self.data)
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
        
        query_string = sql.SQL(sql_str).format(
            sql.SQL(', ').join([sql.Identifier(x) for x in columns]),
            *[sql.Literal(a) for a in args]
            )
        
        return query_string

    def _assemble_select(self, sql_str, columns, *args, **kwargs):
        """ Alias for _assemble_with_columns
        """
        warnings.warn("_assemble_select has been depreciated for _assemble_with_columns. It will be removed in a future version.", DeprecationWarning)
        return self._assemble_with_columns(sql_str, columns, *args, **kwargs)

    def _assemble_simple(self, sql_str, *args, **kwargs):
        """ 
        Format a select statement with specific columns 

        :sql_str:   An SQL string template
        :*args:     Arguments to use as query parameters.
        :returns:   Psycopg2 compiled query
        """
        
        query_string = sql.SQL(sql_str).format(
            *[sql.Literal(a) for a in args]
            )

        return query_string

    def _execute(self, query, commit=False, working_columns=None):
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

        with RawlConnection(self.dsn) as conn:

            query_id = random.randrange(9999)

            curs = conn.cursor()

            try:
                log.debug("Executing(%s): %s" % (query_id, query.as_string(curs)))
            except:
                log.exception("LOGGING EXCEPTION LOL")

            curs.execute(query)

            log.debug("Executed")

            if commit == True:
                log.debug("COMMIT(%s)" % query_id)
                conn.commit()
            
            if curs.rowcount > 0:
                #result = curs.fetchall()
                # Process the results into a dict and stuff it in a RawlResult
                # object.  Then append that object to result
                result_rows = curs.fetchall()
                for row in result_rows:
                    log.debug("--row--")
                    i = 0
                    row_dict = {}
                    for col in working_columns:
                        try:
                            #log.debug("row_dict[%s] = row[%s] which is %s" % (col, i, row[i]))
                            row_dict[col] = row[i]
                        except IndexError: pass
                        i += 1
                    log.debug("Appending dict to result: %s" % row_dict)
                    rr = RawlResult(working_columns, row_dict)
                    result.append(rr)
            
            curs.close()
        #log.debug("Returning results: %s" % result)
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
        commit = None
        columns = None
        if kwargs.get('commit') is not None:
            commit = kwargs.pop('commit')
        if kwargs.get('columns') is not None:
            columns = kwargs.pop('columns')
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
        working_columns = None
        if kwargs.get('columns') is not None:
            working_columns = kwargs.pop('columns')
        query = self._assemble_select(sql_string, cols, *args, *kwargs)
        return self._execute(query, working_columns=working_columns)

    def insert_dict(self, value_dict, commit=False):
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
                #log.debug("Inserting with column %s" % col)
                insert_cols.append(col)
                value_set.append(value_dict[col])

        # Create SQL statement placeholders for the dynamic values
        placeholders = ', '.join(["{%s}" % x for x in range(1, len(value_set) + 1)])

        # TODO: Maybe don't trust table_name ane pk_name?  Shouldn't really be 
        # user input, but who knows.
        query = self._assemble_with_columns('''
            INSERT INTO "''' + self.table + '''" ({0}) 
            VALUES (''' + placeholders + ''') 
            RETURNING ''' + self.pk + '''
            ''', insert_cols, *value_set)

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
            except ValueError: pass

        return self.select(
            "SELECT {0} FROM " + self.table + " WHERE " + self.pk + " = {1};",
            self.columns, pk)

    def all(self):
        """ 
        Retreive all single record from the table.  Should be implemented but not
        required.
        :returns:       List of results
        """

        return self.select("SELECT {0} FROM " + self.table + ";", 
            self.columns)
