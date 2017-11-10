import logging
import random
from enum import IntEnum
from abc import ABC
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import (
    ISOLATION_LEVEL_READ_COMMITTED, 
    TRANSACTION_STATUS_INTRANS
)

log = logging.getLogger(__name__)


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

        cursor = connection.cursor()
        cursor.execute("SELECT * from my_table;")
        results = cursor.fetchall()
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

    def __init__(self, data_dict):
        self.data = data_dict

    def __getattribute__(self, name):
        if hasattr(self, name):
            return object.__getattribute__(self, name)
        else:
            try:
                return data['name']
            except KeyError:
                raise AttributeError("%s is not available")

    def set(self, col, rowdata):
        """ Set the row data """
        
        self.data[col] = rowdata


class RawlBase(ABC):
    """ And abstract class for creating models out of raw SQL queries """

    def __init__(self, dsn, columns):
        self.dsn = dsn
        self.columns = []
        self.process_columns(columns)

    def _assemble_select(self, sql_str, columns, *args, **kwargs):
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

    def _execute(self, query, commit=False):
        """ 
        Execute a query with provided parameters 

        Parameters
        :query:     SQL string with parameter placeholders
        :commit:    If True, the query will commit
        :returns:   List of rows
        """

        result = []

        with RawlConnection(self.dsn) as conn:
            query_id = random.randrange(9999)

            curs = conn.cursor()
            curs.execute(query)

            log.debug("Executing(%s): %s" % (query_id, query.as_string(curs)))
            if commit == True:
                log.debug("COMMIT(%s)" % query_id)
                conn.commit()
            
            if curs.rowcount > 0:
                result = curs.fetchall()
            
            curs.close()
            
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
        commit=None
        if kwargs.get('commit') is not None:
            commit = kwargs.pop('commit')
        query = self._assemble_simple(sql_string, *args, **kwargs)
        return self._execute(query, commit=commit)

    def select(self, sql_string, columns, *args, **kwargs):
        """ 
        Execute a SELECT statement 

        :sql_string:    An SQL string template
        :columns:       A list of columns to be returned by the query
        :*args:         Arguments to be passed for query parameters.
        :returns:       Psycopg2 result
        """
        query = self._assemble_select(sql_string, columns, *args, *kwargs)
        return self._execute(query)

    def get(self, pk):
        """ 
        Retreive a single record from the table.  Should be implemented but not
        required.

        :pk:            The primary key ID for the record
        :returns:       List of single result
        """
        raise NotImplementedError("Method get was not implemented")

    def all(self):
        """ 
        Retreive all single record from the table.  Should be implemented but not
        required.
        :returns:       List of results
        """
        raise NotImplementedError("Method all was not implemented")
