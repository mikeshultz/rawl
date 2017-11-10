import logging
import random
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
    """ Connection handling for rawl """

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
        self._process_columns(columns)

    def _process_columns(self, columns):
        if type(columns) == list:
            self.columns = columns
        elif type(columns) == str:
            self.columns = [c.strip() for c in columns.split()]
        else:
            raise RawlException("Unknown format for columns")

    def _assemble_select(self, sql_str, columns, *args, **kwargs):
        """ For mat a select statement with specific columns """
        
        query_string = sql.SQL(sql_str).format(
            sql.SQL(', ').join([sql.Identifier(x) for x in columns]),
            *[sql.Literal(a) for a in args]
            )
        
        return query_string

    def _assemble_simple(self, sql_str, *args, **kwargs):
        """ Format the provided SQL """
        
        query_string = sql.SQL(sql_str).format(
            *[sql.Literal(a) for a in args]
            )

        return query_string

    def _execute(self, query, commit=False):
        """ Execute a query with provided parameters 

            Parameters
            query - SQL string with parameter placeholders
            commit - If True, the query will commit
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

    def get(self, id):
        """ Retreive a single record """
        raise NotImplementedError("Method get was not implemented")

    def all(self):
        """ Get all records """
        raise NotImplementedError("Method all was not implemented")
