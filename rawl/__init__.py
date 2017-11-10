import logging
from abc import ABC
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool

log = logging.getLogger(__name__)


class RawlException(Exception): pass


class RawlConnection(object):
    """ Connection handling for rawl """

    def __init__(self, dsn_string):

        log.debug("Connection init")

        self.dsn = dsn_string
        self.pool = ThreadedConnectionPool(1, 25, self.dsn)

    def __enter__(self):
        try: 
            
            log.info("Connecting to %s" % self.dsn)

            conn = self.pool.getconn() 
            return conn 

        finally: 
            self.pool.putconn(conn)

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

    def _assemble(self, sql_str, **kwargs):
        """ Format the provided SQL """
        # TODO: WTF, design?
        self.query_string = sql.SQL(sql_str).format(
            sql.SQL(', ').join([sql.Identifier(x) for x in args[0]]),
            *[sql.Literal(a) for a in args[1:]]
            )
        return self.query_string

    def _execute(self, query=None):
        if not self.query_string and not query:
            raise RawlException("No query to execute")

        result = []

        with RawlConnection(self.dsn) as conn:

            curs = conn.cursor()

            log.debug("Executing: %s" % query.as_string(curs) or self.query_string.as_string(curs))

            curs.execute(query or self.query_string)

            result = curs.fetchall()

        return result

    def get(self, id):
        """ Retreive a single record """
        raise NotImplementedError("Method get was not implemented")

    def all(self):
        """ Get all records """
        raise NotImplementedError("Method all was not implemented")
