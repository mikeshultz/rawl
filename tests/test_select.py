import os
import pytest
import logging
from enum import IntEnum
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from rawl import RawlBase, RawlConnection

log = logging.getLogger(__name__)

DROP_TEST_DB = "DROP DATABASE IF EXISTS rawl_test";
CREATE_TEST_DB = "CREATE DATABASE rawl_test;"
DB_SCHEMA = """
CREATE TABLE rawl (
    rawl_id serial NOT NULL,
    stamp timestamp NOT NULL default now(),
    name varchar
);
INSERT INTO rawl (name) values ('I am row one.');
INSERT INTO rawl (name) values ('I am row two.');
INSERT INTO rawl (name) values ('I am row three.');
INSERT INTO rawl (name) values ('I am row four.');
"""

@pytest.fixture(scope="module")
def pgdb():
    pgconn = connect(os.environ.get('PG_DSN', 'psql://localhost:5432/postgres'))
    pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pgconn.cursor()
    cur.execute(DROP_TEST_DB)
    cur.execute(CREATE_TEST_DB)
    pgconn.commit()
    
    cur.close()
    
    rawlconn = connect(os.environ.get('RAWL_DSN', 'psql://localhost:5432/rawl_test'))

    cur = rawlconn.cursor()
    cur.execute(DB_SCHEMA)
    rawlconn.commit()
    return pgconn


class TheCols(IntEnum):
    rawl_id = 0
    stamp = 1
    name = 2


# Test rawl query
class TheModel(RawlBase):
    def __init__(self, dsn):
        # Generate column list from the Enum
        columns = [str(col).split('.')[1] for col in TheCols]
        
        log.debug("columns: %s" % columns)
        
        # Init the parent
        super(TheModel, self).__init__(dsn, columns=columns)

    def get_rawls(self):
        """ Retelfieurn the rawls from the rawl table """

        sql = self._assemble(
            "SELECT {0}"
            "FROM rawl;", 
            self.columns)

        res = self._execute(sql)

        return res


class TestRawl(object):

    def test_get_rawls(self, pgdb):
        """ Test out a basic SELECT statement """

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        statement = mod.get_rawls()
        
        log.debug(statement)

        assert 'I am row one.' in statement[0]