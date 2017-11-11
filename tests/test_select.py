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
    pgconn = connect(os.environ.get('PG_DSN', 'postgresql://localhost:5432/postgres'))
    pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pgconn.cursor()
    cur.execute(DROP_TEST_DB)
    cur.execute(CREATE_TEST_DB)
    pgconn.commit()
    
    cur.close()
    
    rawlconn = connect(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

    cur = rawlconn.cursor()
    cur.execute(DB_SCHEMA)
    rawlconn.commit()
    cur.close()
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

    def all(self):
        """ Retelfieurn the rawls from the rawl table """

        res = self.select(
            "SELECT {0}"
            " FROM rawl;", 
            self.columns)

        return res

    def get(self, rawl_id):
        """ Retelfieurn the rawls from the rawl table """

        res = self.select(
            "SELECT {0}"
            " FROM rawl"
            " WHERE rawl_id={1}", 
            self.columns, rawl_id)

        if len(res) > 0:
            return res[0]
        else:
            return None

    def delete_rawl(self, rawl_id):
        """ Test a delete """

        return self.query("DELETE FROM rawl WHERE rawl_id={0};", rawl_id, commit=True)


class TestRawl(object):

    @pytest.mark.dependency()
    def test_all(self, pgdb):
        """ Test out a basic SELECT statement """

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        result = mod.all()
        
        log.debug(result)

        assert 'I am row one.' in result[0]

    @pytest.mark.dependency()
    def test_get_single_rawl(self, pgdb):
        """ Test a SELECT WHERE """

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        result = mod.get(2)

        assert result is not None
        assert result[TheCols.name] == 'I am row two.'

    @pytest.mark.dependency(depends=['test_all', 'test_get_single_rawl'])
    def test_delete_rawl(self, pgdb):
        """ Test a DELETE """

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        mod.delete_rawl(2)
        result = mod.get(2)

        assert result is None