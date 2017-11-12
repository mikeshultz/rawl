import os
import pytest
import logging
import pickle
from enum import IntEnum
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from rawl import RawlBase, RawlConnection, RawlException, RawlResult

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
        super(TheModel, self).__init__(dsn, columns=columns, table_name='rawl')

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

    def delete_rawl_without_commit(self, rawl_id):
        """ Test a delete """

        return self.query("DELETE FROM rawl WHERE rawl_id={0};", rawl_id, commit=False)


class TestRawl(object):

    @pytest.mark.dependency()
    def test_all(self, pgdb):
        """ Test out a basic SELECT statement """

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        result = mod.all()
        
        log.debug(result)

        assert result is not None
        assert type(result) == list
        assert type(result[0]) == RawlResult
        assert len(result[0]) == 3
        assert 'rawl_id' in result[0].keys()
        assert 'I am row one.' in result[0]

    @pytest.mark.dependency()
    def test_get_single_rawl(self, pgdb):
        """ Test a SELECT WHERE """

        RAWL_ID = 2

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        result = mod.get(RAWL_ID)

        assert result is not None
        assert type(result) == RawlResult
        assert result[TheCols.name] == 'I am row two.'
        assert result.rawl_id == RAWL_ID
        assert result['rawl_id'] == RAWL_ID
        assert result[0] == RAWL_ID

    @pytest.mark.dependency(depends=['test_all', 'test_get_single_rawl'])
    def test_delete_rawl(self, pgdb):
        """ Test a DELETE """

        RAWL_ID = 2

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        mod.delete_rawl(RAWL_ID)
        result = mod.get(RAWL_ID)

        assert result is None

    @pytest.mark.dependency(depends=['test_all', 'test_get_single_rawl'])
    def test_rollback_without_commit(self, pgdb):
        """ Test a DELETE without a commit """

        RAWL_ID = 3

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        mod.delete_rawl_without_commit(RAWL_ID)

        result = mod.get(RAWL_ID)

        assert result is not None

    @pytest.mark.dependency(depends=['test_all', 'test_get_single_rawl'])
    def test_access_invalid_attribute(self, pgdb):
        """ 
        Test that an invalid attribute on the result object throws an 
        exception.
        """

        RAWL_ID = 3

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        result = mod.get(RAWL_ID)

        try:
            print(result.invalidAttr)
            assert False
        except AttributeError:
            log.exception("Invalid attr as expected")
            assert True

    @pytest.mark.dependency(depends=['test_all', 'test_get_single_rawl'])
    def test_access_invalid_index(self, pgdb):
        """ 
        Test that an invalid column index(in bytes string form) on the result 
        object throws an exception.

        Notes
        -----
        Edge case, but it's there in the code.
        """

        RAWL_ID = 3

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        result = mod.get(RAWL_ID)

        try:
            print(result[b"72"])
            assert False
        except IndexError as e:
            assert "Unknown index value" in str(e)

    @pytest.mark.dependency(depends=['test_all', 'test_get_single_rawl'])
    def test_serialization(self, pgdb):
        """ 
        Test that a RawlResult object can be serialized properly.
        """

        RAWL_ID = 1

        mod = TheModel(os.environ.get('RAWL_DSN', 'postgresql://localhost:5432/rawl_test'))

        result = mod.get(RAWL_ID)

        # Test it can be pickled
        try:
            pickled_result = pickle.dumps(result)
            assert True
        except pickle.PicklingError:
            assert False

        # Test that it can return dict
        assert type(result.to_dict()) == dict

        # Test that it can return list
        assert type(result.to_list()) == list

        # Test that it can be unpickled
        assert type(pickle.loads(pickled_result)) == RawlResult