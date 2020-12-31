import os
import pytest
import logging
import pickle
import json
from enum import IntEnum
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from rawl import POOL_MAX_CONN, RawlBase, RawlResult, RawlJSONEncoder

log = logging.getLogger(__name__)

DROP_TEST_DB = "DROP DATABASE IF EXISTS rawl_test;"
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

RAWL_DSN = os.environ.get("RAWL_DSN", "postgresql://localhost:5432/rawl_test")


@pytest.fixture(scope="module")
def pgdb():
    pgconn = connect(os.environ.get("PG_DSN", "postgresql://localhost:5432/postgres"))
    pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pgconn.cursor()
    cur.execute(DROP_TEST_DB)
    cur.execute(CREATE_TEST_DB)
    pgconn.commit()

    cur.close()

    rawlconn = connect(RAWL_DSN)

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
        columns = [str(col).split(".")[1] for col in TheCols]

        log.debug("columns: %s" % columns)

        # Init the parent
        super(TheModel, self).__init__(dsn, columns=columns, table_name="rawl")

    def select_rawls_with_extra_column(self, rawl_id):
        """Return the rawls from the rawl table but in a way to test more stuff"""

        # We're adding an arbitrary column in
        cols = self.columns.copy()
        cols.append("foo")

        res = self.select(
            "SELECT {0}, TRUE" " FROM rawl" " WHERE rawl_id={1}",
            self.columns,
            rawl_id,
            columns=cols,
        )

        if len(res) > 0:
            return res[0]
        else:
            return None

    def query_rawls_with_asterisk(self, rawl_id):
        """Test out self.query directly using columns"""

        res = self.query(
            "SELECT * FROM rawl WHERE rawl_id={0}", rawl_id, columns=self.columns
        )

        if len(res) > 0:
            return res[0]
        else:
            return None

    def delete_rawl(self, rawl_id):
        """ Test a delete """

        return self.query("DELETE FROM rawl WHERE rawl_id={0};", rawl_id, commit=True)

    def delete_rawl_without_commit(self, rawl_id):
        """ Test a delete """

        self.start_transaction()

        result = self.query(
            "DELETE FROM rawl WHERE rawl_id={0};", rawl_id, commit=False
        )

        self.rollback()

        return result


class TestRawl(object):
    @pytest.mark.dependency()
    def test_all(self, pgdb):
        """ Test out a basic SELECT statement """

        mod = TheModel(RAWL_DSN)

        result = mod.all()

        log.debug(result)

        assert result is not None
        assert type(result) == list
        assert type(result[0]) == RawlResult
        assert len(result[0]) == 3
        assert "rawl_id" in result[0].keys()
        assert "I am row one." in result[0]

    @pytest.mark.dependency()
    def test_get_single_rawl(self, pgdb):
        """ Test a SELECT WHERE """

        RAWL_ID = 2

        mod = TheModel(RAWL_DSN)

        result = mod.get(RAWL_ID)[0]

        assert result is not None
        assert type(result) == RawlResult
        assert result[TheCols.name] == "I am row two."
        assert result.rawl_id == RAWL_ID
        assert result["rawl_id"] == RAWL_ID
        assert result[0] == RAWL_ID

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_delete_rawl(self, pgdb):
        """ Test a DELETE """

        RAWL_ID = 2

        mod = TheModel(RAWL_DSN)

        mod.delete_rawl(RAWL_ID)
        result = mod.get(RAWL_ID)

        assert result == []

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_rollback_without_commit(self, pgdb):
        """ Test a DELETE without a commit """

        RAWL_ID = 3

        mod = TheModel(RAWL_DSN)

        mod.delete_rawl_without_commit(RAWL_ID)

        result = mod.get(RAWL_ID)

        assert len(result) > 0

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_access_invalid_attribute(self, pgdb):
        """
        Test that an invalid attribute on the result object throws an
        exception.
        """

        RAWL_ID = 3

        mod = TheModel(RAWL_DSN)

        result = mod.get(RAWL_ID)[0]

        try:
            print(result.invalidAttr)
            assert False
        except AttributeError:
            log.exception("Invalid attr as expected")
            assert True

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_access_invalid_index(self, pgdb):
        """
        Test that an invalid column index(in bytes string form) on the result
        object throws an exception.

        Notes
        -----
        Edge case, but it's there in the code.
        """

        RAWL_ID = 3

        mod = TheModel(RAWL_DSN)

        result = mod.get(RAWL_ID)[0]

        try:
            print(result[b"72"])
            assert False
        except IndexError as e:
            assert "Unknown index value" in str(e)

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_insert_dict(self, pgdb):
        """
        Test that a new rawl entry can be created with insert_dict
        """

        mod = TheModel(RAWL_DSN)

        orig_result = mod.all()
        new_row_id = mod.insert_dict({"name": "Row five is alive!"}, commit=True)
        new_result = mod.all()

        # Test that standard RETURNING is working
        assert new_row_id == 5

        # Make sure the new one is in the results from all()
        assert len(new_result) - len(orig_result) == 1

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_insert_dict_without_commit(self, pgdb):
        """
        Test that multople insert_dicts can happen in one transaction
        """

        mod = TheModel(RAWL_DSN)
        other_mod = TheModel(RAWL_DSN)

        mod.start_transaction()
        conn_id = id(mod._open_transaction)

        orig_result = mod.all()

        assert mod._open_transaction is not None

        new_row_id = mod.insert_dict({"name": "Row six of sticks!"}, commit=False)
        uncommitted_new_result = mod.all()
        new_result = other_mod.all()

        assert conn_id == id(mod._open_transaction)

        # Test that standard RETURNING is working
        assert new_row_id == 6
        assert id(mod._open_transaction) != id(other_mod._open_transaction)
        assert len(new_result) == len(orig_result)
        assert len(uncommitted_new_result) == len(orig_result) + 1

        # Make sure the new one is in the results from all()
        assert len(new_result) == len(orig_result)

        newer_row_id = mod.insert_dict(
            {"name": "Row seven is not eleven"}, commit=False
        )
        uncommitted_newer_result = mod.all()
        newer_result = other_mod.all()

        assert conn_id == id(mod._open_transaction)

        assert newer_row_id == 7
        assert len(newer_result) == len(orig_result)
        assert len(uncommitted_newer_result) == len(orig_result) + 2

        mod.commit()

        assert mod._open_transaction is None

        assert conn_id != id(mod._open_transaction)

        newest_result = mod.all()

        assert len(newest_result) - len(orig_result) == 2

        mod.rollback()

        assert mod._open_transaction is None

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_insert_dict_with_invalid_column(self, pgdb):
        """
        Test case that an insert_dict with an invalid column fails
        """

        mod = TheModel(RAWL_DSN)

        try:
            mod.insert_dict({"not_a_column": "foobar"}, commit=False)
            assert False
        except ValueError:
            assert True

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_serialization(self, pgdb):
        """
        Test that a RawlResult object can be serialized properly.
        """

        RAWL_ID = 1

        mod = TheModel(RAWL_DSN)

        result = mod.get(RAWL_ID)[0]

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

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_json_serialization(self, pgdb):
        """
        Test that a RawlResult object can be serialized properly.
        """

        mod = TheModel(RAWL_DSN)

        result = mod.all()

        try:
            json.dumps(result, cls=RawlJSONEncoder)
            assert True
        except TypeError as e:
            if "serializable" in str(e):
                assert False
            else:
                raise e

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_select_with_columns(self, pgdb):
        """
        Test a case with a select query with different columns that given
        for formatting.
        """

        RAWL_ID = 5

        mod = TheModel(RAWL_DSN)

        result = mod.select_rawls_with_extra_column(RAWL_ID)

        assert type(result) == RawlResult

        # Test that there is one extra column
        assert len(result) == len(mod.columns) + 1

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_query_with_columns(self, pgdb):
        """
        Test a case with a query with an asterisk for columns so result columns
        must be specified
        """

        RAWL_ID = 5

        mod = TheModel(RAWL_DSN)

        result = mod.query_rawls_with_asterisk(RAWL_ID)

        assert type(result) == RawlResult

        # Test that there is the same amount of columns as provided to the model
        assert len(result) == len(mod.columns)

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_get_with_string_pk(self, pgdb):
        """
        Test case that covers if a string is given as pk to get()
        """

        RAWL_ID = 5

        mod = TheModel(RAWL_DSN)

        result = mod.get(str(RAWL_ID))[0]

        assert type(result) == RawlResult

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_single_line_call(self, pgdb):
        """
        Test single line calls where the model is instantiated and a method is
        called at the same time.
        """

        RAWL_ID = 5

        result = TheModel(RAWL_DSN).get(str(RAWL_ID))[0]

        assert type(result) == RawlResult

    @pytest.mark.dependency(
        depends=["TestRawl::test_all", "TestRawl::test_get_single_rawl"]
    )
    def test_dict_assignment(self, pgdb):
        """
        This test tries to assign something to RawlResult as if it were a dict
        """

        RAWL_ID = 5
        NAME = "Shoopadoop"

        result = TheModel(RAWL_DSN).get(RAWL_ID)

        result[0]["name"] = NAME

        assert result[0].name == NAME

    @pytest.mark.dependency(
        depends=[
            "TestRawl::test_insert_dict",
            "TestRawl::test_insert_dict_without_commit",
            "TestRawl::test_insert_dict_with_invalid_column",
        ]
    )
    def test_many_insert(self, pgdb):
        """
        Test more inserts than POOL_MAX_CONN.  Tests against "connection pool
        exhausted" errors
        """

        mod = TheModel(RAWL_DSN)

        for i in range(0, POOL_MAX_CONN * 2):
            x = mod.insert_dict({"name": "Row seven is not eleven"}, commit=True)
            assert x > 0

    @pytest.mark.dependency(
        depends=[
            "TestRawl::test_insert_dict",
            "TestRawl::test_insert_dict_without_commit",
            "TestRawl::test_insert_dict_with_invalid_column",
        ]
    )
    def test_many_insert_in_transaction(self, pgdb):
        """
        Test more inserts than POOL_MAX_CONN.  Tests against "connection pool
        exhausted" errors
        """

        mod = TheModel(RAWL_DSN)

        for i in range(0, POOL_MAX_CONN * 2):
            mod.start_transaction()
            x = mod.insert_dict({"name": "Row seven is not eleven"}, commit=False)
            assert x > 0
            mod.rollback()
