from data_compare.sql_database import SQLDatabase, IGNORE_ITEMS, remove_ignore
from data_compare.test.model import BaseA, BaseB
from . import SQLDatabaseTestCase


class TestSQLDatabase (SQLDatabaseTestCase):

    def test_remove_ignore(self):
        """Confirm only expected remaining after adding ignore items to a set and running remove_ignore."""
        expected_remaining = set(["fooa"])
        test_set = set(["{}_a".format(IGNORE_ITEMS[0])]).union(expected_remaining)
        assert remove_ignore(test_set) == expected_remaining

    def test_compare_a_b(self):
        da = SQLDatabase(self.dbs['pg_a'])
        db = SQLDatabase(self.dbs['pg_b'])

        BaseA.metadata.create_all(da.engine)
        BaseB.metadata.create_all(db.engine)
        da.metadata.reflect()
        db.metadata.reflect()

        da.compare_schemas(db)
        da.print_differences()

        table_a = da.table_from_name('person')
        table_b = db.table_from_name('person')

        res_a = da.conn.execute(table_a.select())
        res_b = db.conn.execute(table_b.select())

        a = res_a.fetchall()
        b = res_b.fetchall()

        headers_a = tuple(c.name for c in table_a.columns)
        headers_b = tuple(c.name for c in table_b.columns)

        # Add error in "real" case for when the length > 1
        pk_a = da.table_pk_col_names(table_a)[0]
        pk_b = db.table_pk_col_names(table_b)[0]

        a.insert(0, headers_a)
        b.insert(0, headers_b)

