from data_compare.sql_database import SQLDatabase
from model_a_test import BaseA
from model_b_test import BaseB
from . import SQLDatabaseTestCase


class TestSQLDatabase (SQLDatabaseTestCase):

    def test_compare_a_b(self):
        a = SQLDatabase(self.dbs['pg_a'])
        b = SQLDatabase(self.dbs['pg_b'])

        BaseA.metadata.create_all(a.engine)
        BaseB.metadata.create_all(b.engine)

        a.compare_schemas(b)

        table_a = a.table_from_name('person')
        table_b = b.table_from_name('person')

        res_a = a.conn.execute(table_a.select())
        res_b = b.conn.execute(table_b.select())

        a = res_a.fetchall()
        b = res_b.fetchall()

        headers_a = tuple(c.name for c in table_a.columns)
        headers_b = tuple(c.name for c in table_b.columns)

        # Add error in "real" case for when the length > 1
        pk_a = a.table_pk_col_names(table_a)[0]
        pk_b = b.table_pk_col_names(table_b)[0]

        a.insert(0, headers_a)
        b.insert(0, headers_b)

        a
        b

        a_rd = RelationalData(a, pk_a)
        b_rd = RelationalData(b, pk_b)
        a_rd.compare(b_rd)
        a_rd.errors
