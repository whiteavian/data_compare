from data_compare.sql_database import SQLDatabase, IGNORE_ITEMS, remove_ignore
from data_compare.test.model import BaseA, BaseB
from data_compare.test.model.db_a import (
    Address as AddressA,
    Person as PersonA,
    Street as StreetA,
)
from data_compare.test.model.db_b import (
    Address as AddressB,
    Person as PersonB,
    Street as StreetB,
)
from . import SQLDatabaseTestCase


class TestSQLDatabase (SQLDatabaseTestCase):

    def test_remove_ignore(self):
        """Confirm only expected remaining after adding ignore items to a set and running remove_ignore."""
        expected_remaining = set(["fooa"])
        test_set = set(["{}_a".format(IGNORE_ITEMS[0])]).union(expected_remaining)
        assert remove_ignore(test_set) == expected_remaining

    def test_compare_a_b(self):
        try:
            da = SQLDatabase(self.dbs['my_a'])
            db = SQLDatabase(self.dbs['my_b'])

            BaseA.metadata.create_all(da.engine)
            BaseB.metadata.create_all(db.engine)
            da.metadata.reflect()
            db.metadata.reflect()

            da.compare_schemas(db)
            da.print_differences()

            # with da.session.begin():
            s1 = StreetA(id=1, name='Cedar')
            s2 = StreetA(id=2, name='MLK')
            s3 = StreetB(id=2, name='Malcolm X')
            s4 = StreetB(id=3, name='Burns')
            s5 = StreetA(id=4, name='Broadway')
            s6 = StreetB(id=4, name='Broadway')

            a1 = AddressA(id=1, street_number=5, street=s1)
            a2 = AddressA(id=2, street_number=10, street=s2)
            a3 = AddressB(id=2, street_number=30, street=s3)
            a4 = AddressB(id=3, street_number=50, street=s4)
            a5 = AddressA(id=4, street_number=60, street=s5)
            a6 = AddressB(id=4, street_number=60, street=s6)

            p1 = PersonA(
                id=1, name="Constance", fullname="Constance Filbert", password="password", address=a1)
            p2 = PersonA(
                id=2, name="Foo", fullname="Foo Filbert", password="password", address=a2)
            p3 = PersonB(
                id=2, nameb="Fil", fullname="Fil Fil", password="password")
            p4 = PersonB(
                id=3, nameb="Jemima", fullname="Jemima", password="password")
            p5 = PersonA(
                id=4, name="Cereal", fullname="C Real", password="password", address=a5)
            p6 = PersonB(
                id=4, nameb="Cereal", fullname="C Real", password="password")

            map(da.session.add, [s1, s2, s5, a1, a2, a5, p1, p2, p5])
            map(db.session.add, [s3, s4, s6, a3, a4, a6, p3, p4, p6])

            da.session.commit()
            db.session.commit()

            data_diffs = da.compare_data(update=True)

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

        finally:
            da.session.close()
            db.session.close()

