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
from sqlalchemy.sql import select


class TestSQLDatabase (SQLDatabaseTestCase):

    def setUp(self):
        super(TestSQLDatabase, self).setUp()
        self.da = SQLDatabase(self.dbs['my_a'])
        self.db = SQLDatabase(self.dbs['my_b'])

    def tearDown(self):
        self.da.session.close()
        self.db.session.close()

    def test_remove_ignore(self):
        """Confirm only expected remaining after adding ignore items to a set and running remove_ignore."""
        expected_remaining = set(["fooa"])
        test_set = set(["{}_a".format(IGNORE_ITEMS[0])]).union(expected_remaining)
        assert remove_ignore(test_set) == expected_remaining

    def test_delete_missing_rows(self):
        """Confirm missing rows are deleted from the database."""
        BaseA.metadata.create_all(self.da.engine)
        self.da.metadata.reflect()
        table = self.da.table_from_name("address")

        q = select([table])
        table_rows = [r for r in self.da.session.execute(q)]
        assert not table_rows

        self.da.session.execute(table.insert().values({'street_number': 1, 'id': 1}))
        self.da.session.commit()

        table_rows = [r for r in self.da.session.execute(q)]
        assert table_rows

        rows = [(1, 1, None,)]
        self.da.delete_missing_rows(table, rows)

        table_rows = [r for r in self.da.session.execute(q)]
        assert not table_rows


    def test_add_missing_rows(self):
        """Confirm missing rows are added to the database."""
        pass

    def test_compare_a_b(self):
        BaseA.metadata.create_all(self.da.engine)
        BaseB.metadata.create_all(self.db.engine)

        self.da.metadata.reflect()
        self.db.metadata.reflect()

        self.da.compare_schemas(self.db)

        # with self.da.session.begin():
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

        map(self.da.session.add, [s1, s2, s5, a1, a2, a5, p1, p2, p5])
        map(self.db.session.add, [s3, s4, s6, a3, a4, a6, p3, p4, p6])

        self.da.session.commit()
        self.db.session.commit()

        self.da.update_data_to_match_comparand()
        self.da.compare_data()

        table_a = self.da.table_from_name('person')
        table_b = self.db.table_from_name('person')

        res_a = self.da.conn.execute(table_a.select())
        res_b = self.db.conn.execute(table_b.select())

        a = res_a.fetchall()
        b = res_b.fetchall()

        headers_a = tuple(c.name for c in table_a.columns)
        headers_b = tuple(c.name for c in table_b.columns)

        # Add error in "real" case for when the length > 1
        pk_a = self.da.table_pk_col_names(table_a)[0]
        pk_b = self.db.table_pk_col_names(table_b)[0]

        a.insert(0, headers_a)
        b.insert(0, headers_b)

