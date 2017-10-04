from time import time
from sqlalchemy_utils import create_database, drop_database
from unittest import TestCase


class DataCompareTestCase (TestCase):
    """Create ephemeral databases for testing purposes, and destroy them after the
    tests have finished."""
    # TODO put these in a configuration file.
    DB_USER = 'db_user'
    DB_PASS = 'db_pass123'
    HOST = 'localhost'

    def __init__(self):
        self.CONN_STR_SUFFIX = time()

        self.PG_CONN_STR = "postgresql+psycopg2://{}:{}@{}:5432/pgdb_{}".format(
                            self.DB_USER, self.DB_PASS, self.HOST, self.CONN_STR_SUFFIX)
        self.MY_CONN_STR = "mysql+mysqldb://{}:{}@{}/mydb_{}".format(
                            self.DB_USER, self.DB_PASS, self.HOST, self.CONN_STR_SUFFIX)
        # Because of the way pyodbc requires odbc.ini, maybe this setup will not work. 
        # Maybe we should use pymssql driver instead? TODO
        # self.MS_CONN_STR = "mssql+pyodbc://{}:{}@?driver=SQL+Server+Native+Client+11.0".format(self.DB_USER, self.DB_PASS)
        self.LITE_CONN_STR = "sqlite:///test{}.db".format(self.CONN_STR_SUFFIX)


    def test_create_dbs(self):
        dbs = [
            self.LITE_CONN_STR,
            self.PG_CONN_STR,
            self.MY_CONN_STR,
            # self.MS_CONN_STR,
        ]

        map(create_database, dbs)

        # Do this on exit.
        map(drop_database, dbs)

def main():
    ts = TestSetup()
    ts.test_create_dbs()

if __name__ == "__main__":
    main()
