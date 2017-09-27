from time import time
from sqlalchemy_utils import create_database, drop_database


class TestSetup:
    """Create ephemeral databases for testing purposes, and destroy them after the
    tests have finished."""
    # TODO put these in a configuration file.
    DB_USER = 'db_user'
    DB_PASS = 'db_pass'
    HOST = 'localhost'

    def __init__(self):
        CONN_STR_SUFFIX = time()

        self.PG_CONN_STR = "postgresql+psycopg2://{}:{}@{}:5432/pgdb_{}".format(
                            self.DB_USER, self.DB_PASS, self.HOST, CONN_STR_SUFFIX)
        self.MY_CONN_STR = "mysql+mysqldb://{}:{}@{}/mydb_{}".format(
                            self.DB_USER, self.DB_PASS, self.HOST, CONN_STR_SUFFIX)
        # Because of the way pyodbc requires odbc.ini, maybe this setup will not work. 
        # Maybe we should use pymssql driver instead? TODO
        self.MS_CONN_STR = "mssql+pyodbc://{}:{}@".format(self.DB_USER, self.DB_PASS)
        self.LITE_CONN_STR = "sqlite:///test{}.db".format(CONN_STR_SUFFIX)


    def test_create_dbs(self):
        create_database(self.LITE_CONN_STR)
        create_database(self.PG_CONN_STR)
        create_database(self.MY_CONN_STR)
      
        # Do this on exit. 
        drop_database(self.LITE_CONN_STR)
        drop_database(self.PG_CONN_STR)
        drop_database(self.MY_CONN_STR)

def main():
    ts = TestSetup()
    ts.test_create_dbs()

if __name__ == "__main__":
    main()
