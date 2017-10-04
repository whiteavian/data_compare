import atexit
from time import time
from sqlalchemy_utils import create_database, drop_database
from unittest import TestCase


class SQLDatabaseTestCase (TestCase):
    """Create ephemeral databases for testing purposes, and destroy them after the
    tests have finished."""
    # TODO put these in a configuration file.
    DB_USER = 'db_user'
    DB_PASS = 'db_pass123'
    HOST = 'localhost'

    def __init__(self):
        CONN_STR_SUFFIX = time()

        PG_CONN_STR = "postgresql+psycopg2://{}:{}@{}:5432/pgdb_{}{{}}".format(
                            self.DB_USER, self.DB_PASS, self.HOST, self.CONN_STR_SUFFIX)
        MY_CONN_STR = "mysql+mysqldb://{}:{}@{}/mydb_{}{{}}".format(
                            self.DB_USER, self.DB_PASS, self.HOST, self.CONN_STR_SUFFIX)
        # Because of the way pyodbc requires odbc.ini, maybe this setup will not work. 
        # Maybe we should use pymssql driver instead? TODO
        # MS_CONN_STR = "mssql+pyodbc://{}:{}@?driver=SQL+Server+Native+Client+11.0".format(self.DB_USER, self.DB_PASS)
        LITE_CONN_STR = "sqlite:///test{}{{}}.db".format(CONN_STR_SUFFIX)

        self.dbs = {
            'lite_a': LITE_CONN_STR.format("a"),
            'lite_b': LITE_CONN_STR.format("b"),
            'pg_a': PG_CONN_STR.format("a"),
            'pg_b': PG_CONN_STR.format("b"),
            'my_a': MY_CONN_STR.format("a"),
            'my_b': MY_CONN_STR.format("b"),
            # MS_CONN_STR,
        }
        map(create_database, self.dbs)

        # Ensure test databases are ephemeral.
        atexit.register(map, drop_database, self.dbs)
