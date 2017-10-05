from data_compare.test.model_a_test import BaseA
from data_compare.test.model_b_test import BaseB
from sqlalchemy import create_engine


# parameter format borrowed from https://gist.github.com/sprin/5846464
DB_CONFIG_DICT = {
    'user': 'db_user',
    'password': 'db_pass123',
    'host': 'localhost',
    'port': 5432,
}

DB_CONN_FORMAT = "postgresql://{user}:{password}@{host}:{port}/{database}"
DBA_CONN = DB_CONN_FORMAT.format(database='db_a', **DB_CONFIG_DICT)
DBB_CONN = DB_CONN_FORMAT.format(database='db_b', **DB_CONFIG_DICT)


def test_compare_schemas():
    engine_a = create_engine(DB_CONN_FORMAT.format(
        database='db_a', **DB_CONFIG_DICT))

    engine_b = create_engine(DB_CONN_FORMAT.format(
        database='db_b', **DB_CONFIG_DICT))

    BaseA.metadata.create_all(engine_a)
    BaseB.metadata.create_all(engine_b)

    # create sqlite, postgres, mysql, maria? and mssql databases and exhaustively
    # compare them. Start with sqlite for now, and later extend.

