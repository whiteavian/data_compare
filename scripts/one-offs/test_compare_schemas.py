from data_compare.test.model.db_a import BaseA
from data_compare.test.model.db_b import BaseB
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, drop_database


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

drop_database(DBA_CONN)
drop_database(DBB_CONN)

create_database(DBA_CONN)
create_database(DBB_CONN)

engine_a = create_engine(DB_CONN_FORMAT.format(
    database='db_a', **DB_CONFIG_DICT))

engine_b = create_engine(DB_CONN_FORMAT.format(
    database='db_b', **DB_CONFIG_DICT))

BaseA.metadata.create_all(engine_a)
BaseB.metadata.create_all(engine_b)

from sqlalchemy.orm import sessionmaker

SessionA = sessionmaker(bind=engine_a)
session_a = SessionA(bind=engine_a.connect())

SessionB = sessionmaker(bind=engine_b)
session_b = SessionA(bind=engine_b.connect())

from data_compare.test.model.db_a.person import Person as PersonA
from data_compare.test.model.db_b.person import Person as PersonB


plant_a = PersonA(id=1, name="Plant", fullname="Green Plant", password="soil")
plant_b = PersonB(id=1, nameb="Plant", fullname="Green Plant", password="soil")

tree_a = PersonA(id=2, name="Tree", fullname="Treefriend", password="sunshine")
tree_b = PersonB(id=2, nameb="Tree", fullname="Treefiend", password="sunshine")

lamp_a = PersonA(id=3, name="Floor", fullname="Floorlamp", password="bright")
lamp_b = PersonB(id=4, nameb="Floor", fullname="Floorlamp", password="bright")


session_a.add(plant_a)
session_a.add(tree_a)
session_a.add(lamp_a)
session_a.commit()
session_a.flush()

session_b.add(plant_b)
session_b.add(tree_b)
session_b.add(lamp_b)
session_b.commit()
session_b.flush()


from data_compare.sql_database import SQLDatabase

sda = SQLDatabase(DBA_CONN)
sdb = SQLDatabase(DBB_CONN)

sda.compare_schemas(sdb)
print "DB A diffs"
print sda.differences['person']['id']
print sda.differences['person']['nameb_b']
print sda.differences['person']['name_a']
print sda.differences['person']['address_id_a']
