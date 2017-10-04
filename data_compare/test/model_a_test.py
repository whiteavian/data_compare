# copied from http://docs.sqlalchemy.org/en/latest/orm/tutorial.html to spare
# creation of a pretend table.

from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy.ext.declarative import declarative_base


BaseA = declarative_base(MetaData())


class PersonA (BaseA):
     __tablename__ = 'person'

     id = Column(Integer, primary_key=True)
     name = Column(String)
     fullname = Column(String)
     password = Column(String)