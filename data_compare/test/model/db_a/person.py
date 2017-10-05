# copied from http://docs.sqlalchemy.org/en/latest/orm/tutorial.html to spare
# creation of a pretend table.
from sqlalchemy import Column, Integer, String
from . import BaseA


class Person (BaseA):
     __tablename__ = 'person'

     id = Column(Integer, primary_key=True)
     name = Column(String)
     fullname = Column(String)
     password = Column(String)