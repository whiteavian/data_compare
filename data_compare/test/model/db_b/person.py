# copied from http://docs.sqlalchemy.org/en/latest/orm/tutorial.html to spare
# creation of a pretend table.
from .base import BaseB
from sqlalchemy import Column, Integer, String


class Person (BaseB):
     __tablename__ = 'person'

     id = Column(Integer, primary_key=True)
     nameb = Column(String)
     fullname = Column(String)
     password = Column(String)