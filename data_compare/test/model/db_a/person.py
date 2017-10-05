# copied from http://docs.sqlalchemy.org/en/latest/orm/tutorial.html to spare
# creation of a pretend table.
from .address import Address
from . import BaseA
from sqlalchemy import Column, ForeignKey, Integer, String


class Person (BaseA):
     __tablename__ = 'person'

     id = Column(Integer, primary_key=True)
     name = Column(String)
     fullname = Column(String)
     password = Column(String)
     address_id = Column(ForeignKey(Address.id))
