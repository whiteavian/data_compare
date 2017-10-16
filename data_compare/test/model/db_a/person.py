# copied from http://docs.sqlalchemy.org/en/latest/orm/tutorial.html to spare
# creation of a pretend table.
from .address import Address
from .base import BaseA
from sqlalchemy import Column, ForeignKey, Integer, String


class Person (BaseA):
     __tablename__ = 'person'

     id = Column(Integer, primary_key=True)
     name = Column(String(32))
     fullname = Column(String(50))
     password = Column(String(40))
     address_id = Column(ForeignKey(Address.id))
