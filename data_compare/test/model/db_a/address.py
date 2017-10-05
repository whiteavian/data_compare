from . import BaseA
from sqlalchemy import Column, ForeignKey, Integer, String
from .street import Street


class Address (BaseA):
     __tablename__ = 'address'

     id = Column(Integer, primary_key=True)
     street_number = Column(String)
     street_id = Column(ForeignKey(Street.id))
