from .base import BaseB
from sqlalchemy import Column, ForeignKey, Integer, String
from .street import Street


class Address (BaseB):
     __tablename__ = 'address'

     id = Column(Integer, primary_key=True)
     street_number = Column(String(10))
     street_id = Column(ForeignKey(Street.id))
