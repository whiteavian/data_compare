from .base import BaseA
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from .street import Street


class Address (BaseA):
     __tablename__ = 'address'

     id = Column(Integer, primary_key=True)
     street_number = Column(String(10))
     street_id = Column(ForeignKey(Street.id))

     people = relationship("Person", backref='address')
