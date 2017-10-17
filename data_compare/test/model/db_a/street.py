from .base import BaseA
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship


class Street (BaseA):
     __tablename__ = 'street'

     id = Column(Integer, primary_key=True)
     name = Column(String(40))

     addresses = relationship("Address", backref='street')
