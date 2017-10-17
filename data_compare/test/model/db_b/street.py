from .base import BaseB
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship


class Street (BaseB):
     __tablename__ = 'street'

     id = Column(Integer, primary_key=True)
     name = Column(String(40))

     addresses = relationship("Address", backref='street')
