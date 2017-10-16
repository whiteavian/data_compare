from sqlalchemy import Column, ForeignKey, Integer, String
from .base import BaseA


class Street (BaseA):
     __tablename__ = 'street'

     id = Column(Integer, primary_key=True)
     name = Column(String(40))
