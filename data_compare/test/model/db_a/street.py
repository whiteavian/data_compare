from sqlalchemy import Column, ForeignKey, Integer, String
from . import BaseA


class Street (BaseA):
     __tablename__ = 'street'

     id = Column(Integer, primary_key=True)
     name = Column(String)
