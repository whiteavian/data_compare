from sqlalchemy import Column, ForeignKey, Integer, String
from . import BaseB


class Street (BaseB):
     __tablename__ = 'street'

     id = Column(Integer, primary_key=True)
     name = Column(String)
