# Создайте базовый класс для нашей модели ORM

from sqlalchemy import Column, Integer, String, Date, Enum
from sqlalchemy.orm import DeclarativeBase

from Gender import Gender
from config import TABLE_NAME


class Base(DeclarativeBase):
    pass


# Определите класс для нашей таблицы
class Person(Base):
    __tablename__ = TABLE_NAME
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    date_of_birth = Column(Date)
    gender = Column(Enum(Gender))
