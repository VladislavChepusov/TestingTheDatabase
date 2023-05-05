from sqlalchemy import create_engine, Column, Integer, String, Date, inspect
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists
import sys
from config import params
from datetime import date

# region Объявления

# Строка подключения к серверу PostgreSQL
DB_CONNECTION = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
TABLE_NAME = "people"
# Создание объекта Engine для подключения к серверу PostgreSQL
engine = create_engine(DB_CONNECTION.format(**params), isolation_level='AUTOCOMMIT')
# Подключение к базе данных
# engine.dispose()
# engine = create_engine(DB_CONNECTION.format(**params))

# Создайте фабрику сеансов
Session = sessionmaker(bind=engine)


# Создайте базовый класс для нашей модели ORM
class Base(DeclarativeBase):
    pass


# Определите класс для нашей таблицы
class Person(Base):
    __tablename__ = TABLE_NAME
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    date_of_birth = Column(Date)
    gender = Column(String)


# endregion


# region Функции
def Create_Table():
    inspector = inspect(engine)
    if inspector.has_table(TABLE_NAME):
        print('Table already exists')
    else:
        print('Table does not exist')
        # Создайте базу данных и таблицу
        Base.metadata.create_all(engine)

# ДОПИСАТЬ ВВОД
def AddSingleEntry():
    session = Session()
    Create_Table()
    person1 = Person(name='John Smith', date_of_birth='1990-01-01', gender='Male')
    person2 = Person(name='Jane Doe', date_of_birth='1995-01-01', gender='Female')
    # session.add_all([person1, person2])
    session.add(person1)

    session.commit()
    session.close()


# Подсчет возраста
def get_age(birthdate):
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age


# Вывод всех строк с уникальным значением ФИО+дата, отсортированным по ФИО
def Output_All():
    session = Session()
    people = session.query(Person) \
        .distinct(Person.name, Person.date_of_birth) \
        .order_by(Person.name).all()
    if not people:
        print("No data found")
    else:
        for person in people:
            age = get_age(person.date_of_birth)
            print(f"{person.name}, {person.date_of_birth}, {person.gender}, {age} years old")

    session.close()


# def main(*args):
#     for arg in args:
#         print(arg)


# endregion


# Получить объект сеанса


# Create_Table()
# AddSingleEntry()


if __name__ == '__main__':
    # Проверка наличия базы данных
    if not database_exists(engine.url):
        create_database(engine.url)
    x = int(input("Ввод = "))
    if x == 1:
        Create_Table()
    if x == 2:
        AddSingleEntry()
    if x == 3:
        Output_All()

    # main(*sys.argv[1:])
