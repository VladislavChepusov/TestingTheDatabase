import time

from sqlalchemy import create_engine, Column, Integer, String, Date, inspect, Enum
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists
import sys

from Gender import Gender
from Person import Person, Base
from config import params, TABLE_NAME, DB_CONNECTION
from datetime import date
from faker import Faker
import random


# region Функции
def Create_Table():
    inspector = inspect(engine)
    if inspector.has_table(TABLE_NAME):
        print('Table already exists')
    else:
        # print('Table does not exist')
        # Создайте базу данных и таблицу
        Base.metadata.create_all(engine)
        print('The table has been created')


# ДОПИСАТЬ ВВОД!!!!!!!!!!!!!
def AddSingleEntry():
    session = Session()
    Create_Table()
    person1 = Person(name='John Smith', date_of_birth='1990-01-01', gender=Gender.MALE)
    person2 = Person(name='Jane Doe', date_of_birth='1995-01-01', gender=random.choice(list(Gender)))
    session.add_all([person1, person2])
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
            print(f"{person.name}, {person.date_of_birth}, {person.gender.name}, {age} years old")

    session.close()


def add_person(name, birthdate, gender, session):
    person = Person(
        name=name,
        date_of_birth=birthdate,
        gender=gender
    )
    session.add(person)


# БОЛЬШИЕ ЗНАЧЕНИЯ ПОСТАВИТЬ !!!
def autoGenerationRecords():
    faker = Faker()
    session = Session()
    # Заполняем 1 млн строк
    for i in range(10):
        birthdate = faker.date_of_birth()
        gender = random.choice(list(Gender))
        if gender == Gender.MALE:
            first_name = faker.name_male()
        else:
            first_name = faker.name_female()
        add_person(first_name, birthdate, gender, session)

    # Заполнение автоматически 100 строк в которых пол мужской и ФИО начинается с "F".
    for i in range(10):
        while True:
            name = faker.name_male()
            if name.startswith('F'):
                break
        birthdate = faker.date_of_birth()
        gender = Gender.MALE
        add_person(name, birthdate, gender, session)
    session.commit()


# Функция для выполнения запроса к базе данных и замера времени
def select_data():
    try:
        # Выполняем запрос к базе данных
        start_time = time.time()
        session = Session()
        query = session.query(Person).filter(Person.gender == Gender.MALE, Person.name.like('F%'))
        result = query.all()
        end_time = time.time()

        # Выводим результаты
        for person in result:
            age = get_age(person.date_of_birth)
            print(f"Full name: {person.name}, Birth date: {person.date_of_birth}, Gender: {person.gender}, Age: {age}")

        print(f"Total rows: {len(result)}")
        print(f"Execution time: {end_time - start_time:.2f} seconds")
    except Exception as e:
        print(f"Error: {str(e)}")


# def main(*args):
#     for arg in args:
#         print(arg)


# endregion


if __name__ == '__main__':
    # region Объявления

    # Создание объекта Engine для подключения к серверу PostgreSQL
    engine = create_engine(DB_CONNECTION.format(**params), isolation_level='AUTOCOMMIT')

    # Подключение к базе данных
    # engine.dispose()
    # engine = create_engine(DB_CONNECTION.format(**params))

    # Создайте фабрику сеансов
    Session = sessionmaker(bind=engine)

    # endregion
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
    if x == 4:
        autoGenerationRecords()
    if x == 5:
        select_data()

    # main(*sys.argv[1:])
