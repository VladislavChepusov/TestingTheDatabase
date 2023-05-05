import random
import sys
import time
from datetime import date

from faker import Faker
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy.exc import OperationalError

from Gender import Gender
from Person import Person, Base
from config import params, TABLE_NAME, DB_CONNECTION


# region Функции
def CreateTable():
    inspector = inspect(engine)
    if inspector.has_table(TABLE_NAME):
        print('Table already exists')
    else:
        # print('Table does not exist')
        # Создайте базу данных и таблицу
        Base.metadata.create_all(engine)
        print('The table has been created')

# МОДИЦИКАЦИЯ НУЖНА ТУТ !!!!!!
def CheckingInput():
    pass


# # МОДИЦИКАЦИЯ НУЖНА ТУТ !!!!!!
def AddSingleEntry():
    session = Session()
    CreateTable()
    add_person('John Smith', '1990-01-01', Gender.MALE, session)
    session.commit()
    session.close()


# Подсчет возраста
def get_age(birthdate):
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age


# Вывод всех строк с уникальным значением ФИО + дата, отсортированным по ФИО
def OutputAll():
    session = Session()
    people = session.query(Person) \
        .distinct(Person.name, Person.date_of_birth) \
        .order_by(Person.name).all()
    if not people:
        print("No data found")
    else:
        for person in people:
            age = get_age(person.date_of_birth)
            # print(f"{person.name}, {person.date_of_birth}, {person.gender.name}, {age} years old")
            print(f"Full name: {person.name}, Birth date: {person.date_of_birth}, Gender: {person.gender.name}, Age: {age}")
    session.close()


def add_person(name, birthdate, gender, session):
    person = Person(
        name=name,
        date_of_birth=birthdate,
        gender=gender
    )
    session.add(person)

# МОДИЦИКАЦИЯ НУЖНА ТУТ !!!!!!
# БОЛЬШИЕ ЗНАЧЕНИЯ ПОСТАВИТЬ !!!
def AutoGenerationRecords():
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
    session.close()


# Функция для выполнения запроса к базе данных и замера времени
def SelectData():
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
            print(f"Full name: {person.name}, Birth date: {person.date_of_birth}, Gender: {person.gender.name}, Age: {age}")

        print(f"Total rows: {len(result)}")
        print(f"Execution time: {end_time - start_time:.2f} seconds")
        session.close()
    except Exception as e:
        print(f"Error: {str(e)}")


# def main(*args):
#     for arg in args:
#         print(arg)


# endregion

# МОДИЦИКАЦИЯ НУЖНА ТУТ !!!!!!
if __name__ == '__main__':
    # region Объявления/Подключение
    try:
        # Создание объекта Engine для подключения к серверу PostgresSQL
        engine = create_engine(DB_CONNECTION.format(**params), isolation_level='AUTOCOMMIT')
        engine.connect()
    except OperationalError as e:
        print("Connection failed:", e)
        sys.exit()
    try:
        # Создайте фабрику сеансов
        Session = sessionmaker(bind=engine)
    except Exception as e:
        print(f"Error: {str(e)}")

    # endregion
    # Проверка наличия базы данных
    if not database_exists(engine.url):
        create_database(engine.url)

    x = int(input("Ввод = "))
    if x == 1:
        CreateTable()
    if x == 2:
        AddSingleEntry()
    if x == 3:
        OutputAll()
    if x == 4:
        AutoGenerationRecords()
    if x == 5:
        SelectData()

    engine.dispose()

    # main(*sys.argv[1:])
