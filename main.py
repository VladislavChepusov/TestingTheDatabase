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

# region БАЗОВЫЕ ОБЪЯВЛЕНИЯ/ПОДКЛЮЧЕНИЯ
try:
    # Создание объекта Engine для подключения к серверу PostgresSQL
    engine = create_engine(DB_CONNECTION.format(**params), isolation_level='AUTOCOMMIT')
    # Проверка наличия базы данных
    if not database_exists(engine.url):
        create_database(engine.url)
    engine.connect()
except OperationalError as e:
    print("Connection failed:", e)
    sys.exit()
try:
    # Создайте фабрику сеансов
    Session = sessionmaker(bind=engine)
except Exception as e:
    print(f"Error: {str(e)}")
    sys.exit()

# endregion


# region ФУНКЦИИ

# Создание таблицы в БД
def CreateTable():
    inspector = inspect(engine)
    if not inspector.has_table(TABLE_NAME):
        # Создайте базу данных и таблицу
        Base.metadata.create_all(engine)
        print('The table has been created')


# Добавление записи в БД
def AddSingleEntry(arg):
    session = Session()
    CreateTable()
    add_person(f"{arg[0]} {arg[1]}", arg[2], Gender(arg[3]), session)
    # add_person('John Smith', '1990-01-01', Gender.MALE, session)
    session.commit()
    print(f"Data added: {arg[0]} {arg[1]} {arg[2]} {Gender(arg[3]).name}")
    session.close()


# Подсчет возраста
def get_age(birthdate):
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age


# Вывод всех строк с уникальным значением ФИО + дата, отсортированным по ФИО
def OutputAll():
    session = Session()
    CreateTable()
    people = session.query(Person) \
        .distinct(Person.name, Person.date_of_birth) \
        .order_by(Person.name).all()
    if not people:
        print("No data found")
    else:
        for person in people:
            age = get_age(person.date_of_birth)
            # print(f"{person.name}, {person.date_of_birth}, {person.gender.name}, {age} years old")
            print(
                f"Full name: {person.name}, Birth date: {person.date_of_birth}, "
                f"Gender: {person.gender.name}, Age: {age}")
    session.close()


# Вспомогательная функция добавления данных в БД
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
    CreateTable()
    # Заполняем 1 млн строк
    for i in range(1000000):
        birthdate = faker.date_of_birth()
        gender = random.choice(list(Gender))
        if gender == Gender.MALE:
            first_name = faker.name_male()
        else:
            first_name = faker.name_female()
        add_person(first_name, birthdate, gender, session)

    # Заполнение автоматически 100 строк в которых пол мужской и ФИО начинается с "F".
    for i in range(100):
        while True:
            name = faker.name_male()
            if name.startswith('F'):
                break
        birthdate = faker.date_of_birth()
        gender = Gender.MALE
        add_person(name, birthdate, gender, session)
    session.commit()
    print("Data created")
    session.close()


# Функция для выполнения запроса к базе данных и замера времени
def SelectData():
    try:
        # Выполняем запрос к базе данных
        session = Session()
        CreateTable()
        start_time = time.time()
        query = session.query(Person).filter(Person.gender == Gender.MALE, Person.name.like('F%'))
        result = query.all()
        end_time = time.time()
        # Выводим результаты
        for person in result:
            age = get_age(person.date_of_birth)
            print(
                f"Full name: {person.name}, Birth date: {person.date_of_birth}, "
                f"Gender: {person.gender.name}, Age: {age}")

        print(f"Total rows: {len(result)}")
        print(f"Execution time: {end_time - start_time:.2f} seconds")
        session.close()
    except Exception as er:
        print(f"Error: {str(er)}")


# Основная функция
def main(*args):
    if args[0] == "1":
        CreateTable()
    elif args[0] == "2":
        if check_arguments(args):
            AddSingleEntry(args[1:])
        else:
            print("Invalid input.Try the following input format."
                  "<Operation Number> <Name> <Last name> <Date Of Birth: yyyy-mm-dd> <Gender: F or M>"
                  "Example: 2 John Nash 1928-06-13 M")
            sys.exit()
    elif args[0] == "3":
        OutputAll()
    elif args[0] == "4":
        AutoGenerationRecords()
    elif args[0] == "5":
        SelectData()
    else:
        print("Invalid input")
    engine.dispose()


# Проверка вводимых аргументов
def check_arguments(argv):
    if len(argv) != 5:
        return False
    if not isinstance(argv[1], str):
        return False
    if not isinstance(argv[2], str):
        return False
    try:
        date.fromisoformat(argv[3])
    except ValueError:
        return False
    if argv[4] not in ["F", "M"]:
        return False
    return True

# endregion


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(*sys.argv[1:])
    else:
        print("Invalid input.Enter the parameters")
