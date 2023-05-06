# Параметры подключения
params = {
    'host': 'localhost',
    'port': 5432,
    'username': 'dduser',
    'password': 'dduser',
    'database': 'TTdatabase'
}

# Название таблицы в БД
TABLE_NAME = 'people'

# Строка подключения к серверу PostgreSQL
DB_CONNECTION = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
