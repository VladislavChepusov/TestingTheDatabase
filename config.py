params = {
    'host': 'localhost',
    'port': 5432,
    'username': 'dduser',
    'password': 'dduser',
    'database': 'TTdatabase'
}

TABLE_NAME = 'people'
# Строка подключения к серверу PostgreSQL
DB_CONNECTION = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'