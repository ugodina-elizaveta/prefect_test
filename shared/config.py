import os

# Конфигурация PostgreSQL
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'postgres'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# Конфигурация Kafka
KAFKA_CONFIG = {
    'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')],
    'value_serializer': lambda x: x if isinstance(x, bytes) else x.encode('utf-8')
}

# Пути
AIRFLOW_DATA_PATH = os.getenv('DATA_PATH', '/opt/prefect/data/')
ODATA_TASKS_GENERATOR_TABLE = "odata_tasks_generator"
