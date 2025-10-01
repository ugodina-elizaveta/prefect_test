import os
import json
import shutil
from io import BytesIO
from typing import List, Dict, Any

import fastavro
import pandas as pd
import psycopg2
from fastavro import writer, parse_schema
from kafka import KafkaProducer
from prefect import task, get_run_logger
from psycopg2.extras import DictCursor

from shared.config import POSTGRES_CONFIG, KAFKA_CONFIG, AIRFLOW_DATA_PATH, ODATA_TASKS_GENERATOR_TABLE


@task(
    retries=1,
    retry_delay_seconds=300
)
def get_params() -> List[Dict[str, Any]]:
    """
    Получает параметры задач из базы данных.
    """
    logger = get_run_logger()

    conn = psycopg2.connect(**POSTGRES_CONFIG)

    sql = f"""
        SELECT id, "sql", avro_schema, directory_path, topic_name, batch_size, task_name, task_format
        FROM {ODATA_TASKS_GENERATOR_TABLE}
        WHERE is_enabled = true
        ORDER BY task_name;
    """

    try:
        df = pd.read_sql(sql, conn)
        tasks = df.to_dict(orient='records')

        for task_data in tasks:
            task_data['directory_path'] = AIRFLOW_DATA_PATH + task_data['directory_path']
            if isinstance(task_data['avro_schema'], str):
                task_data['avro_schema'] = json.loads(task_data['avro_schema'])

        logger.info(f"Found {len(tasks)} enabled tasks")
        return tasks

    except Exception as e:
        logger.error(f"Error getting params: {e}")
        raise
    finally:
        conn.close()


@task(
    retries=1,
    retry_delay_seconds=300
)
def fetch_data_and_save_to_files(task_config: Dict[str, Any]) -> None:
    """
    Задача для извлечения данных и сохранения в Avro файлы.
    """
    logger = get_run_logger()
    task_name = task_config['task_name']

    directory_path = task_config['directory_path']
    sql = task_config['sql']
    avro_schema = task_config['avro_schema']
    batch_size = task_config['batch_size']

    logger.info(f"Starting data fetch for: {task_name}")

    # Очистка и создание директории
    if os.path.isdir(directory_path):
        if os.listdir(directory_path):
            raise FileExistsError(f"The directory '{directory_path}' already contains files.")
        shutil.rmtree(directory_path)
    os.makedirs(directory_path, exist_ok=True)

    # Подключение к базе данных
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor(name='streaming_cursor', cursor_factory=DictCursor)
    cursor.itersize = batch_size

    # Парсинг схемы Avro
    parsed_schema = parse_schema(avro_schema)

    # Выполнение SQL-запроса
    cursor.execute(sql)

    batch_data = []
    file_counter = 1

    try:
        # Потоковая обработка данных
        for row in cursor:
            batch_data.append(dict(row))

            if len(batch_data) >= batch_size:
                file_path = os.path.join(directory_path, f'batch_{file_counter}.avro')
                with open(file_path, 'wb') as f:
                    writer(f, parsed_schema, batch_data)
                logger.info(f"Saved batch {file_counter} to {file_path}")

                batch_data = []
                file_counter += 1

        if batch_data:
            file_path = os.path.join(directory_path, f'batch_{file_counter}.avro')
            with open(file_path, 'wb') as f:
                writer(f, parsed_schema, batch_data)
            logger.info(f"Saved final batch {file_counter} to {file_path}")

        logger.info(f"Completed data fetch for: {task_name}")

    except Exception as e:
        logger.error(f"Error processing data for {task_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


@task(
    retries=1,
    retry_delay_seconds=300
)
def send_messages_to_kafka(task_config: Dict[str, Any]) -> None:
    """
    Задача для отправки данных в Kafka.
    """
    logger = get_run_logger()
    task_name = task_config['task_name']

    directory_path = task_config['directory_path']
    topic_name = task_config['topic_name']
    task_format = task_config['task_format']

    logger.info(f"Starting Kafka send for: {task_name}")
    logger.info(f"Directory: {directory_path}, Format: {task_format}")

    if task_format not in ('avro', 'json'):
        raise ValueError(f"Unsupported format: {task_format}. Must be 'avro' or 'json'")

    producer = KafkaProducer(**KAFKA_CONFIG)

    try:
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)

            with open(file_path, 'rb') as f:
                file_data = f.read()

                if not file_data:
                    logger.warning(f"No data in {filename} for producing to Kafka")
                    continue

                if task_format == 'json':
                    try:
                        avro_file = BytesIO(file_data)
                        reader = fastavro.reader(avro_file)
                        records = [record for record in reader]
                        file_data = json.dumps(records).encode('utf-8')
                    except Exception as e:
                        logger.error(f"Failed to convert Avro to JSON for {filename}: {e}")
                        continue

                producer.send(topic_name, value=file_data)
                logger.info(f"Sent {filename} to topic {topic_name}")

            os.remove(file_path)
            logger.info(f"Sent and removed {filename}")

        producer.flush()
        logger.info(f"Completed Kafka send for: {task_name}")

    except Exception as e:
        logger.error(f"Error sending messages to Kafka for {task_name}: {e}")
        raise
    finally:
        producer.close()
