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
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from psycopg2.extras import DictCursor

from shared.config import POSTGRES_CONFIG, KAFKA_CONFIG, AIRFLOW_DATA_PATH, ODATA_TASKS_GENERATOR_TABLE


@task(retries=1, retry_delay_seconds=300)
def fetch_data_and_save_to_files(
    directory_path: str,
    sql: str,
    avro_schema: dict,
    batch_size: int = 1000
) -> None:
    """
    Извлекает данные из PostgreSQL и сохраняет их в файлы Avro батчами.
    """
    logger = get_run_logger()

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

            # Если накопился батч, записываем его в файл
            if len(batch_data) >= batch_size:
                file_path = os.path.join(directory_path, f'batch_{file_counter}.avro')
                with open(file_path, 'wb') as f:
                    writer(f, parsed_schema, batch_data)
                logger.info(f"Saved batch {file_counter} to {file_path}")

                # Очищаем батч и увеличиваем счетчик файлов
                batch_data = []
                file_counter += 1

        # Запись оставшихся данных (последний батч)
        if batch_data:
            file_path = os.path.join(directory_path, f'batch_{file_counter}.avro')
            with open(file_path, 'wb') as f:
                writer(f, parsed_schema, batch_data)
            logger.info(f"Saved final batch {file_counter} to {file_path}")

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


@task(retries=1, retry_delay_seconds=300)
def send_messages_to_kafka(directory_path: str, topic_name: str, task_format: str) -> None:
    """
    Обработка всех файлов в директории и отправка их в Kafka в указанном формате.
    """
    logger = get_run_logger()

    logger.info(f"directory_path {directory_path}, format: {task_format}")

    if task_format not in ('avro', 'json'):
        raise ValueError(f"Unsupported format: {task_format}. Must be 'avro' or 'json'")

    # Создаем Kafka producer
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
                        # Преобразуем Avro в JSON
                        avro_file = BytesIO(file_data)
                        reader = fastavro.reader(avro_file)
                        records = [record for record in reader]
                        file_data = json.dumps(records).encode('utf-8')
                    except Exception as e:
                        logger.error(f"Failed to convert Avro to JSON for {filename}: {e}")
                        continue

                # Отправляем в Kafka
                producer.send(topic_name, value=file_data)
                logger.info(f"Sent {filename} to topic {topic_name}")

            # Удаление файла после отправки
            os.remove(file_path)
            logger.info(f"Sent and removed {filename}")

        # Убеждаемся, что все сообщения отправлены
        producer.flush()

    except Exception as e:
        logger.error(f"Error sending messages to Kafka: {e}")
        raise
    finally:
        producer.close()


@task
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

        for taska in tasks:
            taska['directory_path'] = AIRFLOW_DATA_PATH + taska['directory_path']
            # Парсим JSON строку в словарь для avro_schema
            if isinstance(taska['avro_schema'], str):
                taska['avro_schema'] = json.loads(taska['avro_schema'])

        logger.info(f"Found {len(tasks)} enabled tasks")
        return tasks

    except Exception as e:
        logger.error(f"Error getting params: {e}")
        raise
    finally:
        conn.close()


@flow(
    name="odata_tasks_generator",
    task_runner=ConcurrentTaskRunner(),
    retries=1,
    retry_delay_seconds=300
)
def odata_tasks_flow():
    """
    Основной flow для обработки задач извлечения данных и отправки в Kafka.
    """
    logger = get_run_logger()

    try:
        # Получаем параметры всех задач
        params_list = get_params()

        if not params_list:
            logger.info("No enabled tasks found")
            return

        # Обрабатываем каждую задачу
        for param in params_list:
            logger.info(f"Processing task: {param['task_name']}")

            # Извлекаем данные и сохраняем в файлы
            fetch_data_and_save_to_files(
                directory_path=param['directory_path'],
                sql=param['sql'],
                avro_schema=param['avro_schema'],
                batch_size=param['batch_size']
            )

            # Отправляем данные в Kafka
            send_messages_to_kafka(
                directory_path=param['directory_path'],
                topic_name=param['topic_name'],
                task_format=param['task_format']
            )

            logger.info(f"Completed task: {param['task_name']}")

    except Exception as e:
        logger.error(f"Flow failed: {e}")
        raise
