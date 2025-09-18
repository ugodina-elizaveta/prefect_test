from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from fastavro import writer, parse_schema
from psycopg2.extras import DictCursor
from io import BytesIO
import fastavro
import json
import os
import shutil
from typing import Dict, Any
# from prefect import flow

import psycopg2
from kafka import KafkaProducer as SyncKafkaProducer


POSTGRES_CONFIG = {
    "host": "postgres",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

KAFKA_CONFIG = {
    "bootstrap_servers": "kafka:9092",
    "security_protocol": "PLAINTEXT"
}

DIR_PATH = "/opt/prefect/data"


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_params_from_db():
    """Получение параметров задач из базы данных"""
    logger = get_run_logger()

    odata_tasks_generator_table = "odata_tasks_generator"

    sql = f"""
        SELECT id, "sql", avro_schema, directory_path, topic_name, batch_size, task_name, task_format
        FROM {odata_tasks_generator_table}
        WHERE is_enabled = true
        ORDER BY task_name;
    """

    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            tasks = [dict(zip(columns, row)) for row in results]

            for taska in tasks:
                # Преобразуем строку схемы в dict
                if isinstance(taska['avro_schema'], str):
                    taska['avro_schema'] = json.loads(taska['avro_schema'])
                taska['directory_path'] = os.path.join(DIR_PATH, taska['directory_path'].lstrip('/'))

            logger.info(f"Retrieved {len(tasks)} tasks from database")
            return tasks

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


@task(retries=3, retry_delay_seconds=300)
def fetch_data_and_save_to_files(directory_path: str, sql: str, avro_schema: dict, batch_size: int = 1000):
    """Извлечение данных и сохранение в Avro файлы"""
    logger = get_run_logger()

    # Очистка и создание директории
    if os.path.isdir(directory_path):
        if os.listdir(directory_path):
            raise FileExistsError(f"The directory '{directory_path}' already contains files.")
        shutil.rmtree(directory_path)
    os.makedirs(directory_path, exist_ok=True)

    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cursor:
            cursor.itersize = batch_size

            # Парсинг схемы Avro
            parsed_schema = parse_schema(avro_schema)

            # Выполнение SQL-запроса
            cursor.execute(sql)

            batch_data = []
            file_counter = 1

            # Потоковая обработка данных
            for row in cursor:
                batch_data.append(dict(row))

                # Если накопился батч, записываем его в файл
                if len(batch_data) >= batch_size:
                    file_path = os.path.join(directory_path, f'batch_{file_counter}.avro')
                    with open(file_path, 'wb') as f:
                        writer(f, parsed_schema, batch_data)
                    logger.info(f"Saved batch {file_counter} to {file_path}")

                    batch_data = []
                    file_counter += 1

            # Запись оставшихся данных
            if batch_data:
                file_path = os.path.join(directory_path, f'batch_{file_counter}.avro')
                with open(file_path, 'wb') as f:
                    writer(f, parsed_schema, batch_data)
                logger.info(f"Saved final batch {file_counter} to {file_path}")

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise
    finally:
        if conn:
            conn.close()

    return directory_path


@task(retries=3, retry_delay_seconds=300)
def send_messages_to_kafka_sync(directory_path: str, topic_name: str, task_format: str):
    """Синхронная отправка сообщений в Kafka"""
    logger = get_run_logger()

    if task_format not in ('avro', 'json'):
        raise ValueError(f"Unsupported format: {task_format}. Must be 'avro' or 'json'")

    producer = SyncKafkaProducer(**KAFKA_CONFIG)
    files_processed = 0

    try:
        for filename in sorted(os.listdir(directory_path)):
            file_path = os.path.join(directory_path, filename)

            try:
                with open(file_path, 'rb') as f:
                    file_data = f.read()

                    if not file_data:
                        logger.warning(f"No data in {filename}")
                        continue

                    if task_format == 'json':
                        try:
                            # Преобразуем Avro в JSON
                            avro_file = BytesIO(file_data)
                            reader = fastavro.reader(avro_file)
                            records = [record for record in reader]
                            message_value = json.dumps(records).encode('utf-8')
                        except Exception as e:
                            logger.error(f"Failed to convert Avro to JSON for {filename}: {e}")
                            continue
                    else:
                        message_value = file_data

                    # Отправка в Kafka
                    future = producer.send(
                        topic=topic_name,
                        value=message_value,
                        key=None
                    )

                    # Ждем подтверждения
                    future.get(timeout=10)

                    logger.info(f"Sent {filename} to Kafka topic {topic_name}")

                # Удаление файла после успешной отправки
                os.remove(file_path)
                files_processed += 1

            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                raise

    finally:
        producer.close()

    return files_processed


@task
def cleanup_directory(directory_path: str):
    """Очистка директории после обработки"""
    logger = get_run_logger()

    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        logger.info(f"Cleaned up directory: {directory_path}")


@flow(name="odata-task-processor", retries=2, retry_delay_seconds=600)
def process_odata_task(task_params: Dict[str, Any]):
    """Flow для обработки одной OData задачи"""
    logger = get_run_logger()

    logger.info(f"Starting processing for task: {task_params['task_name']}")

    try:
        # Шаг 1: Извлечение данных и сохранение в файлы
        directory_path = fetch_data_and_save_to_files(
            task_params['directory_path'],
            task_params['sql'],
            task_params['avro_schema'],
            task_params.get('batch_size', 1000)
        )

        # Шаг 2: Отправка в Kafka
        files_sent = send_messages_to_kafka_sync(
            directory_path,
            task_params['topic_name'],
            task_params['task_format']
        )

        # Шаг 3: Очистка
        cleanup_directory(directory_path)

        logger.info(f"Successfully processed {files_sent} files for task: {task_params['task_name']}")

        return files_sent

    except Exception as e:
        logger.error(f"Failed to process task {task_params['task_name']}: {e}")
        # Очищаем директорию в случае ошибки
        if 'directory_path' in locals():
            cleanup_directory(directory_path)
        raise


@flow(name="odata-tasks-generator")
def odata_tasks_generator():
    """Основной flow для генерации OData задач"""
    logger = get_run_logger()

    logger.info("Starting OData tasks generator")

    try:
        # Получаем параметры задач из БД
        tasks_params = get_params_from_db()

        if not tasks_params:
            logger.info("No enabled tasks found")
            return

        logger.info(f"Found {len(tasks_params)} tasks to process")

        # Обрабатываем каждую задачу
        results = []
        for task_params in tasks_params:
            result = process_odata_task.with_options(
                name=f"process-{task_params['task_name']}"
            )(task_params)
            results.append(result)

        logger.info(f"Completed processing {len(results)} tasks")

        return results

    except Exception as e:
        logger.error(f"OData tasks generator failed: {e}")
        raise


if __name__ == "__main__":
    odata_tasks_generator()
