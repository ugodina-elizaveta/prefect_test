from prefect import flow, get_run_logger
from typing import Dict, Any
from .tasks import fetch_data_and_save_to_files, send_messages_to_kafka


def create_task_subflow(task_config: Dict[str, Any]):
    """
    Фабрика для создания subflow с динамическим именем
    """
    task_name = task_config['task_name']

    @flow(
        name=f"process-{task_name}",
        description=f"Subflow for {task_name} - extracts data and sends to Kafka",
        retries=1,
        retry_delay_seconds=300
    )
    def task_subflow():
        """
        Subflow для обработки одной конкретной задачи.
        """
        logger = get_run_logger()

        logger.info(f"🚀 Starting subflow for: {task_name}")

        try:
            # Шаг 1: Извлечение данных
            logger.info(f"📥 Fetching data for: {task_name}")
            fetch_result = fetch_data_and_save_to_files(task_config)

            # Шаг 2: Отправка в Kafka
            logger.info(f"📤 Sending to Kafka for: {task_name}")
            send_result = send_messages_to_kafka(task_config)

            logger.info(f"✅ Completed subflow for: {task_name}")
            return {
                "task_name": task_name,
                "fetch_result": fetch_result,
                "send_result": send_result,
                "status": "success"
            }

        except Exception as e:
            logger.error(f"❌ Subflow failed for {task_name}: {e}")
            raise

    return task_subflow
