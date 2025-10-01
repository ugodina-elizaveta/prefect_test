from prefect import flow, get_run_logger
from typing import Dict, Any
from .tasks import fetch_data_task, send_to_kafka_task


@flow(
    name="process-{task_name}",
    description="Subflow for processing {task_name} - extracts data and sends to Kafka",
    retries=1,
    retry_delay_seconds=300
)
def process_single_task_subflow(task_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Subflow для обработки одной конкретной задачи.
    """
    logger = get_run_logger()
    task_name = task_config['task_name']

    logger.info(f"🚀 Starting subflow for: {task_name}")

    try:
        # Шаг 1: Извлечение данных
        logger.info(f"📥 Fetching data for: {task_name}")
        fetch_result = fetch_data_task(task_config)

        # Шаг 2: Отправка в Kafka
        logger.info(f"📤 Sending to Kafka for: {task_name}")
        send_result = send_to_kafka_task(task_config)

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
