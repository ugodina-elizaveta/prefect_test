from prefect import flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from .tasks import get_params
from .subflows import process_single_task_subflow


@flow(
    name="odata-tasks-generator",
    task_runner=ConcurrentTaskRunner(),
    retries=1,
    retry_delay_seconds=300,
    description="Master flow for OData ETL tasks"
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

        logger.info(f"Found {len(params_list)} enabled tasks")

        # Запускаем subflow для каждой задачи
        results = []
        for param in params_list:
            result = process_single_task_subflow(param)
            results.append(result)

        logger.info("All tasks completed successfully")
        return results

    except Exception as e:
        logger.error(f"Master flow failed: {e}")
        raise
