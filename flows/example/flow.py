from prefect import flow, task, get_run_logger
from prefect.context import get_run_context


@task
def bar():
    logger = get_run_logger()
    task_run_name = get_run_context().task_run.name
    logger.info(f"Hi from {task_run_name}!")


@flow
def foo():
    bar()
