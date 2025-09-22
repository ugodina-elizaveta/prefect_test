from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from odata_tasks_flow import odata_tasks_flow

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=odata_tasks_flow,
        name="odata-tasks-deployment",
        schedule=CronSchedule(cron="03 23 * * *"),  # Запуск в 23:03 каждый день
        work_pool_name="default-pool",
        tags=["odata", "kafka", "etl"]
    )

    deployment.apply()
    print("Deployment created successfully!")
