from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from odata_tasks_flow import odata_tasks_flow
from example import foo


def create_deployment(flow, deployment_config):
    deployment = Deployment.build_from_flow(
        flow=flow,
        **deployment_config
    )
    deployment.apply()
    return deployment


if __name__ == "__main__":
    deployments_config = [
        {
            "flow": odata_tasks_flow,
            "name": "odata-tasks-deployment",
            "schedules": CronSchedule(cron="03 23 * * *"),   # Запуск в 23:03 каждый день
            "work_pool_name": "default-pool",
            "tags": ["odata", "kafka", "etl"]
        },
        {
            "flow": foo,
            "name": "another-flow-deployment", 
            "schedules": CronSchedule(cron="0 2 * * *"),
            "work_pool_name": "default-pool",
            "tags": ["test"]
        }
    ]

    for config in deployments_config:
        deployment = create_deployment(**config)
        print(f"Deployment {config['name']} created successfully!")
