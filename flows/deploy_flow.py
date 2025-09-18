#!/usr/bin/env python3
import asyncio
from prefect.deployments import Deployment
from prefect.client.orchestration import get_client
from odata_tasks_generator import odata_tasks_generator

async def create_work_pool_if_not_exists():
    """Создает work pool если он не существует"""
    try:
        async with get_client() as client:
            try:
                await client.read_work_pool("default")
                print("Work pool 'default' already exists")
            except Exception:
                await client.create_work_pool(
                    name="default",
                    type="process",
                    description="Default process work pool"
                )
                print("Work pool 'default' created successfully")
    except Exception as e:
        print(f"Error checking/creating work pool: {e}")
        # Продолжаем выполнение, так как worker может создать пул

async def deploy_flow():
    try:
        # Создаем work pool если нужно
        await create_work_pool_if_not_exists()

        # Создаем deployment
        deployment = await Deployment.build_from_flow(
            flow=odata_tasks_generator,
            name="odata-tasks-generator",
            work_pool_name="default",
            parameters={},
            tags=["odata", "etl"],
            apply=True
        )

        print("Deployment created successfully!")
        return True

    except Exception as e:
        print(f"Error creating deployment: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(deploy_flow())
    exit(0 if success else 1)