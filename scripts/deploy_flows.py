#!/usr/bin/env python3
"""
Скрипт для автоматического деплоя всех flows с их prefect.yaml файлами.
"""

import os
import subprocess
import sys
from pathlib import Path


def find_prefect_files(flows_dir: str) -> list:
    """Находит все prefect.yaml файлы в директории flows."""
    prefect_files = []
    flows_path = Path(flows_dir)

    for item in flows_path.iterdir():
        if item.is_dir():
            prefect_file = item / "prefect.yaml"
            if prefect_file.exists():
                prefect_files.append(str(prefect_file))

    return prefect_files


def deploy_flow(prefect_file: str) -> bool:
    """Деплоит flow используя prefect.yaml файл."""
    try:
        print(f"Deploying {prefect_file}...")
        result = subprocess.run(
            ["prefect", "deploy", "--prefect-file", prefect_file],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✅ Successfully deployed {prefect_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to deploy {prefect_file}: {e.stderr}")
        return False


def create_work_pools_from_yaml(prefect_file: str) -> None:
    """Извлекает work pools из prefect.yaml и создает их."""
    try:
        import yaml
        with open(prefect_file, 'r') as f:
            config = yaml.safe_load(f)

        deployments = config.get('deployments', [])
        pools = set()

        for deployment in deployments:
            work_pool = deployment.get('work_pool', {})
            pool_name = work_pool.get('name')
            if pool_name:
                pools.add(pool_name)

        for pool_name in pools:
            try:
                subprocess.run(
                    ["prefect", "work-pool", "create", "--type", "process", pool_name],
                    capture_output=True,
                    check=True
                )
                print(f"✅ Created work pool: {pool_name}")
            except subprocess.CalledProcessError:
                print(f"ℹ️  Work pool {pool_name} already exists or failed to create")

    except Exception as e:
        print(f"⚠️  Could not process work pools from {prefect_file}: {e}")


def main():

    # Проверка файлов
    print("🔍 Checking project structure...")
    flows_dir = "/opt/prefect/flows"

    print("Current directory:", os.getcwd())
    print("Files in /opt/prefect:")
    os.system("ls -la /opt/prefect/")
    print("Files in scripts directory:")
    os.system("ls -la /opt/prefect/scripts/")

    if not os.path.exists(flows_dir):
        print(f"❌ Flows directory not found: {flows_dir}")
        sys.exit(1)
    flows_dir = "/opt/prefect/flows"

    if not os.path.exists(flows_dir):
        print(f"❌ Flows directory not found: {flows_dir}")
        sys.exit(1)

    prefect_files = find_prefect_files(flows_dir)

    if not prefect_files:
        print("ℹ️  No prefect.yaml files found in flows directory")
        return

    print(f"Found {len(prefect_files)} prefect.yaml files")

    # Создаем work pools
    for prefect_file in prefect_files:
        create_work_pools_from_yaml(prefect_file)

    # Деплоим flows
    success_count = 0
    for prefect_file in prefect_files:
        if deploy_flow(prefect_file):
            success_count += 1

    print(f"\n🎉 Deployment complete: {success_count}/{len(prefect_files)} flows deployed successfully")


if __name__ == "__main__":
    main()
