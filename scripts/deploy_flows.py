#!/usr/bin/env python3
"""
Скрипт для автоматического деплоя всех flows с их prefect.yaml файлами.
"""

import os
import subprocess
import sys
import time
import yaml
from pathlib import Path


def wait_for_server(max_retries=30, delay=5):
    """Ждет пока Prefect сервер станет доступен."""
    print("⏳ Waiting for Prefect server to be ready...")

    for i in range(max_retries):
        try:
            result = subprocess.run(
                ["prefect", "config", "view"],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                print("✅ Prefect server is ready!")
                return True
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            pass

        print(f"⏰ Server not ready yet (attempt {i+1}/{max_retries})...")
        time.sleep(delay)

    print("❌ Prefect server did not become ready in time")
    return False


def find_prefect_files(flows_dir: str) -> list:
    """Находит все prefect.yaml файлы в директории flows."""
    prefect_files = []
    flows_path = Path(flows_dir)

    # Ищем prefect.yaml в корне flows/odata/
    for item in flows_path.iterdir():
        if item.is_dir():
            # Проверяем корневую директорию flow
            prefect_file = item / "prefect.yaml"
            if prefect_file.exists():
                prefect_files.append(str(prefect_file))

            # Также проверяем вложенные директории (на всякий случай)
            for sub_item in item.iterdir():
                if sub_item.is_dir():
                    sub_prefect_file = sub_item / "prefect.yaml"
                    if sub_prefect_file.exists():
                        prefect_files.append(str(sub_prefect_file))

    return prefect_files


def create_work_pool(pool_name: str, max_retries=5) -> bool:
    """Создает work pool с retry логикой."""
    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                ["prefect", "work-pool", "create", "--type", "process", pool_name],
                capture_output=True,
                text=True,
                check=True
            )
            print(f"✅ Created work pool: {pool_name}")
            return True
        except subprocess.CalledProcessError as e:
            if "already exists" in e.stderr.lower():
                print(f"ℹ️  Work pool {pool_name} already exists")
                return True
            elif attempt < max_retries - 1:
                print(f"⚠️  Failed to create work pool {pool_name} (attempt {attempt+1}), retrying...")
                time.sleep(2)
            else:
                print(f"❌ Failed to create work pool {pool_name} after {max_retries} attempts: {e.stderr}")
                return False
    return False


def deploy_flow(prefect_file: str, max_retries=3) -> bool:
    """Деплоит flow используя prefect.yaml файл с retry логикой."""
    for attempt in range(max_retries):
        try:
            print(f"🚀 Deploying {prefect_file} (attempt {attempt+1}/{max_retries})...")
            result = subprocess.run(
                ["prefect", "deploy", "--prefect-file", prefect_file],
                capture_output=True,
                text=True,
                check=True,
                timeout=120
            )
            print(f"✅ Successfully deployed {prefect_file}")
            return True
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.lower()
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"⚠️  Deployment failed (attempt {attempt+1}), retrying in {wait_time}s...")
                print(f"   Error: {e.stderr.strip()}")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed to deploy {prefect_file} after {max_retries} attempts: {e.stderr}")
                return False
        except subprocess.TimeoutExpired:
            if attempt < max_retries - 1:
                print(f"⚠️  Deployment timeout (attempt {attempt+1}), retrying...")
            else:
                print(f"❌ Deployment timeout for {prefect_file} after {max_retries} attempts")
                return False
    return False


def create_work_pools_from_yaml(prefect_file: str) -> None:
    """Извлекает work pools из prefect.yaml и создает их."""
    try:
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
            create_work_pool(pool_name)

    except Exception as e:
        print(f"⚠️  Could not process work pools from {prefect_file}: {e}")


def main():
    # Ждем пока сервер станет доступен
    if not wait_for_server():
        print("❌ Cannot continue without Prefect server")
        sys.exit(1)

    # Проверка файлов
    print("🔍 Checking project structure...")
    flows_dir = "/opt/prefect/flows"

    if not os.path.exists(flows_dir):
        print(f"❌ Flows directory not found: {flows_dir}")
        sys.exit(1)

    prefect_files = find_prefect_files(flows_dir)

    if not prefect_files:
        print("ℹ️  No prefect.yaml files found in flows directory")
        return

    print(f"📁 Found {len(prefect_files)} prefect.yaml files")

    # Создаем work pools
    for prefect_file in prefect_files:
        create_work_pools_from_yaml(prefect_file)

    # Деплоим flows
    success_count = 0
    for prefect_file in prefect_files:
        if deploy_flow(prefect_file):
            success_count += 1

    print(f"\n🎉 Deployment complete: {success_count}/{len(prefect_files)} flows deployed successfully")

    if success_count < len(prefect_files):
        sys.exit(1)  # Exit with error code if not all flows deployed


if __name__ == "__main__":
    main()
