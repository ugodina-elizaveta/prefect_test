#!/usr/bin/env python3
"""
Скрипт для запуска воркеров для всех work pools, найденных в prefect.yaml файлах.
"""

import os
import subprocess
import threading
import time
import yaml
from pathlib import Path


def find_work_pools(flows_dir: str) -> set:
    """Находит все work pools из prefect.yaml файлов."""
    pools = set()
    flows_path = Path(flows_dir)

    for item in flows_path.iterdir():
        if item.is_dir():
            prefect_file = item / "prefect.yaml"
            if prefect_file.exists():
                try:
                    with open(prefect_file, 'r') as f:
                        config = yaml.safe_load(f)

                    deployments = config.get('deployments', [])
                    for deployment in deployments:
                        work_pool = deployment.get('work_pool', {})
                        pool_name = work_pool.get('name')
                        if pool_name:
                            pools.add(pool_name)
                except Exception as e:
                    print(f"⚠️  Could not parse {prefect_file}: {e}")

    return pools


def start_worker(pool_name: str):
    """Запускает воркер для указанного пула."""
    print(f"🚀 Starting worker for pool: {pool_name}")
    try:
        subprocess.run(
            ["prefect", "worker", "start", "--pool", pool_name],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"❌ Worker for pool {pool_name} failed: {e}")
    except KeyboardInterrupt:
        print(f"🛑 Worker for pool {pool_name} stopped")


def main():
    flows_dir = "/opt/prefect/flows"

    if not os.path.exists(flows_dir):
        print(f"❌ Flows directory not found: {flows_dir}")
        return

    pools = find_work_pools(flows_dir)

    if not pools:
        print("ℹ️  No work pools found, starting default worker")
        start_worker("default-pool")
        return

    print(f"Found work pools: {', '.join(pools)}")

    # Если только один пул, запускаем в основном потоке
    if len(pools) == 1:
        pool_name = list(pools)[0]
        start_worker(pool_name)
        return

    # Если несколько пулов, запускаем в отдельных потоках
    threads = []
    for pool_name in pools:
        thread = threading.Thread(target=start_worker, args=(pool_name,))
        thread.daemon = True
        thread.start()
        threads.append(thread)
        time.sleep(2)  # Небольшая задержка между запусками

    try:
        # Ждем завершения всех потоков
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all workers...")


if __name__ == "__main__":
    main()
