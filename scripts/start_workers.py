#!/usr/bin/env python3
"""
Скрипт для запуска воркеров для всех work pools.
"""

import os
import subprocess
import sys
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


def main():
    print("🔍 Looking for work pools...")
    flows_dir = "/opt/prefect/flows"

    if not os.path.exists(flows_dir):
        print(f"❌ Flows directory not found: {flows_dir}")
        sys.exit(1)

    pools = find_work_pools(flows_dir)

    if not pools:
        print("⚠️  No work pools found, exiting...")
        return

    print(f"🎯 Starting workers for pools: {', '.join(pools)}")

    # Запускаем воркеров последовательно (они работают в foreground)
    for pool_name in pools:
        print(f"🚀 Starting worker for pool: {pool_name}")
        try:
            # Этот процесс будет работать пока не будет остановлен
            subprocess.run(
                ["prefect", "worker", "start", "--pool", pool_name],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"❌ Worker for pool {pool_name} failed: {e}")
            # Продолжаем с следующим воркером
            continue
        except KeyboardInterrupt:
            print(f"🛑 Worker for pool {pool_name} stopped by user")
            break


if __name__ == "__main__":
    main()
