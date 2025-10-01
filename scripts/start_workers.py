#!/usr/bin/env python3
"""
Скрипт для запуска воркеров для всех work pools.
"""

import os
import subprocess
import sys
import threading
import time
import yaml
from pathlib import Path


def cleanup_old_workers():
    """Очищает старых оффлайн воркеров."""
    print("🧹 Cleaning up old offline workers...")
    try:
        # Получаем список всех воркеров
        result = subprocess.run(
            ["prefect", "worker", "ls", "--json"],
            capture_output=True,
            text=True,
            check=True
        )

        import json
        workers = json.loads(result.stdout)

        for worker in workers:
            if worker.get('status') == 'OFFLINE':
                worker_name = worker.get('name')
                print(f"🗑️  Removing offline worker: {worker_name}")
                subprocess.run(
                    ["prefect", "worker", "delete", "--name", worker_name],
                    capture_output=True
                )

    except Exception as e:
        print(f"⚠️  Could not cleanup old workers: {e}")


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


def start_worker_process(pool_name: str):
    """Запускает воркер в отдельном процессе."""
    print(f"🚀 Starting worker for pool: {pool_name}")
    try:
        process = subprocess.Popen(
            ["prefect", "worker", "start", "--pool", pool_name, "--name", f"worker-{pool_name}-{int(time.time())}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        # Читаем вывод в реальном времени
        def read_output(stream, prefix):
            for line in iter(stream.readline, ''):
                if line:
                    print(f"[{prefix}] {line.strip()}")

        stdout_thread = threading.Thread(target=read_output, args=(process.stdout, pool_name))
        stderr_thread = threading.Thread(target=read_output, args=(process.stderr, f"{pool_name}-ERROR"))

        stdout_thread.daemon = True
        stderr_thread.daemon = True

        stdout_thread.start()
        stderr_thread.start()

        return process

    except Exception as e:
        print(f"❌ Failed to start worker for pool {pool_name}: {e}")
        return None


def main():
    print("🔍 Looking for work pools...")
    flows_dir = "/opt/prefect/flows"

    if not os.path.exists(flows_dir):
        print(f"❌ Flows directory not found: {flows_dir}")
        sys.exit(1)

    # Очищаем старых воркеров перед запуском новых
    cleanup_old_workers()

    pools = find_work_pools(flows_dir)

    if not pools:
        print("⚠️  No work pools found, exiting...")
        return

    processes = []

    # Запускаем всех воркеров
    for pool_name in pools:
        process = start_worker_process(pool_name)
        if process:
            processes.append((pool_name, process))
        time.sleep(3)  # Задержка между запусками

    print(f"✅ Started {len(processes)} workers")

    # Мониторим процессы
    try:
        while True:
            time.sleep(10)

            # Проверяем статус процессов
            for pool_name, process in processes:
                return_code = process.poll()
                if return_code is not None:
                    print(f"⚠️  Worker for {pool_name} died with code {return_code}, restarting...")
                    processes.remove((pool_name, process))
                    new_process = start_worker_process(pool_name)
                    if new_process:
                        processes.append((pool_name, new_process))

    except KeyboardInterrupt:
        print("\n🛑 Stopping all workers...")
        for pool_name, process in processes:
            process.terminate()
        for pool_name, process in processes:
            process.wait()


if __name__ == "__main__":
    main()
