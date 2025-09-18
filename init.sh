#!/bin/bash
# Ждем запуска сервера
echo "Waiting for Prefect server to be ready..."
sleep 30

# Создаем work pool
prefect work-pool create default --type process

# Развертываем flow
python /opt/prefect/flows/deploy_flow.py

# Запускаем worker
prefect worker start --pool default --name my-worker