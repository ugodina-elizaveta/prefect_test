FROM prefecthq/prefect:2-python3.11

# Устанавливаем дополнительные зависимости
RUN pip install --no-cache-dir --upgrade pip && \
    pip install psycopg2-binary fastavro kafka-python

# Копируем flow файлы
COPY flows/ /opt/prefect/flows/

# Создаем рабочую директорию
WORKDIR /opt/prefect