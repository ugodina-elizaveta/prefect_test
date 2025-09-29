FROM prefecthq/prefect:3-python3.11

WORKDIR /opt/prefect

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ВЕСЬ проект
COPY . .

# Создаем необходимые директории
RUN mkdir -p /opt/prefect/data

# Устанавливаем рабочую директорию
WORKDIR /opt/prefect

# Проверяем что файлы скопированы
RUN ls -la /opt/prefect/ && \
    ls -la /opt/prefect/scripts/ && \
    ls -la /opt/prefect/flows/