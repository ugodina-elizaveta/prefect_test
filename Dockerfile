FROM prefecthq/prefect:3-python3.11

# Устанавливаем рабочую директорию
WORKDIR /opt/prefect

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ВЕСЬ проект
COPY . .

# Создаем необходимые директории
RUN mkdir -p /opt/prefect/data