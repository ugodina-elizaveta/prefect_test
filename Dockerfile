FROM prefecthq/prefect:3-python3.11
WORKDIR /opt/prefect
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /opt/prefect/data