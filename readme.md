Запустите сервисы:
```docker-compose up -d```

Дождитесь запуска всех сервисов и создайте deployment:
```docker exec -it prefect-worker bash```
```cd /opt/prefect/flows```
```python deploy.py```

Откройте Prefect UI:
Перейдите по адресу: http://localhost:4200

Вы увидите ваш deployment в разделе "Deployments"