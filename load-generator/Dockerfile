FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей
COPY load-generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY load-generator/generator.py .

# Запуск генератора
CMD ["python", "generator.py"]

