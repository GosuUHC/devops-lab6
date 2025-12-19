# 6. DevOps для Big Data - развёртка, мониторинг и управление ресурсами

#### Датасет:

Вариант 6 - [Медицинские данные о повторных госпитализациях](https://www.kaggle.com/datasets/siddharth0935/hospital-readmission-predictionsynthetic-dataset)

#### Бизнес-сценарий

3. Прогнозирование спроса
- Ежедневный прогноз готов к 6:00 утра
- Точность (MAPE) < 15%
- SLA: 99% доступности

#### Уникальное требование по cost optimization

- 6. Снизить затраты на обработку на 35% через правильные размеры
инстансов

Размеры образов до и после оптимизации

```bash
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
```
Или
```bash
python compare_image_sizes.py
```

| Образ | До оптимизации | После оптимизации | Экономия |
|-------|----------------|-------------------|----------|
| Flink | 2.1GB | 1.68GB | 420MB (20%) |
| Kafka | 852MB | 590MB | 262MB (31%) |
| Model Server | 1.33GB | 954MB | 376MB (28%) |
| Redis | 187MB | 67.2MB | 119.8MB (64%) |
| **ИТОГО** | **~4.47GB** | **~3.28GB** | **~1.19GB (27%)** |

Конфигурация лимитов ресурсов

Все компоненты настроены с лимитами ресурсов в `docker-compose.yml`:

| Компонент | CPU Limit | Memory Limit | CPU Reservation | Memory Reservation |
|-----------|-----------|--------------|-----------------|-------------------|
| Zookeeper | 1 CPU | 1GB | 0.5 CPU | 512MB |
| Kafka | 4 CPU | 4GB | 2 CPU | 2GB |
| Redis | 2 CPU | 1GB | 1 CPU | 512MB |
| Flink JobManager | 4 CPU | 4GB | 2 CPU | 2GB |
| Flink TaskManager | 8 CPU | 16GB | 4 CPU | 8GB |
| Model Server | 2 CPU | 2GB | 1 CPU | 1GB |

Потребление ресурсов:

| Container | CPU % | Memory % | Memory Usage |
|-----------|-------|----------|--------------|
| zookeeper | 0.06 | 12.50 | 128MiB / 1GiB |
| kafka | 2.22 | 7.40 | 303.2MiB / 4GiB |
| redis | 0.08 | 0.62 | 6.371MiB / 1GiB |
| flink-jobmanager | 2.87 | 8.86 | 362.8MiB / 4GiB |
| flink-taskmanager | 0.58 | 5.02 | 392.7MiB / 7.645GiB |
| model-server | 0.11 | 1.97 | 40.32MiB / 2GiB |