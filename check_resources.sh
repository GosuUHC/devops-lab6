#!/bin/bash
# Скрипт для проверки потребления ресурсов контейнерами

echo "============================================================"
echo "Проверка потребления ресурсов контейнерами"
echo "============================================================"
echo ""

# Функция для форматирования вывода
format_output() {
    echo "------------------------------------------------------------"
    echo "$1"
    echo "------------------------------------------------------------"
}

# Проверка статуса контейнеров
format_output "1. Статус контейнеров"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "zookeeper|kafka|redis|flink|model-server"

echo ""
echo ""

# Проверка использования CPU и памяти
format_output "2. Использование CPU и памяти (текущее)"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | grep -E "NAME|zookeeper|kafka|redis|flink|model-server"

echo ""
echo ""

# Проверка лимитов ресурсов
format_output "3. Лимиты ресурсов контейнеров"
for container in zookeeper kafka redis flink-jobmanager flink-taskmanager model-server; do
    if docker inspect "$container" &>/dev/null; then
        echo "--- $container ---"
        docker inspect "$container" --format '{{json .HostConfig}}' | python3 -m json.tool | grep -E "CpuShares|Memory|CpuQuota|CpuPeriod" || echo "  Лимиты не установлены"
        echo ""
    fi
done

echo ""

# Проверка размера образов
format_output "4. Размеры Docker образов"
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}" | grep -E "REPOSITORY|devops-lab6|flink|kafka|redis|model-server"

echo ""
echo ""

# Проверка использования дискового пространства
format_output "5. Использование дискового пространства"
docker system df

echo ""
echo "============================================================"
echo "Проверка завершена!"
echo "============================================================"

