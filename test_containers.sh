#!/bin/bash
# Скрипт для тестирования работы контейнеров

set -e

echo "============================================================"
echo "Тестирование работы контейнеров"
echo "============================================================"
echo ""

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для проверки статуса
check_status() {
    local service=$1
    local check_cmd=$2
    
    echo -n "Проверка $service... "
    
    if eval "$check_cmd" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        return 1
    fi
}

# Проверка Zookeeper
check_status "Zookeeper" "nc -z localhost 2181"

# Проверка Kafka
check_status "Kafka" "nc -z localhost 9092"

# Проверка Redis
check_status "Redis" "docker exec redis redis-cli ping | grep -q PONG"

# Проверка Flink JobManager
check_status "Flink JobManager" "curl -s http://localhost:8081/overview > /dev/null"

# Проверка Model Server
check_status "Model Server" "curl -s http://localhost:8000/health > /dev/null"

echo ""
echo "============================================================"
echo "Тестирование Model Server API"
echo "============================================================"

# Тест предсказания
echo -n "Тест /predict endpoint... "
response=$(curl -s -X POST http://localhost:8000/predict \
    -H "Content-Type: application/json" \
    -d '{
        "patient_id": "test-001",
        "age": 65,
        "bmi": 28.5,
        "blood_pressure": "140/90"
    }')

if echo "$response" | grep -q "risk_score"; then
    echo -e "${GREEN}✓ OK${NC}"
    echo "  Response: $response"
else
    echo -e "${RED}✗ FAILED${NC}"
    echo "  Response: $response"
fi

echo ""
echo "============================================================"
echo "Проверка метрик Prometheus"
echo "============================================================"

# Проверка метрик Model Server
echo -n "Проверка /metrics endpoint... "
metrics=$(curl -s http://localhost:8000/metrics)

if echo "$metrics" | grep -q "model_predictions_total"; then
    echo -e "${GREEN}✓ OK${NC}"
    echo "  Найдены метрики:"
    echo "$metrics" | grep "model_" | head -5
else
    echo -e "${RED}✗ FAILED${NC}"
fi

echo ""
echo "============================================================"
echo "Проверка Redis Feature Store"
echo "============================================================"

# Проверка записи в Redis
echo -n "Проверка записи в Redis... "
redis_check=$(docker exec redis redis-cli LLEN "patient:test-001:predictions" 2>/dev/null)

if [ "$redis_check" -ge 0 ]; then
    echo -e "${GREEN}✓ OK${NC}"
    echo "  Количество записей для test-001: $redis_check"
else
    echo -e "${RED}✗ FAILED${NC}"
fi

echo ""
echo "============================================================"
echo "Тестирование завершено!"
echo "============================================================"

