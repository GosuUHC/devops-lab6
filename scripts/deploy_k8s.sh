#!/bin/bash

# Скрипт для развертывания Big Data стека в Kubernetes
# Использует манифесты из папки kubernetes/

set -e

KUBERNETES_DIR="kubernetes"
NAMESPACE="bigdata"

echo "========================================="
echo "Развертывание Big Data стека в Kubernetes"
echo "========================================="
echo ""

# Проверка наличия kubectl
if ! command -v kubectl &> /dev/null; then
    echo "Ошибка: kubectl не установлен"
    exit 1
fi

# Проверка подключения к кластеру
if ! kubectl cluster-info &> /dev/null; then
    echo "Ошибка: не удалось подключиться к Kubernetes кластеру"
    echo "Убедитесь, что кластер запущен и kubectl настроен"
    exit 1
fi

echo "1. Создание namespace..."
kubectl apply -f ${KUBERNETES_DIR}/00-namespace.yaml

echo ""
echo "2. Применение конфигурации Flink..."
kubectl apply -f ${KUBERNETES_DIR}/01-flink-config.yaml

echo ""
echo "3. Развертывание Flink JobManager..."
kubectl apply -f ${KUBERNETES_DIR}/02-flink-jobmanager.yaml

echo ""
echo "4. Развертывание Flink TaskManager StatefulSet..."
kubectl apply -f ${KUBERNETES_DIR}/03-flink-taskmanager-statefulset.yaml

echo ""
echo "5. Создание Service для TaskManager..."
kubectl apply -f ${KUBERNETES_DIR}/04-flink-service.yaml

echo ""
echo "Ожидание готовности подов..."
kubectl wait --for=condition=ready pod -l app=flink,component=jobmanager -n ${NAMESPACE} --timeout=300s || true
kubectl wait --for=condition=ready pod -l app=flink,component=taskmanager -n ${NAMESPACE} --timeout=300s || true

echo ""
echo "========================================="
echo "Развертывание завершено"
echo "========================================="
echo ""
echo "Проверка статуса:"
kubectl get pods -n ${NAMESPACE}
echo ""
echo "Для доступа к Flink UI:"
echo "  kubectl port-forward -n ${NAMESPACE} service/flink-jobmanager 8081:8081"
echo ""
echo "Для просмотра логов:"
echo "  kubectl logs -n ${NAMESPACE} deployment/flink-jobmanager"
echo "  kubectl logs -n ${NAMESPACE} statefulset/flink-taskmanager -c taskmanager"



