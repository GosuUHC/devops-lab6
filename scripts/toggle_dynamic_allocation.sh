#!/bin/bash
# Скрипт для включения/выключения dynamic allocation в Flink

set -e

NAMESPACE="bigdata"
CONFIG_MAP="flink-config"
STATEFULSET="flink-taskmanager"

# Проверка доступности кластера
if ! kubectl get nodes >/dev/null 2>&1; then
    echo "❌ Ошибка: Kubernetes кластер недоступен"
    echo "Проверьте, что кластер запущен и kubectl настроен правильно"
    exit 1
fi

if [ "$1" == "enable" ]; then
    echo "Включение Dynamic Allocation..."
    TMP_CONF=$(mktemp)
    TMP_YAML=$(mktemp)
    cat > "$TMP_CONF" <<'EOF'
jobmanager.rpc.address: flink-jobmanager
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 4096m
taskmanager.memory.process.size: 8192m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 8
state.backend: hashmap
state.checkpoints.dir: file:///opt/flink/checkpoints
state.savepoints.dir: file:///opt/flink/savepoints
state.savepoint.default.savepoint.path: file:///opt/flink/savepoints
python.client.executable: /usr/bin/python3
python.executable: /usr/bin/python3
metrics.reporters: prom
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9250
metrics.reporter.prom.host: 0.0.0.0
# Dynamic allocation для Flink (аналог Spark dynamic allocation)
kubernetes.taskmanager.min: 2
kubernetes.taskmanager.max: 10
kubernetes.taskmanager.initial: 4
kubernetes.taskmanager.auto-scale.enabled: "true"
kubernetes.taskmanager.auto-scale.cpu-threshold: "0.8"
kubernetes.taskmanager.auto-scale.cpu-threshold-down: "0.3"
kubernetes.taskmanager.auto-scale.metrics.window: "5m"
kubernetes.taskmanager.auto-scale.metrics.interval: "30s"
kubernetes.taskmanager.auto-scale.memory-threshold: "0.85"
kubernetes.taskmanager.auto-scale.memory-threshold-down: "0.4"
EOF
    kubectl create configmap $CONFIG_MAP -n $NAMESPACE --from-file=flink-conf.yaml="$TMP_CONF" --dry-run=client -o yaml > "$TMP_YAML"
    kubectl apply -f "$TMP_YAML"
    rm -f "$TMP_CONF" "$TMP_YAML"
    echo "✅ Dynamic Allocation включен"
    echo "Перезапуск TaskManagers..."
    kubectl rollout restart statefulset/$STATEFULSET -n $NAMESPACE
    echo "Ожидание готовности подов..."
    kubectl rollout status statefulset/$STATEFULSET -n $NAMESPACE

elif [ "$1" == "disable" ]; then
    echo "Выключение Dynamic Allocation..."
    TMP_CONF=$(mktemp)
    TMP_YAML=$(mktemp)
    cat > "$TMP_CONF" <<'EOF'
jobmanager.rpc.address: flink-jobmanager
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 4096m
taskmanager.memory.process.size: 8192m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 8
state.backend: hashmap
state.checkpoints.dir: file:///opt/flink/checkpoints
state.savepoints.dir: file:///opt/flink/savepoints
state.savepoint.default.savepoint.path: file:///opt/flink/savepoints
python.client.executable: /usr/bin/python3
python.executable: /usr/bin/python3
metrics.reporters: prom
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9250
metrics.reporter.prom.host: 0.0.0.0
EOF
    kubectl create configmap $CONFIG_MAP -n $NAMESPACE --from-file=flink-conf.yaml="$TMP_CONF" --dry-run=client -o yaml > "$TMP_YAML"
    kubectl apply -f "$TMP_YAML"
    rm -f "$TMP_CONF" "$TMP_YAML"
    echo "✅ Dynamic Allocation выключен"
    echo "Перезапуск TaskManagers..."
    kubectl rollout restart statefulset/$STATEFULSET -n $NAMESPACE
    echo "Ожидание готовности подов..."
    kubectl rollout status statefulset/$STATEFULSET -n $NAMESPACE

elif [ "$1" == "status" ]; then
    echo "Проверка статуса Dynamic Allocation..."
    kubectl get configmap $CONFIG_MAP -n $NAMESPACE -o jsonpath='{.data.flink-conf\.yaml}' | grep -q "auto-scale.enabled" && echo "✅ Dynamic Allocation включен" || echo "❌ Dynamic Allocation выключен"

else
    echo "Использование: $0 {enable|disable|status}"
    echo ""
    echo "  enable  - Включить dynamic allocation"
    echo "  disable - Выключить dynamic allocation"
    echo "  status  - Проверить текущий статус"
    exit 1
fi

