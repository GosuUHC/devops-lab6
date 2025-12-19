#!/bin/bash
set -e

# Ожидание Zookeeper (если используется)
if [ -n "$KAFKA_ZOOKEEPER_CONNECT" ]; then
    echo "Waiting for Zookeeper..."
    zk_host=$(echo $KAFKA_ZOOKEEPER_CONNECT | cut -d: -f1)
    zk_port=$(echo $KAFKA_ZOOKEEPER_CONNECT | cut -d: -f2)
    until nc -z $zk_host $zk_port 2>/dev/null; do
        sleep 1
    done
    echo "Zookeeper is ready!"
fi

# Создание конфигурации server.properties из переменных окружения
CONFIG_FILE="$KAFKA_HOME/config/server.properties"

# Базовые настройки
cat > "$CONFIG_FILE" <<EOF
# Generated from environment variables
broker.id=${KAFKA_BROKER_ID:-1}
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-1}
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=${KAFKA_LOG_RETENTION_HOURS:-168}
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=${KAFKA_ZOOKEEPER_CONNECT}
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
EOF

# Настройка listeners из переменных окружения
if [ -n "$KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" ]; then
    echo "listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}" >> "$CONFIG_FILE"
fi

if [ -n "$KAFKA_ADVERTISED_LISTENERS" ]; then
    echo "advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}" >> "$CONFIG_FILE"
    # Генерируем listeners на основе advertised.listeners, заменяя хосты на 0.0.0.0
    listeners=$(echo "$KAFKA_ADVERTISED_LISTENERS" | sed 's/:\/\/[^:]*:/:\/\/0.0.0.0:/g')
    echo "listeners=${listeners}" >> "$CONFIG_FILE"
fi

if [ -n "$KAFKA_INTER_BROKER_LISTENER_NAME" ]; then
    echo "inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME}" >> "$CONFIG_FILE"
fi

# Auto create topics
if [ -n "$KAFKA_AUTO_CREATE_TOPICS_ENABLE" ]; then
    echo "auto.create.topics.enable=${KAFKA_AUTO_CREATE_TOPICS_ENABLE}" >> "$CONFIG_FILE"
fi

echo "Kafka configuration:"
cat "$CONFIG_FILE"

# Запуск Kafka
exec $KAFKA_HOME/bin/kafka-server-start.sh "$CONFIG_FILE"

