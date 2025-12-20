"""
Генератор нагрузки для тестирования Big Data системы
Генерирует поток транзакций и отправляет их в Kafka
"""
import json
import time
import random
import logging
import os
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import push_to_gateway, CollectorRegistry, Gauge, Histogram, Counter
from collections import deque

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Prometheus метрики
registry = CollectorRegistry()
LATENCY_P50 = Gauge('load_generator_latency_p50_ms', 'P50 latency in milliseconds', registry=registry)
LATENCY_P95 = Gauge('load_generator_latency_p95_ms', 'P95 latency in milliseconds', registry=registry)
LATENCY_P99 = Gauge('load_generator_latency_p99_ms', 'P99 latency in milliseconds', registry=registry)
LATENCY_AVG = Gauge('load_generator_latency_avg_ms', 'Average latency in milliseconds', registry=registry)
REQUEST_COUNTER = Counter('load_generator_requests_total', 'Total number of requests', ['status'], registry=registry)
REQUEST_LATENCY = Histogram('load_generator_request_latency_seconds', 'Request latency in seconds', registry=registry)
TRANSACTIONS_SENT = Counter('load_generator_transactions_sent_total', 'Total transactions sent to Kafka', registry=registry)


class LoadGenerator:
    """Генератор нагрузки для тестирования системы"""
    
    def __init__(self):
        self._wait_for_kafka()
        self.producer = KafkaProducer(
            bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:19092')],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5,
            request_timeout_ms=30000,
            acks='all'  # Гарантия доставки
        )
        logger.info("Kafka producer initialized")
        
        # Настройка для HTTP запросов к model-server (опционально)
        self.model_server_url = os.getenv('MODEL_SERVER_URL', 'http://model-server:8000/predict')
        self.prometheus_gateway = os.getenv('PROMETHEUS_GATEWAY', 'pushgateway:9091')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'transactions')
        
        # Буфер для метрик latency (последние 1000 запросов)
        self.latency_buffer = deque(maxlen=1000)
        
        # Счетчики для метрик
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.transactions_sent = 0
        
    def _wait_for_kafka(self):
        """Ожидание готовности Kafka"""
        max_retries = 30
        retry_count = 0
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:19092').split(',')
        
        while retry_count < max_retries:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=kafka_servers,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    request_timeout_ms=5000
                )
                producer.close()
                logger.info("Kafka is ready!")
                return
            except Exception as e:
                retry_count += 1
                logger.info(f"Waiting for Kafka... ({retry_count}/{max_retries})")
                time.sleep(2)
        
        raise Exception("Kafka not ready after waiting")
    
    def generate_transaction(self, user_id=None):
        """Генерация транзакции"""
        transaction = {
            "user_id": user_id if user_id is not None else random.randint(1, 10000),
            "amount": round(random.uniform(1, 1000), 2),
            "timestamp": datetime.now().isoformat()
        }
        return transaction
    
    def send_to_kafka(self, transaction):
        """Отправка транзакции в Kafka"""
        try:
            future = self.producer.send(self.kafka_topic, transaction)
            # Не ждем подтверждения для производительности
            self.transactions_sent += 1
            TRANSACTIONS_SENT.inc()
            return True
        except KafkaError as e:
            logger.error(f"Failed to send transaction to Kafka: {e}")
            self.failed_requests += 1
            REQUEST_COUNTER.labels(status='error').inc()
            return False
    
    def _push_metrics_to_prometheus(self):
        """Расчет и отправка метрик в Prometheus через pushgateway"""
        if len(self.latency_buffer) == 0:
            return
        
        try:
            # Расчет перцентилей
            latencies = np.array(list(self.latency_buffer))
            p50 = np.percentile(latencies, 50)
            p95 = np.percentile(latencies, 95)
            p99 = np.percentile(latencies, 99)
            avg_latency = np.mean(latencies)
            
            # Установка значений метрик
            LATENCY_P50.set(p50)
            LATENCY_P95.set(p95)
            LATENCY_P99.set(p99)
            LATENCY_AVG.set(avg_latency)
            
            # Отправка в pushgateway
            push_to_gateway(
                self.prometheus_gateway,
                job='load_generator',
                registry=registry
            )
            
            logger.info(f"Metrics pushed: p50={p50:.2f}ms, p95={p95:.2f}ms, p99={p99:.2f}ms, avg={avg_latency:.2f}ms")
            
        except Exception as e:
            logger.warning(f"Failed to push metrics to Prometheus: {e}")
    
    def generate_transactions(self, events_per_second=1000, duration_seconds=None):
        """
        Генерация потока транзакций
        
        Args:
            events_per_second: Количество транзакций в секунду
            duration_seconds: Длительность генерации в секундах (None = бесконечно)
        """
        logger.info(f"Starting transaction generation: {events_per_second} events/sec")
        
        start_time = time.time()
        metrics_push_counter = 0
        user_counter = 1
        
        try:
            while True:
                batch_start = time.time()
                
                # Генерация батча транзакций
                for i in range(events_per_second):
                    transaction = self.generate_transaction(user_counter)
                    self.send_to_kafka(transaction)
                    user_counter += 1
                    
                    # Периодически проверяем задержку (опционально)
                    if i % 100 == 0:
                        latency = (time.time() - batch_start) * 1000
                        self.latency_buffer.append(latency)
                        REQUEST_LATENCY.observe(latency / 1000.0)
                
                # Периодическая отправка метрик в Prometheus (каждые 5 секунд)
                metrics_push_counter += 1
                if metrics_push_counter >= 5:
                    self._push_metrics_to_prometheus()
                    metrics_push_counter = 0
                
                # Поддержание стабильной скорости
                batch_time = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_time)
                time.sleep(sleep_time)
                
                # Проверка длительности
                if duration_seconds is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= duration_seconds:
                        logger.info(f"Generation completed after {elapsed:.2f} seconds")
                        break
                        
        except KeyboardInterrupt:
            logger.info("Generation interrupted by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Total transactions sent: {self.transactions_sent}")
            logger.info(f"Successful: {self.successful_requests}, Failed: {self.failed_requests}")


def generate_transactions():
    """Генерация потока транзакций (основная функция)"""
    events_per_second = int(os.getenv('EVENTS_PER_SECOND', 1000))
    
    generator = LoadGenerator()
    # Даем время другим сервисам запуститься
    time.sleep(10)
    generator.generate_transactions(events_per_second=events_per_second)


if __name__ == '__main__':
    try:
        generate_transactions()
    except Exception as e:
        logger.error(f"Generator failed: {e}")
        import traceback
        traceback.print_exc()
        raise

