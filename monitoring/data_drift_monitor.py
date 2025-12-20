"""
Мониторинг data drift с использованием Evidently
Отслеживает изменения в распределении данных и отправляет метрики в Prometheus
"""
import json
import time
import logging
import pandas as pd
from typing import Dict, Optional
from prometheus_client import Gauge, push_to_gateway, CollectorRegistry
import redis
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from evidently.report import Report
    from evidently.metrics import DataDriftTable
    EVIDENTLY_AVAILABLE = True
except ImportError:
    EVIDENTLY_AVAILABLE = False
    logger.warning("Evidently not available. Data drift monitoring disabled.")


class DataDriftMonitor:
    """Монитор для отслеживания data drift"""
    
    def __init__(self, redis_client: Optional[redis.Redis] = None,
                 prometheus_gateway: str = "pushgateway:9091"):
        self.redis_client = redis_client
        self.prometheus_gateway = prometheus_gateway
        self.reference_data = None
        self.registry = CollectorRegistry()
        
        if not EVIDENTLY_AVAILABLE:
            logger.warning("Evidently not available. Using simple drift detection.")
    
    def set_reference_data(self, data: pd.DataFrame):
        """Установка референсных данных"""
        self.reference_data = data
        logger.info(f"Reference data set: {data.shape}")
    
    def monitor_data_drift(self, current_data: pd.DataFrame) -> Dict:
        """
        Мониторинг data drift между референсными и текущими данными
        
        Args:
            current_data: Текущие данные для сравнения
        
        Returns:
            Словарь с результатами drift detection
        """
        if self.reference_data is None:
            logger.warning("Reference data not set. Skipping drift detection.")
            return {'drift_score': 0.0, 'drift_detected': False}
        
        if not EVIDENTLY_AVAILABLE:
            # Простая проверка на основе статистик
            return self._simple_drift_detection(current_data)
        
        try:
            # Использование Evidently для детекции drift
            drift_report = Report(metrics=[DataDriftTable()])
            drift_report.run(
                reference_data=self.reference_data,
                current_data=current_data
            )
            
            # Извлечение drift score
            report_dict = drift_report.as_dict()
            drift_score = 0.0
            drift_detected = False
            
            if 'metrics' in report_dict and len(report_dict['metrics']) > 0:
                metric_result = report_dict['metrics'][0].get('result', {})
                drift_score = metric_result.get('drift_score', 0.0)
                drift_detected = drift_score > 0.2  # Порог для детекции drift
            
            # Push метрик в Prometheus gateway
            try:
                # Создаем временный registry для push
                push_registry = CollectorRegistry()
                drift_score_gauge = Gauge('data_drift_score', 'Data drift score (0-1)', registry=push_registry)
                drift_detected_gauge = Gauge('data_drift_detected', 'Data drift detected (1=yes, 0=no)', registry=push_registry)
                
                drift_score_gauge.set(drift_score)
                drift_detected_gauge.set(1 if drift_detected else 0)
                
                push_to_gateway(
                    self.prometheus_gateway,
                    job="data_drift",
                    registry=push_registry
                )
                logger.debug(f"Pushed drift metrics: score={drift_score:.3f}, detected={drift_detected}")
            except Exception as e:
                logger.warning(f"Failed to push metrics to Prometheus: {e}")
            
            # Алерт при превышении порога
            if drift_detected:
                logger.warning(f"⚠️ Data drift detected! Score: {drift_score:.3f}")
                self._send_alert(f"Обнаружен data drift (score={drift_score:.2f})")
            
            return {
                'drift_score': drift_score,
                'drift_detected': drift_detected,
                'timestamp': time.time()
            }
            
        except Exception as e:
            logger.error(f"Error in drift detection: {e}")
            return {'drift_score': 0.0, 'drift_detected': False, 'error': str(e)}
    
    def _simple_drift_detection(self, current_data: pd.DataFrame) -> Dict:
        """Простая детекция drift на основе статистик"""
        try:
            # Сравнение средних значений числовых колонок
            numeric_cols = self.reference_data.select_dtypes(include=['number']).columns
            
            drift_scores = []
            for col in numeric_cols:
                if col in current_data.columns:
                    ref_mean = self.reference_data[col].mean()
                    curr_mean = current_data[col].mean()
                    
                    # Простой drift score на основе относительного изменения
                    if ref_mean != 0:
                        relative_change = abs(curr_mean - ref_mean) / abs(ref_mean)
                        drift_scores.append(min(relative_change, 1.0))
            
            avg_drift_score = sum(drift_scores) / len(drift_scores) if drift_scores else 0.0
            drift_detected = avg_drift_score > 0.2
            
            # Push метрик в Prometheus gateway
            try:
                push_registry = CollectorRegistry()
                drift_score_gauge = Gauge('data_drift_score', 'Data drift score (0-1)', registry=push_registry)
                drift_detected_gauge = Gauge('data_drift_detected', 'Data drift detected (1=yes, 0=no)', registry=push_registry)
                
                drift_score_gauge.set(avg_drift_score)
                drift_detected_gauge.set(1 if drift_detected else 0)
                
                push_to_gateway(
                    self.prometheus_gateway,
                    job="data_drift",
                    registry=push_registry
                )
            except Exception as e:
                logger.warning(f"Failed to push metrics to Prometheus: {e}")
            
            return {
                'drift_score': avg_drift_score,
                'drift_detected': drift_detected,
                'timestamp': time.time()
            }
        except Exception as e:
            logger.error(f"Error in simple drift detection: {e}")
            return {'drift_score': 0.0, 'drift_detected': False}
    
    def monitor_late_data(self) -> float:
        """
        Мониторинг доли late-arriving данных
        
        Returns:
            Доля late данных (0-1)
        """
        late_ratio = 0.0
        
        if self.redis_client:
            try:
                # Попытка получить метрики из Redis
                late_count = self.redis_client.get('late_data_count') or 0
                total_count = self.redis_client.get('total_data_count') or 1
                late_ratio = float(late_count) / float(total_count) if total_count > 0 else 0.0
            except Exception as e:
                logger.warning(f"Failed to get late data metrics from Redis: {e}")
        
        # Push метрики late data в Prometheus
        try:
            push_registry = CollectorRegistry()
            late_ratio_gauge = Gauge('late_data_ratio', 'Ratio of late-arriving data', registry=push_registry)
            late_ratio_gauge.set(late_ratio)
            
            push_to_gateway(
                self.prometheus_gateway,
                job="data_drift",
                registry=push_registry
            )
        except Exception as e:
            logger.warning(f"Failed to push late data metrics to Prometheus: {e}")
        
        return late_ratio
    
    def monitor_schema_compliance(self, current_data: pd.DataFrame) -> int:
        """
        Мониторинг соответствия схеме данных
        
        Args:
            current_data: Текущие данные для проверки
        
        Returns:
            Количество ошибок схемы
        """
        failures = 0
        
        if self.reference_data is not None:
            # Проверка наличия всех колонок
            ref_cols = set(self.reference_data.columns)
            curr_cols = set(current_data.columns)
            
            missing_cols = ref_cols - curr_cols
            if missing_cols:
                failures += len(missing_cols)
                logger.warning(f"Missing columns: {missing_cols}")
            
            # Проверка типов данных
            for col in ref_cols & curr_cols:
                ref_dtype = self.reference_data[col].dtype
                curr_dtype = current_data[col].dtype
                if ref_dtype != curr_dtype:
                    failures += 1
                    logger.warning(f"Type mismatch for {col}: {ref_dtype} vs {curr_dtype}")
        
        # Push метрики в Prometheus
        try:
            push_registry = CollectorRegistry()
            schema_failures_gauge = Gauge('schema_compliance_failures_total', 'Total schema compliance failures', registry=push_registry)
            schema_failures_gauge.set(failures)
            
            push_to_gateway(
                self.prometheus_gateway,
                job="data_drift",
                registry=push_registry
            )
        except Exception as e:
            logger.warning(f"Failed to push schema compliance metrics to Prometheus: {e}")
        
        return failures
    
    def _send_alert(self, message: str):
        """Отправка алерта (можно расширить для интеграции с alerting системой)"""
        logger.warning(f"ALERT: {message}")
        # Здесь можно добавить интеграцию с Slack, PagerDuty и т.д.


def load_reference_data(file_path: str) -> pd.DataFrame:
    """Загрузка референсных данных"""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Reference data loaded from {file_path}: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Failed to load reference data: {e}")
        raise


if __name__ == '__main__':
    # Пример использования
    import sys
    
    reference_path = sys.argv[1] if len(sys.argv) > 1 else '../data/hospital_readmissions_30k.csv'
    
    try:
        # Загрузка референсных данных
        reference_data = load_reference_data(reference_path)
        
        # Инициализация монитора
        redis_host = os.getenv('REDIS_HOST', 'redis')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        try:
            redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, socket_connect_timeout=5)
            redis_client.ping()
        except:
            redis_client = None
            logger.warning("Redis not available, continuing without Redis")
        
        monitor = DataDriftMonitor(redis_client=redis_client)
        monitor.set_reference_data(reference_data)
        
        # Пример текущих данных (в реальности читать из Kafka/Flink)
        current_data = reference_data.sample(1000)  # Для демонстрации
        
        # Мониторинг drift
        drift_result = monitor.monitor_data_drift(current_data)
        
        # Мониторинг late data
        late_ratio = monitor.monitor_late_data()
        
        # Мониторинг schema compliance
        schema_failures = monitor.monitor_schema_compliance(current_data)
        
        print(f"\nData Drift Results:")
        print(f"  Drift Score: {drift_result['drift_score']:.3f}")
        print(f"  Drift Detected: {drift_result['drift_detected']}")
        print(f"  Late Data Ratio: {late_ratio:.3f}")
        print(f"  Schema Compliance Failures: {schema_failures}")
        
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

