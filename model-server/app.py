from fastapi import FastAPI
import redis
import json
import time
import logging
from prometheus_client import Counter, Histogram, Gauge, generate_latest, REGISTRY, CONTENT_TYPE_LATEST
from fastapi.responses import Response

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Prometheus метрики
PREDICTION_COUNTER = Counter('model_predictions_total', 'Total number of predictions', ['status'])
PREDICTION_LATENCY = Histogram(
    'model_prediction_latency_seconds',
    'Prediction latency in seconds',
    buckets=[0.001, 0.002, 0.005, 0.01, 0.02, 0.03, 0.05, 0.1, 0.2, 0.5, 1.0]
)
PREDICTION_RISK_SCORE = Gauge('model_prediction_risk_score', 'Latest prediction risk score')

# Подключение к Redis
try:
    redis_client = redis.Redis(host='redis', port=6379, decode_responses=True, socket_connect_timeout=5)
    redis_client.ping()
    logger.info("Successfully connected to Redis")
except redis.ConnectionError as e:
    logger.error(f"Redis connection error: {e}")
    redis_client = None

@app.get("/health")
async def health():
    redis_status = "connected" if redis_client and redis_client.ping() else "disconnected"
    return {
        "status": "healthy",
        "redis": redis_status,
        "timestamp": time.time()
    }

@app.post("/predict")
async def predict(features: dict):
    start_time = time.time()
    
    try:
        # Простой расчет risk_score на основе входных данных
        age = float(features.get('age', 50))
        bmi = float(features.get('bmi', 25))
        
        # Простая модель для демонстрации
        risk_score = min(1.0, (age / 100.0) * 0.5 + (bmi / 50.0) * 0.5)
        
        # Сохранение в Redis
        if redis_client:
            feature_record = {
                **features,
                'risk_score': risk_score,
                'timestamp': time.time()
            }
            patient_id = features.get('patient_id', 'unknown')
            redis_client.lpush(f"patient:{patient_id}:predictions", json.dumps(feature_record))
            redis_client.ltrim(f"patient:{patient_id}:predictions", 0, 99)
        
        # Обновление метрик
        PREDICTION_COUNTER.labels(status='success').inc()
        latency = time.time() - start_time
        PREDICTION_LATENCY.observe(latency)
        PREDICTION_RISK_SCORE.set(risk_score)
        
        logger.info(f"Prediction: risk={risk_score:.3f}, time={latency*1000:.2f}ms")
        
        return {
            'risk_score': round(risk_score, 4),
            'processing_time_ms': round(latency * 1000, 2)
        }
        
    except Exception as e:
        PREDICTION_COUNTER.labels(status='error').inc()
        logger.error(f"Prediction error: {e}")
        return {
            "error": str(e),
            "risk_score": 0.0
        }

@app.get("/metrics")
async def metrics():
    """Эндпоинт для Prometheus метрик"""
    return Response(
        generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST
    )

@app.get("/")
async def root():
    return {"message": "Patient Readmission Model Server"}

