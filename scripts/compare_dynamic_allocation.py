#!/usr/bin/env python3
"""
Скрипт для сравнения загруженности CPU и RAM с dynamic allocation и без него.
Собирает реальные метрики из Kubernetes.
"""

import subprocess
import json
import time
import sys
from typing import Dict, List
from datetime import datetime

def get_pod_metrics(namespace: str = "bigdata", label_selector: str = "component=taskmanager") -> List[Dict]:
    """Получить метрики подов через kubectl top"""
    try:
        cmd = [
            "kubectl", "top", "pods",
            "-n", namespace,
            "-l", label_selector,
            "--no-headers"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
            print(f"[ERROR] kubectl top не доступен: {result.stderr}")
            return []
        
        pods = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 3:
                pod_name = parts[0]
                cpu = parts[1]
                memory = parts[2]
                pods.append({
                    "name": pod_name,
                    "cpu": cpu,
                    "memory": memory
                })
        return pods
    except Exception as e:
        print(f"[ERROR] Ошибка получения метрик: {e}")
        return []

def get_replica_count(namespace: str = "bigdata", statefulset: str = "flink-taskmanager") -> int:
    """Получить текущее количество реплик StatefulSet"""
    try:
        cmd = [
            "kubectl", "get", "statefulset",
            statefulset,
            "-n", namespace,
            "-o", "jsonpath={.status.replicas}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip())
        return 0
    except Exception as e:
        print(f"[ERROR] Ошибка получения количества реплик: {e}")
        return 0

def parse_cpu(cpu_str: str) -> float:
    """Парсинг CPU значения в коры"""
    cpu_str = cpu_str.strip()
    if cpu_str.endswith('m'):
        return float(cpu_str[:-1]) / 1000.0
    elif cpu_str.endswith('n'):
        return float(cpu_str[:-1]) / 1000000000.0
    else:
        return float(cpu_str)

def parse_memory(mem_str: str) -> float:
    """Парсинг памяти в гигабайты"""
    mem_str = mem_str.strip()
    if mem_str.endswith('Gi'):
        return float(mem_str[:-2])
    elif mem_str.endswith('Mi'):
        return float(mem_str[:-2]) / 1024.0
    elif mem_str.endswith('Ki'):
        return float(mem_str[:-2]) / (1024 * 1024.0)
    else:
        return float(mem_str) / (1024 * 1024 * 1024.0)

def collect_metrics(duration_seconds: int = 60, interval: int = 10) -> Dict:
    """Собрать метрики за указанный период"""
    print(f"Сбор метрик в течение {duration_seconds} секунд (интервал: {interval}с)...")
    
    measurements = []
    iterations = duration_seconds // interval
    
    for i in range(iterations):
        timestamp = datetime.now().isoformat()
        replicas = get_replica_count()
        pods = get_pod_metrics()
        
        if pods:
            total_cpu = sum(parse_cpu(p['cpu']) for p in pods)
            total_memory = sum(parse_memory(p['memory']) for p in pods)
            avg_cpu_per_pod = total_cpu / len(pods) if pods else 0
            avg_memory_per_pod = total_memory / len(pods) if pods else 0
            
            measurement = {
                "timestamp": timestamp,
                "replicas": replicas,
                "pod_count": len(pods),
                "total_cpu_cores": round(total_cpu, 3),
                "total_memory_gb": round(total_memory, 3),
                "avg_cpu_per_pod": round(avg_cpu_per_pod, 3),
                "avg_memory_per_pod": round(avg_memory_per_pod, 3),
            }
            measurements.append(measurement)
            
            print(f"[{i+1}/{iterations}] Реплик: {replicas}, Подов: {len(pods)}, "
                  f"CPU: {total_cpu:.2f} cores, RAM: {total_memory:.2f} GB")
        
        if i < iterations - 1:
            time.sleep(interval)
    
    # Вычислить средние значения
    if measurements:
        return {
            "measurements": measurements,
            "avg_replicas": round(sum(m['replicas'] for m in measurements) / len(measurements), 2),
            "avg_total_cpu": round(sum(m['total_cpu_cores'] for m in measurements) / len(measurements), 3),
            "avg_total_memory": round(sum(m['total_memory_gb'] for m in measurements) / len(measurements), 3),
            "avg_cpu_per_pod": round(sum(m['avg_cpu_per_pod'] for m in measurements) / len(measurements), 3),
            "avg_memory_per_pod": round(sum(m['avg_memory_per_pod'] for m in measurements) / len(measurements), 3),
            "min_replicas": min(m['replicas'] for m in measurements),
            "max_replicas": max(m['replicas'] for m in measurements),
        }
    
    return {}

def print_comparison(without_da: Dict, with_da: Dict):
    """Вывести сравнение метрик"""
    print("\n" + "=" * 80)
    print("СРАВНЕНИЕ ЗАГРУЖЕННОСТИ CPU И RAM")
    print("=" * 80)
    print()
    
    print("БЕЗ Dynamic Allocation:")
    print("-" * 80)
    if without_da:
        print(f"  Среднее количество реплик: {without_da['avg_replicas']}")
        print(f"  Диапазон реплик: {without_da['min_replicas']} - {without_da['max_replicas']}")
        print(f"  Средний CPU (всего): {without_da['avg_total_cpu']:.3f} cores")
        print(f"  Средний RAM (всего): {without_da['avg_total_memory']:.3f} GB")
        print(f"  Средний CPU (на под): {without_da['avg_cpu_per_pod']:.3f} cores")
        print(f"  Средний RAM (на под): {without_da['avg_memory_per_pod']:.3f} GB")
    else:
        print("  [Нет данных]")
    
    print()
    print("С Dynamic Allocation:")
    print("-" * 80)
    if with_da:
        print(f"  Среднее количество реплик: {with_da['avg_replicas']}")
        print(f"  Диапазон реплик: {with_da['min_replicas']} - {with_da['max_replicas']}")
        print(f"  Средний CPU (всего): {with_da['avg_total_cpu']:.3f} cores")
        print(f"  Средний RAM (всего): {with_da['avg_total_memory']:.3f} GB")
        print(f"  Средний CPU (на под): {with_da['avg_cpu_per_pod']:.3f} cores")
        print(f"  Средний RAM (на под): {with_da['avg_memory_per_pod']:.3f} GB")
    else:
        print("  [Нет данных]")
    
    print()
    print("СРАВНЕНИЕ:")
    print("-" * 80)
    
    if without_da and with_da:
        cpu_diff = with_da['avg_total_cpu'] - without_da['avg_total_cpu']
        memory_diff = with_da['avg_total_memory'] - without_da['avg_total_memory']
        cpu_percent = (cpu_diff / without_da['avg_total_cpu'] * 100) if without_da['avg_total_cpu'] > 0 else 0
        memory_percent = (memory_diff / without_da['avg_total_memory'] * 100) if without_da['avg_total_memory'] > 0 else 0
        
        print(f"  CPU (всего): {cpu_diff:+.3f} cores ({cpu_percent:+.1f}%)")
        print(f"  RAM (всего): {memory_diff:+.3f} GB ({memory_percent:+.1f}%)")
        print(f"  CPU (на под): {with_da['avg_cpu_per_pod'] - without_da['avg_cpu_per_pod']:+.3f} cores")
        print(f"  RAM (на под): {with_da['avg_memory_per_pod'] - without_da['avg_memory_per_pod']:+.3f} GB")
        print(f"  Диапазон реплик: {without_da['min_replicas']}-{without_da['max_replicas']} -> "
              f"{with_da['min_replicas']}-{with_da['max_replicas']}")
    else:
        print("  [Недостаточно данных для сравнения]")

def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Сравнение метрик с dynamic allocation и без")
    parser.add_argument("--duration", type=int, default=60, help="Длительность сбора метрик в секундах")
    parser.add_argument("--interval", type=int, default=10, help="Интервал между измерениями в секундах")
    parser.add_argument("--namespace", default="bigdata", help="Kubernetes namespace")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("СРАВНЕНИЕ ЗАГРУЖЕННОСТИ CPU И RAM")
    print("Dynamic Allocation: ВКЛ vs ВЫКЛ")
    print("=" * 80)
    print()
    
    # Проверка доступности kubectl
    try:
        subprocess.run(["kubectl", "version", "--client"], capture_output=True, check=True, timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        print("[ERROR] kubectl не доступен. Убедитесь, что kubectl установлен и настроен.")
        return 1
    
    # Сбор метрик БЕЗ dynamic allocation
    print("\n[ШАГ 1] Сбор метрик БЕЗ Dynamic Allocation")
    print("Выключение dynamic allocation...")
    print("Выполните вручную:")
    print("  1. Отредактируйте kubernetes/01-flink-config.yaml - удалите строки с kubernetes.taskmanager.*")
    print("  2. kubectl apply -f kubernetes/01-flink-config.yaml")
    print("  3. kubectl rollout restart statefulset/flink-taskmanager -n bigdata")
    input("Нажмите Enter когда dynamic allocation выключен и будете готовы начать сбор метрик...")
    
    without_da = collect_metrics(args.duration, args.interval)
    
    if not without_da:
        print("[ERROR] Не удалось собрать метрики без dynamic allocation")
        return 1
    
    # Сбор метрик С dynamic allocation
    print("\n[ШАГ 2] Сбор метрик С Dynamic Allocation")
    print("Включение dynamic allocation...")
    print("Выполните вручную:")
    print("  1. Убедитесь, что в kubernetes/01-flink-config.yaml есть строки с kubernetes.taskmanager.*")
    print("  2. kubectl apply -f kubernetes/01-flink-config.yaml")
    print("  3. kubectl rollout restart statefulset/flink-taskmanager -n bigdata")
    input("Нажмите Enter когда dynamic allocation включен и будете готовы начать сбор метрик...")
    
    with_da = collect_metrics(args.duration, args.interval)
    
    if not with_da:
        print("[ERROR] Не удалось собрать метрики с dynamic allocation")
        return 1
    
    # Сравнение
    print_comparison(without_da, with_da)
    
    # Сохранение результатов
    output = {
        "timestamp": datetime.now().isoformat(),
        "duration_seconds": args.duration,
        "interval_seconds": args.interval,
        "without_dynamic_allocation": without_da,
        "with_dynamic_allocation": with_da
    }
    
    with open("dynamic_allocation_comparison.json", "w", encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n[OK] Результаты сохранены в dynamic_allocation_comparison.json")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

