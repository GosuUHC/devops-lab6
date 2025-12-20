#!/usr/bin/env python3
"""
Скрипт для проверки конфигураций Части 3.
Проверяет наличие всех необходимых файлов и их корректность.
"""

import os
import sys
from pathlib import Path

REQUIRED_FILES = {
    "yarn-config/capacity-scheduler.xml": "Конфигурация YARN",
    "kubernetes/01-flink-config.yaml": "Dynamic allocation для Flink",
    "kubernetes/02-flink-jobmanager.yaml": "JobManager с on-demand",
    "kubernetes/03-flink-taskmanager-statefulset.yaml": "TaskManagers с spot",
    "kubernetes/06-keda-scaledobject.yaml": "KEDA autoscaling",
    "kubernetes/07-cluster-autoscaler-config.yaml": "Cluster Autoscaler",
    "kubernetes/08-instance-optimization.yaml": "Оптимизация инстансов",
}

REQUIRED_SCRIPTS = {
    "scripts/dynamic_allocation_flink.sh": "Скрипт применения dynamic allocation",
    "tests/test_cost_optimization.py": "Тесты оптимизации",
}

def check_file_exists(filepath, description):
    """Проверить существование файла"""
    path = Path(filepath)
    if path.exists():
        print(f"[OK] {description}: {filepath}")
        return True
    else:
        print(f"[FAIL] {description}: {filepath} - НЕ НАЙДЕН")
        return False

def check_yaml_content(filepath, required_strings):
    """Проверить наличие ключевых строк в YAML"""
    path = Path(filepath)
    if not path.exists():
        return False
    
    content = path.read_text(encoding='utf-8')
    all_found = True
    for req_str in required_strings:
        if req_str in content:
            print(f"  [OK] Найдено: {req_str}")
        else:
            print(f"  [FAIL] Отсутствует: {req_str}")
            all_found = False
    return all_found

def main():
    """Основная функция проверки"""
    print("=" * 80)
    print("ПРОВЕРКА КОНФИГУРАЦИЙ ЧАСТИ 3")
    print("=" * 80)
    print()
    
    all_ok = True
    
    # Проверка файлов
    print("Проверка наличия файлов:")
    print("-" * 80)
    for filepath, description in REQUIRED_FILES.items():
        if not check_file_exists(filepath, description):
            all_ok = False
    print()
    
    # Проверка скриптов
    print("Проверка скриптов:")
    print("-" * 80)
    for filepath, description in REQUIRED_SCRIPTS.items():
        if not check_file_exists(filepath, description):
            all_ok = False
    print()
    
    # Проверка содержимого ключевых файлов
    print("Проверка содержимого конфигураций:")
    print("-" * 80)
    
    # Проверка YARN конфигурации
    print("\n1. YARN (capacity-scheduler.xml):")
    check_yaml_content("yarn-config/capacity-scheduler.xml", [
        "yarn.scheduler.capacity.root.queues",
        "default,etl,ml",
        "etl.capacity",
        "ml.capacity"
    ])
    
    # Проверка KEDA
    print("\n2. KEDA ScaledObject:")
    check_yaml_content("kubernetes/06-keda-scaledobject.yaml", [
        "ScaledObject",
        "flink-taskmanager",
        "kafka",
        "minReplicaCount",
        "maxReplicaCount"
    ])
    
    # Проверка Cluster Autoscaler
    print("\n3. Cluster Autoscaler:")
    check_yaml_content("kubernetes/07-cluster-autoscaler-config.yaml", [
        "cluster-autoscaler-config",
        "scaleDownUnneededTime",
        "scaleDownUtilizationThreshold"
    ])
    
    # Проверка оптимизации инстансов
    print("\n4. Оптимизация инстансов:")
    check_yaml_content("kubernetes/08-instance-optimization.yaml", [
        "cpu-bound-config",
        "memory-bound-config",
        "balanced-config",
        "node.kubernetes.io/instance-type"
    ])
    
    # Проверка spot-инстансов
    print("\n5. Spot-инстансы:")
    taskmanager_ok = check_yaml_content("kubernetes/03-flink-taskmanager-statefulset.yaml", [
        "node-type: spot",
        'key: "spot"',
        "tolerations"
    ])
    jobmanager_ok = check_yaml_content("kubernetes/02-flink-jobmanager.yaml", [
        "node-type: on-demand",
        'key: "on-demand"'
    ])
    
    if not (taskmanager_ok and jobmanager_ok):
        all_ok = False
    
    # Итоги
    print("\n" + "=" * 80)
    print("ИТОГИ ПРОВЕРКИ")
    print("=" * 80)
    
    if all_ok:
        print("[OK] ВСЕ КОНФИГУРАЦИИ НА МЕСТЕ")
        print("\nДля применения конфигураций выполните:")
        print("  kubectl apply -f kubernetes/")
        print("  kubectl apply -f yarn-config/")
        return 0
    else:
        print("[FAIL] ОБНАРУЖЕНЫ ПРОБЛЕМЫ")
        print("   Проверьте отсутствующие файлы или конфигурации")
        return 1

if __name__ == "__main__":
    sys.exit(main())

