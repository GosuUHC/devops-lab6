#!/usr/bin/env python3
"""
Скрипт для сравнения размеров Docker образов до и после оптимизации
"""
import subprocess
import json
import sys
import re
from typing import Dict, Tuple, Optional

def run_command(cmd: list) -> Tuple[str, int]:
    """Выполнение команды и возврат результата"""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.strip() + e.stderr.strip(), e.returncode

def parse_size(size_str: str) -> int:
    """Парсинг размера в байты"""
    # Примеры: "1.2GB", "500MB", "50KB"
    size_str = size_str.strip().upper()
    
    multipliers = {
        'KB': 1024,
        'MB': 1024 * 1024,
        'GB': 1024 * 1024 * 1024,
        'TB': 1024 * 1024 * 1024 * 1024
    }
    
    for unit, multiplier in multipliers.items():
        if unit in size_str:
            number = float(re.sub(rf'[^\d.]', '', size_str))
            return int(number * multiplier)
    
    # Если просто число, считаем байтами
    try:
        return int(re.sub(r'[^\d]', '', size_str))
    except:
        return 0

def format_size(bytes_size: int) -> str:
    """Форматирование размера в читаемый вид"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f}{unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f}PB"

def get_image_size(image_name: str) -> Optional[int]:
    """Получение размера образа в байтах"""
    cmd = ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}\t{{.Size}}"]
    output, returncode = run_command(cmd)
    
    if returncode != 0:
        return None
    
    for line in output.split("\n"):
        if image_name in line:
            parts = line.split("\t")
            if len(parts) == 2:
                return parse_size(parts[1])
    return None

def build_image(dockerfile_path: str, image_name: str, context: str = ".") -> bool:
    """Сборка Docker образа"""
    print(f"  Сборка {image_name}...")
    cmd = [
        "docker", "build",
        "-f", dockerfile_path,
        "-t", image_name,
        context
    ]
    output, returncode = run_command(cmd)
    
    if returncode == 0:
        print(f"  ✓ {image_name} собран успешно")
        return True
    else:
        print(f"  ✗ Ошибка сборки {image_name}: {output[:200]}")
        return False

def compare_images():
    """Основная функция сравнения"""
    print("=" * 80)
    print("Сравнение размеров Docker образов до и после оптимизации")
    print("=" * 80)
    print()
    
    # Конфигурация образов для сравнения
    images_config = [
        {
            "name": "flink",
            "optimized": "dockerfile-optimized/Dockerfile.flink",
            "non_optimized": "dockerfile-without-multistage/Dockerfile.flink",
            "optimized_tag": "flink-optimized:latest",
            "non_optimized_tag": "flink-non-optimized:latest"
        },
        {
            "name": "kafka",
            "optimized": "dockerfile-optimized/Dockerfile.kafka",
            "non_optimized": "dockerfile-without-multistage/Dockerfile.kafka",
            "optimized_tag": "kafka-optimized:latest",
            "non_optimized_tag": "kafka-non-optimized:latest"
        },
        {
            "name": "model-server",
            "optimized": "dockerfile-optimized/Dockerfile.model-server",
            "non_optimized": "dockerfile-without-multistage/Dockerfile.model-server",
            "optimized_tag": "model-server-optimized:latest",
            "non_optimized_tag": "model-server-non-optimized:latest"
        },
        {
            "name": "redis",
            "optimized": "dockerfile-optimized/Dockerfile.redis",
            "non_optimized": "dockerfile-without-multistage/Dockerfile.redis",
            "optimized_tag": "redis-optimized:latest",
            "non_optimized_tag": "redis-non-optimized:latest"
        }
    ]
    
    results = []
    
    print("Шаг 1: Сборка образов БЕЗ оптимизации (многоступенчатой сборки)")
    print("-" * 80)
    
    for config in images_config:
        if not build_image(config["non_optimized"], config["non_optimized_tag"]):
            print(f"⚠️  Пропуск {config['name']} из-за ошибки сборки")
            continue
    
    print()
    print("Шаг 2: Сборка оптимизированных образов (с многоступенчатой сборкой)")
    print("-" * 80)
    
    for config in images_config:
        if not build_image(config["optimized"], config["optimized_tag"]):
            print(f"⚠️  Пропуск {config['name']} из-за ошибки сборки")
            continue
    
    print()
    print("Шаг 3: Сравнение размеров")
    print("-" * 80)
    print(f"{'Образ':<25s} | {'До оптимизации':<20s} | {'После оптимизации':<20s} | {'Экономия':<15s} | {'%':<10s}")
    print("-" * 80)
    
    total_before = 0
    total_after = 0
    
    for config in images_config:
        before_size = get_image_size(config["non_optimized_tag"])
        after_size = get_image_size(config["optimized_tag"])
        
        if before_size is None or after_size is None:
            print(f"{config['name']:<25s} | {'N/A':<20s} | {'N/A':<20s} | {'N/A':<15s} | {'N/A':<10s}")
            continue
        
        savings = before_size - after_size
        savings_percent = (savings / before_size * 100) if before_size > 0 else 0
        
        total_before += before_size
        total_after += after_size
        
        results.append({
            "name": config["name"],
            "before": before_size,
            "after": after_size,
            "savings": savings,
            "savings_percent": savings_percent
        })
        
        print(f"{config['name']:<25s} | {format_size(before_size):<20s} | {format_size(after_size):<20s} | {format_size(savings):<15s} | {savings_percent:.1f}%")
    
    print("-" * 80)
    
    # Итоговая статистика
    if total_before > 0 and total_after > 0:
        total_savings = total_before - total_after
        total_savings_percent = (total_savings / total_before * 100)
        
        print(f"{'ИТОГО':<25s} | {format_size(total_before):<20s} | {format_size(total_after):<20s} | {format_size(total_savings):<15s} | {total_savings_percent:.1f}%")
    
    print()
    print("=" * 80)
    print("Детальная информация:")
    print("=" * 80)
    
    for result in results:
        print(f"\n{result['name'].upper()}:")
        print(f"  До оптимизации:  {format_size(result['before'])}")
        print(f"  После оптимизации: {format_size(result['after'])}")
        print(f"  Экономия:        {format_size(result['savings'])} ({result['savings_percent']:.1f}%)")
    
    print()
    print("=" * 80)
    print("Сравнение завершено!")
    print("=" * 80)
    
    # Сохранение результатов в JSON
    with open("image_size_comparison.json", "w") as f:
        json.dump({
            "results": results,
            "total": {
                "before": total_before,
                "after": total_after,
                "savings": total_before - total_after,
                "savings_percent": ((total_before - total_after) / total_before * 100) if total_before > 0 else 0
            }
        }, f, indent=2)
    
    print("\nРезультаты сохранены в image_size_comparison.json")
    
    return 0

if __name__ == "__main__":
    sys.exit(compare_images())

