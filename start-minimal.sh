#!/bin/bash

# Скрипт для быстрого запуска минимальной версии Bulker

set -e

COMPOSE_FILE="docker-compose.minimal.yml"
PROJECT_NAME="bulker-minimal"

echo "🚚 Быстрый запуск Bulker (минимальная версия)"
echo "============================================="

# Проверка Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен. Установите Docker и повторите попытку."
    exit 1
fi

echo "✅ Docker найден"

# Остановка существующих контейнеров
echo "🛑 Остановка существующих контейнеров..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down --remove-orphans 2>/dev/null || true

echo ""

# Запуск минимальной конфигурации
echo "🚀 Запуск минимальной конфигурации..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d

echo ""
echo "⏳ Ожидание готовности сервисов (30 секунд)..."
sleep 30

# Проверка готовности
echo "🔍 Проверка готовности сервисов..."

services=(
    "http://localhost:3042/ready:Mock Bulker"
    "localhost:5432:PostgreSQL"
)

for service_info in "${services[@]}"; do
    IFS=':' read -r endpoint name <<< "$service_info"
    echo -n "  $name: "
    
    if [[ "$name" == "PostgreSQL" ]]; then
        if docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec -T postgres pg_isready -U bulker &>/dev/null; then
            echo "✅ Готов"
        else
            echo "❌ Не готов"
        fi
    else
        if curl -s "$endpoint" &>/dev/null; then
            echo "✅ Готов"
        else
            echo "❌ Не готов"
        fi
    fi
done

echo ""
echo "🎉 Минимальное развертывание завершено!"
echo ""
echo "📍 Доступные сервисы:"
echo "  • Mock Bulker API: http://localhost:3042"
echo "  • PostgreSQL:      localhost:5432 (bulker/password)"
echo ""
echo "🔑 Токены авторизации:"
echo "  • local-token-123"
echo "  • test-token-456"
echo ""
echo "🧪 Для тестирования запустите:"
echo "  ./test-api.sh"
echo ""
echo "🛑 Для остановки:"
echo "  docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down"