#!/bin/bash

# Скрипт для запуска Bulker в локальной среде

set -e

COMPOSE_FILE="docker-compose.local.yml"
PROJECT_NAME="bulker-local"

echo "🚚 Запуск Bulker в локальной среде"
echo "=================================="

# Проверка Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен. Установите Docker и повторите попытку."
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose не установлен. Установите Docker Compose и повторите попытку."
    exit 1
fi

# Проверка файлов
if [ ! -f "$COMPOSE_FILE" ]; then
    echo "❌ Файл $COMPOSE_FILE не найден"
    exit 1
fi

if [ ! -f "all.Dockerfile" ]; then
    echo "❌ Файл all.Dockerfile не найден"
    exit 1
fi

echo "✅ Предварительные проверки пройдены"
echo ""

# Остановка существующих контейнеров
echo "🛑 Остановка существующих контейнеров..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down --remove-orphans 2>/dev/null || true

echo ""

# Сборка образов
echo "🔨 Сборка образов..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" build --no-cache

echo ""

# Запуск инфраструктурных сервисов
echo "🚀 Запуск инфраструктурных сервисов..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d zookeeper kafka postgres redis

echo "⏳ Ожидание готовности инфраструктуры (60 секунд)..."
sleep 60

# Проверка готовности сервисов
echo "🔍 Проверка готовности сервисов..."

# Kafka
echo -n "  Kafka: "
if docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
    echo "✅ Готов"
else
    echo "❌ Не готов"
fi

# PostgreSQL
echo -n "  PostgreSQL: "
if docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec -T postgres pg_isready -U bulker &>/dev/null; then
    echo "✅ Готов"
else
    echo "❌ Не готов"
fi

# Redis
echo -n "  Redis: "
if docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec -T redis redis-cli ping &>/dev/null; then
    echo "✅ Готов"
else
    echo "❌ Не готов"
fi

echo ""

# Запуск основного приложения
echo "🚀 Запуск основного приложения Bulker..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d bulker

echo "⏳ Ожидание готовности Bulker (120 секунд)..."
sleep 120

# Проверка готовности Bulker
echo "🔍 Проверка готовности Bulker..."
for i in {1..30}; do
    if curl -s http://localhost:3042/ready &>/dev/null; then
        echo "✅ Bulker готов!"
        break
    fi
    echo "  Попытка $i/30..."
    sleep 5
done

echo ""

# Запуск дополнительных сервисов
echo "🚀 Запуск дополнительных сервисов..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d

echo ""

# Финальная проверка
echo "🎯 Финальная проверка сервисов..."
echo ""

# Статус контейнеров
echo "📊 Статус контейнеров:"
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" ps

echo ""

# Проверка endpoints
echo "🔗 Проверка endpoints:"

endpoints=(
    "http://localhost:3042/ready:Bulker API"
    "http://localhost:3042/metrics:Bulker Metrics"
    "http://localhost:3043/health:Ingest Service"
)

for endpoint_info in "${endpoints[@]}"; do
    IFS=':' read -r url name <<< "$endpoint_info"
    echo -n "  $name ($url): "
    if curl -s "$url" &>/dev/null; then
        echo "✅ Доступен"
    else
        echo "❌ Недоступен"
    fi
done

echo ""
echo "🎉 Развертывание завершено!"
echo ""
echo "📍 Доступные сервисы:"
echo "  • Bulker API:      http://localhost:3042"
echo "  • Bulker Metrics:  http://localhost:9090"
echo "  • Ingest Service:  http://localhost:3043"
echo "  • PostgreSQL:      localhost:5432 (bulker/bulker_password)"
echo "  • Kafka:           localhost:9092"
echo "  • Redis:           localhost:6379"
echo ""
echo "🔑 Токены авторизации:"
echo "  • local-token-123"
echo "  • admin-token-456"
echo "  • test-token-789"
echo ""
echo "🧪 Для тестирования запустите:"
echo "  ./test-api.sh"
echo ""
echo "📋 Для просмотра логов:"
echo "  docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f bulker"
echo ""
echo "🛑 Для остановки:"
echo "  ./stop-local.sh"