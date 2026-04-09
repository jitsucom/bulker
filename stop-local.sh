#!/bin/bash

# Скрипт для остановки Bulker в локальной среде

set -e

COMPOSE_FILE="docker-compose.local.yml"
PROJECT_NAME="bulker-local"

echo "🛑 Остановка Bulker в локальной среде"
echo "====================================="

# Остановка и удаление контейнеров
echo "🔄 Остановка контейнеров..."
docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down

echo ""

# Опциональное удаление volumes
read -p "🗑️  Удалить данные (volumes)? [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Удаление volumes..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down -v
    echo "✅ Volumes удалены"
else
    echo "💾 Volumes сохранены"
fi

echo ""

# Опциональное удаление образов
read -p "🗑️  Удалить образы? [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Удаление образов..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down --rmi all
    echo "✅ Образы удалены"
else
    echo "💾 Образы сохранены"
fi

echo ""
echo "✅ Остановка завершена!"