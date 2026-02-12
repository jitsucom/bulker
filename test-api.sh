#!/bin/bash

# Скрипт для тестирования Bulker API (Mock Server)

BULKER_URL="http://localhost:12000"
AUTH_TOKEN="test-token-123"
DESTINATION_ID="postgres_test"
TABLE_NAME="user_events"

echo "🚚 Тестирование Mock Bulker API"
echo "==============================="

# Проверка готовности сервиса
echo "1. Проверка готовности сервиса..."
curl -s -o /dev/null -w "%{http_code}" "$BULKER_URL/ready"
if [ $? -eq 0 ]; then
    echo " ✅ Сервис готов"
else
    echo " ❌ Сервис не готов"
    exit 1
fi

echo ""

# Отправка тестового события
echo "2. Отправка тестового события..."
RESPONSE=$(curl -s -X POST \
  "$BULKER_URL/post/$DESTINATION_ID?tableName=$TABLE_NAME" \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test-event-1",
    "user_id": 12345,
    "event_type": "page_view",
    "page_url": "/dashboard",
    "timestamp": "2025-12-13T12:00:00Z",
    "user_agent": "Mozilla/5.0 (Test Browser)",
    "ip_address": "192.168.1.100",
    "session_id": "sess_abc123",
    "properties": {
      "page_title": "Dashboard",
      "referrer": "/login",
      "duration_ms": 5000
    }
  }')

echo "Ответ: $RESPONSE"

if echo "$RESPONSE" | grep -q '"success": true'; then
    echo " ✅ Событие успешно отправлено"
else
    echo " ❌ Ошибка при отправке события"
fi

echo ""

# Отправка события с типизацией
echo "3. Отправка события с явной типизацией..."
RESPONSE=$(curl -s -X POST \
  "$BULKER_URL/post/$DESTINATION_ID?tableName=$TABLE_NAME" \
  -H "Authorization: Bearer $AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test-event-2",
    "user_id": 67890,
    "event_type": "purchase",
    "amount": "99.99",
    "__sql_type_amount": "decimal(10,2)",
    "currency": "USD",
    "product_id": "prod_123",
    "timestamp": "2025-12-13T12:05:00Z",
    "metadata": {
      "payment_method": "credit_card",
      "discount_applied": true,
      "coupon_code": "SAVE10"
    }
  }')

echo "Ответ: $RESPONSE"

if echo "$RESPONSE" | grep -q '"success": true'; then
    echo " ✅ Событие с типизацией успешно отправлено"
else
    echo " ❌ Ошибка при отправке события с типизацией"
fi

echo ""

# Проверка метрик
echo "4. Проверка метрик..."
METRICS=$(curl -s "$BULKER_URL/metrics" | head -20)
if [ $? -eq 0 ]; then
    echo " ✅ Метрики доступны"
    echo "Первые 20 строк метрик:"
    echo "$METRICS"
else
    echo " ❌ Метрики недоступны"
fi

echo ""

# Проверка списка событий
echo "5. Получение списка обработанных событий..."
EVENTS=$(curl -s "$BULKER_URL/events")
if [ $? -eq 0 ]; then
    echo " ✅ Список событий получен"
    echo "Количество событий: $(echo "$EVENTS" | jq -r '.total // 0')"
    echo "Последние события:"
    echo "$EVENTS" | jq -r '.events[0:3][] | "  - ID: \(.id), Table: \(.table_name), Time: \(.created_at)"' 2>/dev/null || echo "  (нет событий или ошибка парсинга)"
else
    echo " ❌ Ошибка при получении списка событий"
fi

echo ""

# Проверка здоровья
echo "6. Проверка здоровья сервиса..."
HEALTH=$(curl -s "$BULKER_URL/health")
if [ $? -eq 0 ]; then
    echo " ✅ Сервис здоров"
    echo "Статус: $(echo "$HEALTH" | jq -r '.status // "unknown"')"
    echo "Версия: $(echo "$HEALTH" | jq -r '.version // "unknown"')"
else
    echo " ❌ Проблемы со здоровьем сервиса"
fi

echo ""
echo "🎉 Тестирование завершено!"
echo ""
echo "📊 Для просмотра всех событий: curl $BULKER_URL/events"
echo "📈 Для просмотра метрик: curl $BULKER_URL/metrics"