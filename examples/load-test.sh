#!/bin/bash

# Скрипт нагрузочного тестирования Bulker API

BULKER_URL="http://localhost:3042"
AUTH_TOKEN="local-token-123"
DESTINATION_ID="load_test"
TABLE_NAME="performance_events"

echo "🚀 Нагрузочное тестирование Bulker API"
echo "======================================"

# Проверка готовности
echo "1. Проверка готовности сервиса..."
if ! curl -s "$BULKER_URL/ready" > /dev/null; then
    echo "❌ Сервис недоступен. Убедитесь, что Bulker запущен."
    exit 1
fi
echo "✅ Сервис готов"

# Создание тестового события
cat > /tmp/load_event.json << EOF
{
  "user_id": \$((RANDOM % 10000)),
  "action": "load_test",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "session_id": "load_\$RANDOM",
  "properties": {
    "test_run": "$(date +%s)",
    "batch": \$((RANDOM % 100))
  }
}
EOF

echo ""
echo "2. Запуск нагрузочного тестирования..."
echo "   Параметры: 1000 запросов, 10 одновременных соединений"

# Нагрузочное тестирование с Apache Bench
if command -v ab &> /dev/null; then
    ab -n 1000 -c 10 \
       -H "Authorization: Bearer $AUTH_TOKEN" \
       -H "Content-Type: application/json" \
       -p /tmp/load_event.json \
       "$BULKER_URL/post/$DESTINATION_ID?tableName=$TABLE_NAME"
else
    echo "⚠️  Apache Bench (ab) не установлен. Используем curl для простого теста..."
    
    start_time=$(date +%s)
    success_count=0
    error_count=0
    
    for i in {1..100}; do
        # Генерация уникального события
        event_data="{\"user_id\": $((RANDOM % 10000)), \"action\": \"load_test\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"test_id\": $i}"
        
        response=$(curl -s -w "%{http_code}" \
                       -H "Authorization: Bearer $AUTH_TOKEN" \
                       -H "Content-Type: application/json" \
                       -d "$event_data" \
                       "$BULKER_URL/post/$DESTINATION_ID?tableName=$TABLE_NAME")
        
        http_code="${response: -3}"
        
        if [ "$http_code" = "200" ]; then
            ((success_count++))
        else
            ((error_count++))
        fi
        
        # Прогресс
        if [ $((i % 10)) -eq 0 ]; then
            echo "  Обработано: $i/100 (успешно: $success_count, ошибок: $error_count)"
        fi
    done
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    echo ""
    echo "📊 Результаты тестирования:"
    echo "   Общее время: ${duration}s"
    echo "   Успешных запросов: $success_count"
    echo "   Ошибок: $error_count"
    echo "   RPS: $((success_count / duration))"
fi

echo ""
echo "3. Проверка метрик после нагрузки..."
METRICS=$(curl -s "$BULKER_URL/metrics")
echo "   Обработано событий: $(echo "$METRICS" | grep bulker_events_total | awk '{print $2}' | head -1)"
echo "   HTTP запросов: $(echo "$METRICS" | grep bulker_requests_total | awk '{print $2}' | head -1)"

echo ""
echo "4. Проверка последних событий..."
EVENTS=$(curl -s "$BULKER_URL/events")
echo "   Всего событий в системе: $(echo "$EVENTS" | jq -r '.total // 0')"

# Очистка
rm -f /tmp/load_event.json

echo ""
echo "🎉 Нагрузочное тестирование завершено!"