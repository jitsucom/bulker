# 🚚 Bulker - Локальное развертывание с Docker

Быстрое развертывание системы потоковой обработки данных Bulker на локальной машине.

## ⚡ Быстрый старт

### 1. Требования
- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ RAM, 10GB+ диск

### 2. Минимальная версия (Mock сервер)
```bash
# Клонирование
git clone <repository-url>
cd bulker

# Запуск
./start-minimal.sh

# Тестирование
./test-api.sh

# Остановка
docker-compose -f docker-compose.minimal.yml -p bulker-minimal down
```

### 3. Полная версия (с Kafka, PostgreSQL, Redis)
```bash
# Запуск (автоматическая сборка и настройка)
./start-local.sh

# Тестирование
./test-api.sh

# Остановка
./stop-local.sh
```

## 🔗 Доступные сервисы

| Сервис | URL | Описание |
|--------|-----|----------|
| **Bulker API** | http://localhost:3042 | Основной HTTP API |
| **Metrics** | http://localhost:9090 | Prometheus метрики |
| **Ingest** | http://localhost:3043 | Сервис приема данных |
| **PostgreSQL** | localhost:5432 | База данных (bulker/bulker_password) |

## 🔑 Авторизация

Используйте Bearer токены:
- `local-token-123`
- `admin-token-456` 
- `test-token-789`

## 🧪 Примеры использования

### Отправка события
```bash
curl -X POST \
  -H "Authorization: Bearer local-token-123" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 123, "action": "login"}' \
  "http://localhost:3042/post/postgres_test?tableName=events"
```

### Просмотр метрик
```bash
curl http://localhost:3042/metrics
```

### Список событий
```bash
curl http://localhost:3042/events
```

## 📁 Файлы конфигурации

- `docker-compose.minimal.yml` - Минимальная конфигурация
- `docker-compose.local.yml` - Полная конфигурация
- `start-local.sh` - Скрипт запуска полной версии
- `start-minimal.sh` - Скрипт запуска минимальной версии
- `stop-local.sh` - Скрипт остановки
- `test-api.sh` - Скрипт тестирования

## 🔧 Управление

### Просмотр логов
```bash
docker-compose -f docker-compose.local.yml -p bulker-local logs -f bulker
```

### Статус сервисов
```bash
docker-compose -f docker-compose.local.yml -p bulker-local ps
```

### Подключение к PostgreSQL
```bash
docker-compose -f docker-compose.local.yml -p bulker-local exec postgres psql -U bulker -d bulker
```

## 🆘 Устранение неполадок

### Порты заняты
```bash
# Проверка
netstat -tulpn | grep :3042

# Остановка конфликтующих сервисов
sudo systemctl stop postgresql redis
```

### Недостаточно ресурсов
```bash
# Очистка Docker
docker system prune -a

# Проверка ресурсов
docker system df
free -h
```

### Перезапуск
```bash
./stop-local.sh
./start-local.sh
```

## 📖 Подробная документация

Полная инструкция: [LOCAL_DEPLOYMENT_GUIDE.md](LOCAL_DEPLOYMENT_GUIDE.md)

---

**Готово к использованию! 🎉**

Для вопросов и поддержки проверьте логи: `docker-compose logs`