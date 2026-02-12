# 🚚 Инструкция по локальному развертыванию Bulker

Данная инструкция поможет вам развернуть Bulker на локальной машине с помощью Docker.

## 📋 Содержание

1. [Требования](#требования)
2. [Быстрый старт](#быстрый-старт)
3. [Полное развертывание](#полное-развертывание)
4. [Конфигурации](#конфигурации)
5. [Тестирование](#тестирование)
6. [Управление](#управление)
7. [Устранение неполадок](#устранение-неполадок)

---

## 🔧 Требования

### Системные требования
- **ОС:** Linux, macOS, Windows (с WSL2)
- **RAM:** Минимум 4GB, рекомендуется 8GB
- **Диск:** Минимум 10GB свободного места
- **CPU:** 2+ ядра

### Программное обеспечение
- **Docker:** версия 20.10+
- **Docker Compose:** версия 2.0+
- **curl:** для тестирования API
- **jq:** для обработки JSON (опционально)

### Проверка установки
```bash
# Проверка Docker
docker --version
docker-compose --version

# Проверка запуска Docker
docker run hello-world

# Проверка дополнительных утилит
curl --version
jq --version  # опционально
```

---

## ⚡ Быстрый старт

Для быстрого тестирования используйте минимальную конфигурацию:

### 1. Клонирование репозитория
```bash
git clone <repository-url>
cd bulker
```

### 2. Запуск минимальной версии
```bash
# Запуск Mock сервера с PostgreSQL
./start-minimal.sh
```

### 3. Проверка работы
```bash
# Проверка готовности
curl http://localhost:3042/ready

# Запуск тестов
./test-api.sh
```

### 4. Остановка
```bash
docker-compose -f docker-compose.minimal.yml -p bulker-minimal down
```

---

## 🏗️ Полное развертывание

Для полнофункционального развертывания с Kafka, PostgreSQL и Redis:

### 1. Подготовка
```bash
# Убедитесь, что у вас достаточно ресурсов
docker system df
docker system prune  # очистка при необходимости
```

### 2. Запуск полной конфигурации
```bash
# Автоматический запуск всех сервисов
./start-local.sh
```

Скрипт выполнит:
- ✅ Проверку требований
- 🔨 Сборку образов
- 🚀 Запуск инфраструктуры (Kafka, PostgreSQL, Redis)
- 🚀 Запуск Bulker приложения
- 🔍 Проверку готовности всех сервисов

### 3. Проверка развертывания
```bash
# Статус контейнеров
docker-compose -f docker-compose.local.yml -p bulker-local ps

# Проверка логов
docker-compose -f docker-compose.local.yml -p bulker-local logs bulker

# Тестирование API
./test-api.sh
```

### 4. Остановка
```bash
./stop-local.sh
```

---

## 📁 Конфигурации

### Доступные конфигурации

| Файл | Описание | Использование |
|------|----------|---------------|
| `docker-compose.minimal.yml` | Минимальная (Mock + PostgreSQL) | Быстрое тестирование |
| `docker-compose.local.yml` | Полная (все сервисы) | Локальная разработка |
| `docker-compose.test.yml` | Тестовая (с health checks) | CI/CD, тестирование |

### Порты сервисов

| Сервис | Порт | Описание |
|--------|------|----------|
| Bulker API | 3042 | Основной HTTP API |
| Bulker Metrics | 9090 | Prometheus метрики |
| Ingest Service | 3043 | Сервис приема данных |
| PostgreSQL | 5432 | База данных |
| Kafka | 9092/9093 | Брокер сообщений |
| Redis | 6379 | Кеш и сессии |
| Zookeeper | 2181 | Координация Kafka |

### Переменные окружения

#### Основные настройки
```bash
BULKER_HTTP_PORT=3042                    # Порт HTTP API
BULKER_INSTANCE_ID=local-instance        # ID экземпляра
BULKER_LOG_LEVEL=INFO                    # Уровень логирования
BULKER_LOG_FORMAT=json                   # Формат логов
```

#### Kafka
```bash
BULKER_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
BULKER_KAFKA_CONSUMER_GROUP_ID=bulker-local
BULKER_KAFKA_TOPIC_PREFIX=bulker_local_
```

#### PostgreSQL
```bash
BULKER_DESTINATION_POSTGRES_HOST=postgres
BULKER_DESTINATION_POSTGRES_PORT=5432
BULKER_DESTINATION_POSTGRES_DB=bulker
BULKER_DESTINATION_POSTGRES_USER=bulker
BULKER_DESTINATION_POSTGRES_PASSWORD=bulker_password
```

#### Авторизация
```bash
BULKER_RAW_AUTH_TOKENS=local-token-123,admin-token-456,test-token-789
```

---

## 🧪 Тестирование

### Автоматическое тестирование
```bash
# Запуск всех тестов
./test-api.sh
```

### Ручное тестирование

#### Проверка готовности
```bash
curl http://localhost:3042/ready
```

#### Отправка события
```bash
curl -X POST \
  -H "Authorization: Bearer local-token-123" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 123, "action": "test", "timestamp": "2025-12-13T06:00:00Z"}' \
  "http://localhost:3042/post/postgres_test?tableName=user_events"
```

#### Просмотр метрик
```bash
curl http://localhost:3042/metrics
```

#### Просмотр событий
```bash
curl http://localhost:3042/events
```

### Тестирование производительности
```bash
# Нагрузочное тестирование с Apache Bench
ab -n 1000 -c 10 \
   -H "Authorization: Bearer local-token-123" \
   -H "Content-Type: application/json" \
   -p event.json \
   http://localhost:3042/post/test?tableName=load_test
```

---

## 🎛️ Управление

### Просмотр логов
```bash
# Все сервисы
docker-compose -f docker-compose.local.yml -p bulker-local logs -f

# Конкретный сервис
docker-compose -f docker-compose.local.yml -p bulker-local logs -f bulker
docker-compose -f docker-compose.local.yml -p bulker-local logs -f kafka
docker-compose -f docker-compose.local.yml -p bulker-local logs -f postgres
```

### Мониторинг ресурсов
```bash
# Использование ресурсов контейнерами
docker stats

# Использование дискового пространства
docker system df

# Информация о volumes
docker volume ls
docker volume inspect bulker-local_postgres_data
```

### Подключение к сервисам

#### PostgreSQL
```bash
# Через Docker
docker-compose -f docker-compose.local.yml -p bulker-local exec postgres psql -U bulker -d bulker

# Через локальный клиент
psql -h localhost -p 5432 -U bulker -d bulker
```

#### Kafka
```bash
# Список топиков
docker-compose -f docker-compose.local.yml -p bulker-local exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Просмотр сообщений
docker-compose -f docker-compose.local.yml -p bulker-local exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic bulker_local_events --from-beginning
```

#### Redis
```bash
# Подключение к Redis CLI
docker-compose -f docker-compose.local.yml -p bulker-local exec redis redis-cli
```

### Масштабирование
```bash
# Увеличение количества экземпляров Bulker
docker-compose -f docker-compose.local.yml -p bulker-local up -d --scale bulker=3

# Просмотр экземпляров
docker-compose -f docker-compose.local.yml -p bulker-local ps bulker
```

---

## 🔧 Устранение неполадок

### Общие проблемы

#### 1. Контейнеры не запускаются
```bash
# Проверка логов
docker-compose -f docker-compose.local.yml -p bulker-local logs

# Проверка ресурсов
docker system df
free -h  # проверка RAM

# Очистка
docker system prune -a
```

#### 2. Порты заняты
```bash
# Проверка занятых портов
netstat -tulpn | grep :3042
lsof -i :3042

# Остановка конфликтующих сервисов
sudo systemctl stop postgresql  # если локальный PostgreSQL
sudo systemctl stop redis       # если локальный Redis
```

#### 3. Проблемы с сетью
```bash
# Проверка Docker сетей
docker network ls
docker network inspect bulker-local_bulker-network

# Пересоздание сети
docker-compose -f docker-compose.local.yml -p bulker-local down
docker network prune
docker-compose -f docker-compose.local.yml -p bulker-local up -d
```

#### 4. Проблемы с volumes
```bash
# Проверка volumes
docker volume ls | grep bulker
docker volume inspect bulker-local_postgres_data

# Очистка volumes (ВНИМАНИЕ: удалит данные!)
docker-compose -f docker-compose.local.yml -p bulker-local down -v
```

### Специфичные проблемы

#### Kafka не запускается
```bash
# Проверка Zookeeper
docker-compose -f docker-compose.local.yml -p bulker-local logs zookeeper

# Увеличение памяти для Kafka
export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
docker-compose -f docker-compose.local.yml -p bulker-local up -d kafka
```

#### PostgreSQL проблемы с подключением
```bash
# Проверка готовности
docker-compose -f docker-compose.local.yml -p bulker-local exec postgres pg_isready -U bulker

# Проверка настроек
docker-compose -f docker-compose.local.yml -p bulker-local exec postgres psql -U bulker -d bulker -c "SELECT version();"
```

#### Bulker не отвечает
```bash
# Проверка health check
docker-compose -f docker-compose.local.yml -p bulker-local ps bulker

# Проверка переменных окружения
docker-compose -f docker-compose.local.yml -p bulker-local exec bulker env | grep BULKER

# Перезапуск
docker-compose -f docker-compose.local.yml -p bulker-local restart bulker
```

### Диагностические команды
```bash
# Полная диагностика
echo "=== Docker Info ==="
docker info

echo "=== Container Status ==="
docker-compose -f docker-compose.local.yml -p bulker-local ps

echo "=== Resource Usage ==="
docker stats --no-stream

echo "=== Network Info ==="
docker network ls

echo "=== Volume Info ==="
docker volume ls

echo "=== Recent Logs ==="
docker-compose -f docker-compose.local.yml -p bulker-local logs --tail=50
```

---

## 📚 Дополнительные ресурсы

### Документация
- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

### Полезные команды
```bash
# Создание backup PostgreSQL
docker-compose -f docker-compose.local.yml -p bulker-local exec postgres pg_dump -U bulker bulker > backup.sql

# Восстановление backup
docker-compose -f docker-compose.local.yml -p bulker-local exec -T postgres psql -U bulker -d bulker < backup.sql

# Мониторинг в реальном времени
watch -n 2 'docker-compose -f docker-compose.local.yml -p bulker-local ps'
```

### Конфигурация для production
Для production развертывания рекомендуется:
1. Использовать внешние базы данных
2. Настроить SSL/TLS
3. Использовать secrets для паролей
4. Настроить мониторинг и алерты
5. Использовать reverse proxy (nginx)
6. Настроить backup и восстановление

---

## 🆘 Поддержка

Если у вас возникли проблемы:

1. **Проверьте логи:** `docker-compose logs`
2. **Запустите диагностику:** используйте команды из раздела "Устранение неполадок"
3. **Проверьте ресурсы:** убедитесь, что достаточно RAM и дискового пространства
4. **Перезапустите сервисы:** `./stop-local.sh && ./start-local.sh`

**Успешного развертывания! 🚀**