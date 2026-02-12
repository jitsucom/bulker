-- Инициализация базы данных для Bulker
-- Создание схемы по умолчанию
CREATE SCHEMA IF NOT EXISTS public;

-- Создание пользователя для Bulker (если нужно)
-- Пользователь уже создается через переменные окружения в docker-compose

-- Предоставление прав
GRANT ALL PRIVILEGES ON DATABASE bulker_test TO bulker;
GRANT ALL PRIVILEGES ON SCHEMA public TO bulker;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bulker;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bulker;

-- Создание тестовой таблицы для проверки
CREATE TABLE IF NOT EXISTS public.test_events (
    id SERIAL PRIMARY KEY,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставка тестовых данных
INSERT INTO public.test_events (event_data) VALUES 
    ('{"user_id": 1, "action": "login", "timestamp": "2025-12-13T10:00:00Z"}'),
    ('{"user_id": 2, "action": "signup", "timestamp": "2025-12-13T10:05:00Z"}');

-- Создание индексов для производительности
CREATE INDEX IF NOT EXISTS idx_test_events_created_at ON public.test_events(created_at);
CREATE INDEX IF NOT EXISTS idx_test_events_event_data ON public.test_events USING GIN(event_data);