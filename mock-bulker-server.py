#!/usr/bin/env python3
"""
Mock Bulker HTTP API Server для демонстрации
Имитирует основные endpoints Bulker API
"""

import json
import time
import uuid
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading
import sqlite3
import os

class MockBulkerHandler(BaseHTTPRequestHandler):
    
    def __init__(self, *args, **kwargs):
        # Инициализация базы данных
        self.init_db()
        super().__init__(*args, **kwargs)
    
    def init_db(self):
        """Инициализация SQLite базы данных для хранения событий"""
        if not hasattr(self.__class__, 'db_initialized'):
            conn = sqlite3.connect('/tmp/bulker_mock.db')
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    destination_id TEXT,
                    table_name TEXT,
                    event_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            conn.close()
            self.__class__.db_initialized = True
    
    def do_GET(self):
        """Обработка GET запросов"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == '/ready':
            self.handle_ready()
        elif path == '/metrics':
            self.handle_metrics()
        elif path == '/events':
            self.handle_get_events()
        elif path == '/health':
            self.handle_health()
        else:
            self.send_error(404, "Endpoint not found")
    
    def do_POST(self):
        """Обработка POST запросов"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)
        
        if path.startswith('/post/'):
            destination_id = path.split('/post/')[1]
            table_name = query_params.get('tableName', ['default'])[0]
            self.handle_post_event(destination_id, table_name)
        else:
            self.send_error(404, "Endpoint not found")
    
    def handle_ready(self):
        """Endpoint для проверки готовности"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {"status": "ready", "timestamp": datetime.now().isoformat()}
        self.wfile.write(json.dumps(response).encode())
    
    def handle_health(self):
        """Endpoint для проверки здоровья"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {
            "status": "healthy",
            "version": "mock-1.0.0",
            "uptime": "running",
            "timestamp": datetime.now().isoformat()
        }
        self.wfile.write(json.dumps(response).encode())
    
    def handle_metrics(self):
        """Endpoint для метрик (Prometheus format)"""
        conn = sqlite3.connect('/tmp/bulker_mock.db')
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM events')
        total_events = cursor.fetchone()[0]
        conn.close()
        
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        
        metrics = f"""# HELP bulker_events_total Total number of events processed
# TYPE bulker_events_total counter
bulker_events_total {total_events}

# HELP bulker_uptime_seconds Uptime in seconds
# TYPE bulker_uptime_seconds gauge
bulker_uptime_seconds {time.time() - start_time}

# HELP bulker_requests_total Total number of HTTP requests
# TYPE bulker_requests_total counter
bulker_requests_total {getattr(self.__class__, 'request_count', 0)}
"""
        self.wfile.write(metrics.encode())
    
    def handle_post_event(self, destination_id, table_name):
        """Обработка отправки события"""
        # Проверка авторизации
        auth_header = self.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            self.send_error(401, "Missing or invalid authorization header")
            return
        
        token = auth_header.split('Bearer ')[1]
        # Получение токенов из переменных окружения
        env_tokens = os.environ.get('MOCK_TOKENS', 'test-token-123,admin-token-456')
        valid_tokens = [t.strip() for t in env_tokens.split(',')]
        if token not in valid_tokens:
            self.send_error(401, "Invalid token")
            return
        
        # Чтение данных события
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            self.send_error(400, "Empty request body")
            return
        
        try:
            event_data = self.rfile.read(content_length).decode('utf-8')
            event_json = json.loads(event_data)
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON in request body")
            return
        
        # Сохранение события в базу данных
        event_id = str(uuid.uuid4())
        conn = sqlite3.connect('/tmp/bulker_mock.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO events (id, destination_id, table_name, event_data)
            VALUES (?, ?, ?, ?)
        ''', (event_id, destination_id, table_name, event_data))
        conn.commit()
        conn.close()
        
        # Увеличение счетчика запросов
        if not hasattr(self.__class__, 'request_count'):
            self.__class__.request_count = 0
        self.__class__.request_count += 1
        
        # Имитация обработки
        processing_time = 0.1  # 100ms
        time.sleep(processing_time)
        
        # Отправка ответа
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        response = {
            "success": True,
            "event_id": event_id,
            "destination_id": destination_id,
            "table_name": table_name,
            "processing_time_ms": int(processing_time * 1000),
            "timestamp": datetime.now().isoformat()
        }
        self.wfile.write(json.dumps(response).encode())
        
        # Логирование
        print(f"✅ Event processed: {event_id} -> {destination_id}.{table_name}")
    
    def handle_get_events(self):
        """Получение списка обработанных событий"""
        conn = sqlite3.connect('/tmp/bulker_mock.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, destination_id, table_name, event_data, created_at 
            FROM events 
            ORDER BY created_at DESC 
            LIMIT 100
        ''')
        events = cursor.fetchall()
        conn.close()
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        events_list = []
        for event in events:
            events_list.append({
                "id": event[0],
                "destination_id": event[1],
                "table_name": event[2],
                "event_data": json.loads(event[3]),
                "created_at": event[4]
            })
        
        response = {
            "events": events_list,
            "total": len(events_list)
        }
        self.wfile.write(json.dumps(response, indent=2).encode())
    
    def log_message(self, format, *args):
        """Кастомное логирование"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {format % args}")

def run_server(port=None):
    """Запуск mock сервера"""
    global start_time
    start_time = time.time()
    
    # Получение порта из переменных окружения или аргумента
    if port is None:
        port = int(os.environ.get('MOCK_PORT', 12000))
    
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, MockBulkerHandler)
    
    print(f"""
🚚 Mock Bulker Server запущен!
=================================
URL: http://localhost:{port}
Доступные endpoints:
  - GET  /ready          - проверка готовности
  - GET  /health         - проверка здоровья  
  - GET  /metrics        - метрики Prometheus
  - GET  /events         - список событий
  - POST /post/:dest?tableName=:table - отправка события

Авторизация: Bearer test-token-123 или Bearer admin-token-456

Примеры использования:
  curl http://localhost:{port}/ready
  curl -H "Authorization: Bearer test-token-123" \\
       -H "Content-Type: application/json" \\
       -d '{{"user_id": 123, "action": "test"}}' \\
       http://localhost:{port}/post/postgres_test?tableName=events

Для остановки нажмите Ctrl+C
=================================
""")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
        httpd.shutdown()

if __name__ == '__main__':
    run_server()