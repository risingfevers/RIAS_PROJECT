#!/usr/bin/env python3
"""
Упрощённый генератор потоковых событий (только встроенные библиотеки)
"""

import json
import random
import uuid
import time
import threading
import queue
from datetime import datetime, timedelta
from typing import Dict, Any
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver
import urllib.parse

# Конфигурация
MINIO_CONFIG = {
    'endpoint': 'localhost:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin123',
    'secure': False
}

# Глобальная очередь событий
event_queue = queue.Queue()
latest_aggregates = {}
aggregates_lock = threading.Lock()

class StudentEventGenerator:
    """Генератор событий студентов"""
    
    def __init__(self):
        self.students = list(range(1001, 1101))  # 100 студентов
        self.courses = ['CS101', 'CS102', 'MATH201', 'PHYS101', 'ENG202', 'HIST101']
        self.buildings = ['Main', 'Library', 'CS Building', 'Student Center', 'Sports Complex']
        self.event_types = [
            'login', 'logout', 'enter_building', 'exit_building', 
            'submit_assignment', 'view_course', 'watch_video',
            'post_comment', 'start_quiz', 'finish_quiz'
        ]
        
    def generate_event(self) -> Dict[str, Any]:
        """Генерирует одно случайное событие"""
        event_type = random.choice(self.event_types)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'student_id': random.choice(self.students),
            'course_id': random.choice(self.courses) if random.random() > 0.3 else None,
            'building': random.choice(self.buildings) if 'building' in event_type or random.random() > 0.7 else None,
            'session_duration_sec': random.randint(5, 1800) if event_type == 'logout' else None,
            'device': random.choice(['web', 'mobile', 'tablet']),
            'user_agent': random.choice(['chrome', 'firefox', 'safari', 'mobile_app']),
            'domain': 'realtime_events',
            'correlation_id': str(uuid.uuid4())
        }
        
        # Добавляем специфичные поля
        if event_type == 'submit_assignment':
            event['assignment_id'] = f"hw_{random.randint(1, 10)}"
            event['submission_time_minutes'] = random.randint(1, 120)
        elif event_type in ['start_quiz', 'finish_quiz']:
            event['quiz_id'] = f"quiz_{random.randint(1, 5)}"
            event['score'] = round(random.uniform(0, 100), 1) if event_type == 'finish_quiz' else None
        
        return event


class EventAggregator:
    """Агрегирует события в скользящие окна"""
    
    def __init__(self, window_size_seconds=300, slide_seconds=60):
        self.window_size_seconds = window_size_seconds
        self.slide_seconds = slide_seconds
        self.windows = {}
        
    def aggregate_event(self, event: Dict[str, Any]):
        """Агрегирует одно событие в текущие окна"""
        event_time = datetime.fromisoformat(event['timestamp'])
        
        # Для каждого окна, в которое попадает событие
        for window_start in self._get_windows_for_time(event_time):
            window_key = window_start.isoformat()
            
            if window_key not in self.windows:
                self.windows[window_key] = {
                    'window_start': window_start.isoformat(),
                    'window_end': (window_start + timedelta(seconds=self.window_size_seconds)).isoformat(),
                    'event_counts': {},
                    'students_active': set(),
                    'buildings_occupancy': {},
                    'total_events': 0,
                    'submissions_count': 0,
                    'quiz_scores': []
                }
            
            window = self.windows[window_key]
            
            # Счётчик событий по типам
            event_type = event['event_type']
            window['event_counts'][event_type] = window['event_counts'].get(event_type, 0) + 1
            window['total_events'] += 1
            
            # Активные студенты
            window['students_active'].add(event['student_id'])
            
            # Загруженность зданий
            if event.get('building'):
                building = event['building']
                if event_type == 'enter_building':
                    window['buildings_occupancy'][building] = window['buildings_occupancy'].get(building, 0) + 1
                elif event_type == 'exit_building':
                    window['buildings_occupancy'][building] = max(0, window['buildings_occupancy'].get(building, 0) - 1)
            
            # Сдача заданий
            if event_type == 'submit_assignment':
                window['submissions_count'] += 1
            
            # Результаты викторин
            if event_type == 'finish_quiz' and event.get('score'):
                window['quiz_scores'].append(event['score'])
    
    def _get_windows_for_time(self, event_time):
        """Возвращает все окна, в которые попадает событие"""
        windows = []
        epoch = datetime(2026, 1, 1, 0, 0, 0)
        seconds_since_epoch = (event_time - epoch).total_seconds()
        
        start_offset = seconds_since_epoch % self.slide_seconds
        first_window_start = event_time - timedelta(seconds=start_offset)
        
        for i in range(3):
            window_start = first_window_start - timedelta(seconds=i * self.slide_seconds)
            window_end = window_start + timedelta(seconds=self.window_size_seconds)
            
            if window_start <= event_time < window_end:
                windows.append(window_start)
        
        return windows
    
    def get_current_aggregates(self):
        """Возвращает текущие агрегированные данные"""
        if not self.windows:
            return None
        
        # Очищаем старые окна (старше 10 минут)
        current_time = datetime.now()
        old_keys = []
        for key, window in self.windows.items():
            window_end = datetime.fromisoformat(window['window_end'])
            if (current_time - window_end).total_seconds() > 600:  # 10 минут
                old_keys.append(key)
        
        for key in old_keys:
            del self.windows[key]
        
        if not self.windows:
            return None
        
        latest_window_key = max(self.windows.keys())
        latest = self.windows[latest_window_key]
        
        avg_quiz_score = sum(latest['quiz_scores']) / len(latest['quiz_scores']) if latest['quiz_scores'] else 0
        
        # Конвертируем set в list для JSON
        return {
            'timestamp': datetime.now().isoformat(),
            'window_start': latest['window_start'],
            'window_end': latest['window_end'],
            'total_events': latest['total_events'],
            'unique_students': len(latest['students_active']),
            'top_event': max(latest['event_counts'].items(), key=lambda x: x[1])[0] if latest['event_counts'] else 'none',
            'event_counts': latest['event_counts'],
            'buildings_occupancy': latest['buildings_occupancy'],
            'submissions_count': latest['submissions_count'],
            'avg_quiz_score': round(avg_quiz_score, 1),
            'engagement_score': round(min(100, (latest['total_events'] / max(1, len(latest['students_active'])) * 10)), 1)
        }


# HTTP обработчик для API
class APIHandler(BaseHTTPRequestHandler):
    aggregator = None
    generator = None
    event_queue = None
    
    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        
        if parsed_path.path == '/api/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            response = json.dumps({'status': 'healthy', 'timestamp': datetime.now().isoformat()})
            self.wfile.write(response.encode())
            
        elif parsed_path.path == '/api/aggregates':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            aggregates = APIHandler.aggregator.get_current_aggregates() if APIHandler.aggregator else None
            response = json.dumps(aggregates if aggregates else {'error': 'No data yet', 'timestamp': datetime.now().isoformat()})
            self.wfile.write(response.encode())
            
        elif parsed_path.path == '/api/events/latest':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            # Получаем последние события из очереди
            events = []
            if APIHandler.event_queue:
                queue_copy = list(APIHandler.event_queue.queue)[-20:]
                events = queue_copy
            response = json.dumps(events)
            self.wfile.write(response.encode())
            
        elif parsed_path.path == '/api/events/history':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            events = []
            if APIHandler.event_queue:
                events = list(APIHandler.event_queue.queue)[-100:]
            response = json.dumps(events)
            self.wfile.write(response.encode())
            
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response = json.dumps({'error': 'Not found'})
            self.wfile.write(response.encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def log_message(self, format, *args):
        pass  # Отключаем логирование


def event_generator_loop(generator, aggregator, event_queue, stop_event):
    """Цикл генерации событий"""
    print("🎲 Starting event generation loop...")
    event_count = 0
    
    while not stop_event.is_set():
        try:
            # Генерируем событие
            event = generator.generate_event()
            
            # Агрегируем
            aggregator.aggregate_event(event)
            
            # Добавляем в очередь
            event_queue.put(event)
            
            event_count += 1
            
            # Печатаем каждое 5-е событие
            if event_count % 5 == 0:
                print(f"📡 Event #{event_count}: {event['event_type']} - Student {event['student_id']}")
            
            # Интервал между событиями (0.5-2 секунды)
            time.sleep(random.uniform(0.5, 1.5))
            
        except Exception as e:
            print(f"❌ Error generating event: {e}")
            time.sleep(1)


def status_printer_loop(aggregator, stop_event):
    """Печатает статус каждые 10 секунд"""
    while not stop_event.is_set():
        time.sleep(10)
        agg = aggregator.get_current_aggregates()
        if agg:
            print(f"\n📊 [STATUS] {datetime.now().strftime('%H:%M:%S')} | "
                  f"Events: {agg['total_events']} | "
                  f"Students: {agg['unique_students']} | "
                  f"Engagement: {agg['engagement_score']}% | "
                  f"Submissions: {agg['submissions_count']}")
        else:
            print(f"\n📊 [STATUS] {datetime.now().strftime('%H:%M:%S')} - Waiting for events...")


def save_batch_to_file(batch, batch_num):
    """Сохраняет батч событий в JSON файл"""
    if not batch:
        return
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"events_batch_{timestamp}_{batch_num}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(batch, f, indent=2, ensure_ascii=False)
        print(f"💾 Saved {len(batch)} events to {filename}")
    except Exception as e:
        print(f"⚠️ Failed to save batch: {e}")


def batch_saver_loop(event_queue, stop_event):
    """Периодически сохраняет батч событий в файлы"""
    batch = []
    batch_num = 0
    
    while not stop_event.is_set():
        time.sleep(15)  # Каждые 15 секунд
        
        # Забираем все события из очереди
        while not event_queue.empty():
            try:
                event = event_queue.get_nowait()
                batch.append(event)
            except:
                break
        
        # Сохраняем батч если есть события
        if batch:
            batch_num += 1
            save_batch_to_file(batch, batch_num)
            batch = []


def main():
    """Запуск всех сервисов"""
    print("=" * 60)
    print("🎓 REAL-TIME STUDENT EVENTS PLATFORM")
    print("=" * 60)
    
    # Инициализация компонентов
    generator = StudentEventGenerator()
    aggregator = EventAggregator(window_size_seconds=300, slide_seconds=60)
    
    # Настраиваем API обработчик
    APIHandler.aggregator = aggregator
    APIHandler.generator = generator
    APIHandler.event_queue = event_queue
    
    # Флаги остановки
    stop_event = threading.Event()
    
    # Запускаем потоки
    print("\n🚀 Starting threads...")
    
    generator_thread = threading.Thread(
        target=event_generator_loop,
        args=(generator, aggregator, event_queue, stop_event),
        daemon=True
    )
    
    status_thread = threading.Thread(
        target=status_printer_loop,
        args=(aggregator, stop_event),
        daemon=True
    )
    
    saver_thread = threading.Thread(
        target=batch_saver_loop,
        args=(event_queue, stop_event),
        daemon=True
    )
    
    generator_thread.start()
    status_thread.start()
    saver_thread.start()
    
    # Запускаем HTTP сервер
    PORT = 8081
    print(f"\n🌐 Starting HTTP API server on http://localhost:{PORT}")
    print("   Available endpoints:")
    print("   - GET /api/health - Health check")
    print("   - GET /api/aggregates - Current aggregates")
    print("   - GET /api/events/latest - Latest events")
    print("   - GET /api/events/history - Event history")
    
    print("\n✅ Server running! Press Ctrl+C to stop\n")
    
    try:
        with socketserver.TCPServer(("0.0.0.0", PORT), APIHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down...")
        stop_event.set()
    except Exception as e:
        print(f"\n❌ Server error: {e}")
    
    print("👋 Goodbye!")


if __name__ == "__main__":
    main()
