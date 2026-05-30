from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import random
import uuid
from minio import Minio
from io import BytesIO

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def generate_and_upload_to_minio():
    """Генерирует события и загружает напрямую в MinIO"""
    
    # Подключаемся к MinIO
    client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    # Создаём бакет если не существует
    if not client.bucket_exists('student-engagement-bucket'):
        client.make_bucket('student-engagement-bucket')
    
    # Генерируем события
    actions = ['login', 'view_course', 'watch_video', 'submit_assignment', 'post_comment', 'logout']
    devices = ['web', 'mobile', 'tablet']
    courses = ['CS101', 'CS102', 'MATH201', 'PHYS101', 'ENG202']
    
    events = []
    for i in range(25):  # 25 событий за раз
        event = {
            'event_id': str(uuid.uuid4()),
            'user_id': random.randint(1001, 1100),
            'course_id': random.choice(courses),
            'action': random.choice(actions),
            'timestamp': datetime.now().isoformat(),
            'session_duration_sec': random.randint(5, 3600),
            'device': random.choice(devices),
            'domain': 'student_engagement'
        }
        events.append(event)
    
    # Сохраняем в JSON
    filename = f"engagement_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
    json_data = json.dumps(events, indent=2)
    
    # Загружаем в MinIO
    client.put_object(
        'student-engagement-bucket',
        filename,
        data=BytesIO(json_data.encode('utf-8')),
        length=len(json_data),
        content_type='application/json'
    )
    
    print(f"✅ Успешно загружено {len(events)} событий в файл: {filename}")
    return filename

def quality_check_minio():
    """Проверяет качество данных в MinIO"""
    
    client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    total_events = 0
    valid_events = 0
    invalid_reasons = []
    
    # Получаем список объектов
    objects = client.list_objects('student-engagement-bucket', recursive=True)
    
    for obj in objects:
        # Скачиваем объект
        response = client.get_object('student-engagement-bucket', obj.object_name)
        data = json.loads(response.read())
        response.close()
        
        for event in data:
            total_events += 1
            
            # Проверяем обязательные поля
            required_fields = ['event_id', 'user_id', 'action', 'timestamp', 'domain']
            missing_fields = [f for f in required_fields if f not in event]
            
            if not missing_fields:
                valid_events += 1
            else:
                invalid_reasons.append(f"{obj.object_name}: missing {missing_fields}")
    
    quality_score = (valid_events / total_events * 100) if total_events > 0 else 0
    
    print("=" * 50)
    print("📊 DATA QUALITY REPORT")
    print("=" * 50)
    print(f"Total events checked: {total_events}")
    print(f"Valid events: {valid_events}")
    print(f"Quality score: {quality_score:.2f}%")
    
    if invalid_reasons[:5]:  # показываем первые 5 ошибок
        print(f"\n❌ Sample issues:")
        for reason in invalid_reasons[:5]:
            print(f"  - {reason}")
    
    print("=" * 50)
    
    # Метрики для SLA
    metrics = {
        'completeness': valid_events / total_events if total_events > 0 else 0,
        'total_events': total_events,
        'quality_score': quality_score
    }
    
    return metrics

def generate_report(**context):
    """Создаёт отчёт о качестве данных"""
    
    quality_metrics = context['ti'].xcom_pull(task_ids='quality_check')
    
    report = {
        'report_id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'pipeline': 'student_engagement',
        'metrics': quality_metrics,
        'sla_status': 'PASSED' if quality_metrics and quality_metrics['quality_score'] >= 95 else 'FAILED'
    }
    
    # Сохраняем отчёт в MinIO
    client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    report_filename = f"reports/quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_json = json.dumps(report, indent=2)
    
    client.put_object(
        'student-engagement-bucket',
        report_filename,
        data=BytesIO(report_json.encode('utf-8')),
        length=len(report_json),
        content_type='application/json'
    )
    
    print(f"✅ Отчёт сохранён: {report_filename}")
    print(f"📈 SLA Status: {report['sla_status']}")

# Определяем DAG
dag = DAG(
    'student_engagement_pipeline',
    default_args=default_args,
    description='Pipeline for student engagement data (без Kafka)',
    schedule_interval='*/10 * * * *',  # каждые 10 минут
    catchup=False,
    tags=['student', 'engagement', 'minio']
)

# Создаём задачи
generate_task = PythonOperator(
    task_id='generate_and_upload',
    python_callable=generate_and_upload_to_minio,
    dag=dag
)

quality_task = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check_minio,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag
)

# Определяем порядок выполнения
generate_task >> quality_task >> report_task
