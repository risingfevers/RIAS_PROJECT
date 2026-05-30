from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import pandas as pd
import requests
from io import BytesIO
from minio import Minio
import logging

logger = logging.getLogger(__name__)

MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin123',
    'secure': False
}

def check_event_generator_health(**context):
    """Проверяет здоровье генератора событий"""
    try:
        response = requests.get('http://localhost:8081/api/health', timeout=5)
        if response.status_code == 200:
            logger.info("✅ Event generator is healthy")
            return True
    except Exception as e:
        logger.error(f"❌ Event generator not responding: {e}")
        raise
    
    return False

def get_current_aggregates(**context):
    """Получает текущие агрегаты из API"""
    try:
        response = requests.get('http://localhost:8081/api/aggregates', timeout=5)
        if response.status_code == 200:
            aggregates = response.json()
            logger.info(f"📊 Current aggregates: {aggregates}")
            
            context['task_instance'].xcom_push(key='aggregates', value=aggregates)
            return aggregates
    except Exception as e:
        logger.error(f"Failed to get aggregates: {e}")
        raise

def save_aggregates_to_minio(**context):
    """Сохраняет агрегаты в MinIO"""
    aggregates = context['task_instance'].xcom_pull(task_ids='get_aggregates', key='aggregates')
    
    if not aggregates:
        logger.warning("No aggregates to save")
        return
    
    try:
        client = Minio(**MINIO_CONFIG)
        
        if not client.bucket_exists('realtime-aggregates'):
            client.make_bucket('realtime-aggregates')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"aggregates/agg_{timestamp}.json"
        
        json_data = json.dumps(aggregates, indent=2)
        client.put_object(
            'realtime-aggregates',
            filename,
            data=BytesIO(json_data.encode('utf-8')),
            length=len(json_data),
            content_type='application/json'
        )
        
        # Сохраняем последние агрегаты
        client.put_object(
            'realtime-aggregates',
            'latest.json',
            data=BytesIO(json_data.encode('utf-8')),
            length=len(json_data),
            content_type='application/json'
        )
        
        logger.info(f"✅ Saved aggregates to {filename}")
        
    except Exception as e:
        logger.error(f"Failed to save aggregates: {e}")
        raise

def generate_aggregate_report(**context):
    """Генерирует отчёт на основе агрегатов"""
    aggregates = context['task_instance'].xcom_pull(task_ids='get_aggregates', key='aggregates')
    
    if not aggregates:
        logger.warning("No aggregates for report")
        return
    
    report = f"""
    ╔══════════════════════════════════════════════════════════════════╗
    ║              REAL-TIME AGGREGATES REPORT                         ║
    ║                   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                    ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║                                                                   ║
    ║  📊 WINDOW:                                                       ║
    ║     Start: {aggregates.get('window_start', 'N/A')[:19]}                          ║
    ║     End:   {aggregates.get('window_end', 'N/A')[:19]}                          ║
    ║                                                                   ║
    ║  📈 METRICS:                                                      ║
    ║     Total events:          {aggregates.get('total_events', 0):>10}                         ║
    ║     Unique students:       {aggregates.get('unique_students', 0):>10}                         ║
    ║     Submissions:           {aggregates.get('submissions_count', 0):>10}                         ║
    ║     Avg quiz score:        {aggregates.get('avg_quiz_score', 0):>10.1f}                         ║
    ║     Engagement score:      {aggregates.get('engagement_score', 0):>10.1f}%                        ║
    ║                                                                   ║
    ║  🏢 BUILDINGS OCCUPANCY:                                          ║
    """
    
    for building, occupancy in aggregates.get('buildings_occupancy', {}).items():
        report += f"\n  ║     {building:<20} {occupancy:>3} people{' ' * 20}║"
    
    report += """
    ║                                                                   ║
    ║  🎯 TOP EVENTS:                                                   ║
    """
    
    for event_type, count in list(aggregates.get('event_counts', {}).items())[:5]:
        report += f"\n  ║     {event_type:<25} {count:>5} times{' ' * 20}║"
    
    report += """
    ║                                                                   ║
    ║  ✅ STATUS: REAL-TIME STREAMING ACTIVE                            ║
    ║                                                                   ║
    ╚══════════════════════════════════════════════════════════════════╝
    """
    
    logger.info(report)
    print(report)
    
    # Сохраняем отчёт
    try:
        client = Minio(**MINIO_CONFIG)
        
        if not client.bucket_exists('reports'):
            client.make_bucket('reports')
        
        report_buffer = BytesIO(report.encode('utf-8'))
        client.put_object(
            'reports',
            f'realtime_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
            data=report_buffer,
            length=report_buffer.getbuffer().nbytes,
            content_type='text/plain'
        )
    except Exception as e:
        logger.warning(f"Could not save report: {e}")
    
    return report

default_args = {
    'owner': 'realtime_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'streaming_pipeline',
    default_args=default_args,
    description='Real-time event streaming pipeline (no Kafka)',
    schedule_interval='*/2 * * * *',  # Каждые 2 минуты
    catchup=False,
    tags=['streaming', 'realtime', 'websocket']
)

health_check = PythonOperator(
    task_id='check_generator_health',
    python_callable=check_event_generator_health,
    provide_context=True,
    dag=dag
)

get_aggregates = PythonOperator(
    task_id='get_aggregates',
    python_callable=get_current_aggregates,
    provide_context=True,
    dag=dag
)

save_aggregates = PythonOperator(
    task_id='save_aggregates_to_minio',
    python_callable=save_aggregates_to_minio,
    provide_context=True,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_aggregate_report,
    provide_context=True,
    dag=dag
)

health_check >> get_aggregates >> save_aggregates >> generate_report
