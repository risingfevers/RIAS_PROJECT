from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import random
import uuid
import os

default_args = {
    'owner': 'facility_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def generate_facility_data():
    """Генерирует данные об использовании инфраструктуры кампуса"""
    
    os.makedirs('/opt/airflow/data/campus_infrastructure', exist_ok=True)
    
    buildings = ['Main', 'Library', 'CS Building', 'Student Center', 'Sports Complex']
    rooms = ['101', '202', 'Auditorium', 'Lab 1', 'Study Room']
    
    facilities = []
    for i in range(30):
        facility = {
            'facility_id': str(uuid.uuid4()),
            'building': random.choice(buildings),
            'room': random.choice(rooms),
            'occupancy': random.randint(0, 100),
            'temperature_celsius': round(random.uniform(18, 26), 1),
            'energy_usage_kwh': round(random.uniform(10, 200), 1),
            'timestamp': datetime.now().isoformat(),
            'domain': 'campus_infrastructure'
        }
        facilities.append(facility)
    
    filename = f"/opt/airflow/data/campus_infrastructure/facility_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(facilities, f, indent=2)
    
    print(f"✅ Сгенерировано записей: {len(facilities)}")
    print(f"📁 Сохранено: {filename}")
    
    # Анализ загруженности
    high_occupancy = len([f for f in facilities if f['occupancy'] > 80])
    print(f"🏢 Зон с высокой загрузкой (>80%): {high_occupancy}")
    
    return filename

def check_quality():
    """Проверка качества данных инфраструктуры"""
    
    data_dir = '/opt/airflow/data/campus_infrastructure'
    if not os.path.exists(data_dir):
        print("❌ Папка с данными не найдена")
        return
    
    files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    if not files:
        print("❌ Нет файлов для проверки")
        return
    
    total_records = 0
    valid_records = 0
    
    for filename in files[-5:]:
        with open(os.path.join(data_dir, filename), 'r') as f:
            records = json.load(f)
            total_records += len(records)
            
            for record in records:
                required_fields = ['facility_id', 'building', 'occupancy', 'timestamp', 'domain']
                if all(field in record for field in required_fields):
                    valid_records += 1
    
    quality_score = (valid_records / total_records * 100) if total_records > 0 else 0
    
    print("\n" + "="*50)
    print("🏫 CAMPUS INFRASTRUCTURE - Quality Report")
    print("="*50)
    print(f"Total records: {total_records}")
    print(f"Valid records: {valid_records}")
    print(f"Quality score: {quality_score:.2f}%")
    print("="*50)

with DAG(
    'campus_infrastructure_pipeline',
    default_args=default_args,
    description='Data Product: Campus Infrastructure Usage',
    schedule_interval='*/30 * * * *',  # каждые 30 минут
    catchup=False,
    tags=['campus', 'facility']
) as dag:
    
    generate = PythonOperator(
        task_id='generate_facility_data',
        python_callable=generate_facility_data
    )
    
    quality = PythonOperator(
        task_id='check_quality',
        python_callable=check_quality
    )
    
    generate >> quality
