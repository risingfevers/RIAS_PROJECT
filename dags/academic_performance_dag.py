from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import random
import uuid
import os

default_args = {
    'owner': 'academic_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def generate_grades():
    """Генерирует данные об успеваемости студентов"""
    
    os.makedirs('/opt/airflow/data/academic_performance', exist_ok=True)
    
    courses = ['CS101', 'CS102', 'MATH201', 'PHYS101', 'ENG202']
    students = []
    
    for user_id in range(1001, 1051):  # 50 студентов
        grade = {
            'record_id': str(uuid.uuid4()),
            'user_id': user_id,
            'course_id': random.choice(courses),
            'semester': '2026-Spring',
            'grade': round(random.uniform(2.0, 5.0), 1),
            'attendance_rate': round(random.uniform(0.5, 1.0), 2),
            'assignments_completed': random.randint(5, 10),
            'assignments_total': 10,
            'timestamp': datetime.now().isoformat(),
            'domain': 'academic_performance'
        }
        students.append(grade)
    
    filename = f"/opt/airflow/data/academic_performance/grades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(students, f, indent=2)
    
    print(f"✅ Сгенерировано оценок: {len(students)}")
    print(f"📁 Сохранено: {filename}")
    
    # Статистика
    passing = len([s for s in students if s['grade'] >= 3.0])
    print(f"📊 Успеваемость: {passing}/{len(students)} ({passing/len(students)*100:.1f}%)")
    
    return filename

def check_quality():
    """Проверка качества данных об успеваемости"""
    
    data_dir = '/opt/airflow/data/academic_performance'
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
                required_fields = ['record_id', 'user_id', 'course_id', 'grade', 'domain']
                if all(field in record for field in required_fields):
                    valid_records += 1
    
    quality_score = (valid_records / total_records * 100) if total_records > 0 else 0
    
    print("\n" + "="*50)
    print("📊 ACADEMIC PERFORMANCE - Quality Report")
    print("="*50)
    print(f"Total records: {total_records}")
    print(f"Valid records: {valid_records}")
    print(f"Quality score: {quality_score:.2f}%")
    print("="*50)

with DAG(
    'academic_performance_pipeline',
    default_args=default_args,
    description='Data Product: Academic Performance',
    schedule_interval='@daily',
    catchup=False,
    tags=['academic', 'grades']
) as dag:
    
    generate = PythonOperator(
        task_id='generate_grades',
        python_callable=generate_grades
    )
    
    quality = PythonOperator(
        task_id='check_quality',
        python_callable=check_quality
    )
    
    generate >> quality
