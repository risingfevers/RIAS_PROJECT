from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import random
import pandas as pd
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

def extract_students():
    """Извлечение студентов из API"""
    logger.info("📡 Extracting students from LMS API...")
    
    students = []
    for i in range(50):
        student = {
            'student_id': 1000 + i,
            'name': f"Student_{i}",
            'email': f"student_{i}@university.edu",
            'status': random.choice(['active', 'inactive', 'graduated']),
            'enrollment_date': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        }
        students.append(student)
    
    # Сохраняем во временный файл
    with open('/tmp/students.json', 'w') as f:
        json.dump(students, f)
    
    logger.info(f"✅ Extracted {len(students)} students")
    return len(students)

def extract_grades():
    """Извлечение оценок из CSV"""
    logger.info("📄 Extracting grades from CSV...")
    
    grades = []
    courses = ['CS101', 'CS102', 'MATH201', 'PHYS101', 'ENG202']
    
    for student_id in range(1000, 1050):
        for course in random.sample(courses, k=random.randint(1, 3)):
            grade = {
                'student_id': student_id,
                'course_id': course,
                'grade': round(random.uniform(2.0, 5.0), 1),
                'semester': '2026-Spring'
            }
            grades.append(grade)
    
    with open('/tmp/grades.json', 'w') as f:
        json.dump(grades, f)
    
    logger.info(f"✅ Extracted {len(grades)} grades")
    return len(grades)

def validate_students():
    """Валидация студентов"""
    with open('/tmp/students.json', 'r') as f:
        students = json.load(f)
    
    logger.info(f"🔍 Validating {len(students)} students...")
    
    df = pd.DataFrame(students)
    errors = []
    
    # Проверка дубликатов
    duplicates = df['student_id'].duplicated().sum()
    if duplicates > 0:
        errors.append(f"Duplicates: {duplicates}")
    
    # Проверка обязательных полей
    for field in ['student_id', 'name', 'email', 'status']:
        missing = df[field].isnull().sum()
        if missing > 0:
            errors.append(f"Missing {field}: {missing}")
    
    # Проверка статусов
    valid_statuses = ['active', 'inactive', 'graduated']
    invalid = ~df['status'].isin(valid_statuses)
    if invalid.sum() > 0:
        errors.append(f"Invalid status: {invalid.sum()}")
    
    if errors:
        logger.error(f"❌ Validation failed: {errors}")
        raise Exception(f"Student validation failed: {errors}")
    
    logger.info("✅ Student validation passed")
    
    # Сохраняем результаты валидации (ИСПРАВЛЕНО)
    client = Minio(**MINIO_CONFIG)
    if not client.bucket_exists('validation'):
        client.make_bucket('validation')
    
    result = {'passed': True, 'errors': errors, 'count': len(students), 'type': 'students'}
    json_data = json.dumps(result, indent=2)
    
    # Используем BytesIO вместо bytes
    client.put_object(
        'validation',
        f"students_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        data=BytesIO(json_data.encode('utf-8')),
        length=len(json_data),
        content_type='application/json'
    )
    
    return True

def validate_grades():
    """Валидация оценок"""
    with open('/tmp/grades.json', 'r') as f:
        grades = json.load(f)
    
    logger.info(f"🔍 Validating {len(grades)} grades...")
    
    df = pd.DataFrame(grades)
    errors = []
    
    # Проверка диапазона оценок
    out_of_range = ((df['grade'] < 2.0) | (df['grade'] > 5.0)).sum()
    if out_of_range > 0:
        errors.append(f"Grades out of range: {out_of_range}")
    
    # Проверка курсов
    valid_courses = ['CS101', 'CS102', 'MATH201', 'PHYS101', 'ENG202']
    invalid = ~df['course_id'].isin(valid_courses)
    if invalid.sum() > 0:
        errors.append(f"Invalid courses: {invalid.sum()}")
    
    if errors:
        logger.error(f"❌ Validation failed: {errors}")
        raise Exception(f"Grades validation failed: {errors}")
    
    logger.info("✅ Grades validation passed")
    
    # Сохраняем результаты валидации (ИСПРАВЛЕНО)
    client = Minio(**MINIO_CONFIG)
    if not client.bucket_exists('validation'):
        client.make_bucket('validation')
    
    result = {'passed': True, 'errors': errors, 'count': len(grades), 'type': 'grades'}
    json_data = json.dumps(result, indent=2)
    
    client.put_object(
        'validation',
        f"grades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        data=BytesIO(json_data.encode('utf-8')),
        length=len(json_data),
        content_type='application/json'
    )
    
    return True

def load_to_minio():
    """Загрузка данных в MinIO"""
    logger.info("💾 Loading data to MinIO...")
    
    client = Minio(**MINIO_CONFIG)
    
    # Создаём бакет если не существует
    if not client.bucket_exists('raw-layer'):
        client.make_bucket('raw-layer')
    
    # Загружаем студентов
    with open('/tmp/students.json', 'r') as f:
        students = json.load(f)
    
    df_students = pd.DataFrame(students)
    buffer = BytesIO()
    df_students.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    student_obj = f"students/{datetime.now().strftime('%Y%m%d')}/students_{datetime.now().strftime('%H%M%S')}.parquet"
    client.put_object(
        'raw-layer',
        student_obj,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type='application/parquet'
    )
    logger.info(f"✅ Loaded students to {student_obj}")
    
    # Загружаем оценки
    with open('/tmp/grades.json', 'r') as f:
        grades = json.load(f)
    
    df_grades = pd.DataFrame(grades)
    buffer = BytesIO()
    df_grades.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    grade_obj = f"grades/{datetime.now().strftime('%Y%m%d')}/grades_{datetime.now().strftime('%H%M%S')}.parquet"
    client.put_object(
        'raw-layer',
        grade_obj,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type='application/parquet'
    )
    logger.info(f"✅ Loaded grades to {grade_obj}")
    
    # Создаём lineage отчёт
    lineage = {
        'timestamp': datetime.now().isoformat(),
        'students_count': len(students),
        'grades_count': len(grades),
        'validation_passed': True,
        'sources': ['lms_api', 'csv_export'],
        'destination': 'raw-layer'
    }
    
    if not client.bucket_exists('lineage'):
        client.make_bucket('lineage')
    
    lineage_data = json.dumps(lineage, indent=2)
    client.put_object(
        'lineage',
        f"lineage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        data=BytesIO(lineage_data.encode('utf-8')),
        length=len(lineage_data),
        content_type='application/json'
    )
    
    logger.info("✅ Lineage report created")
    logger.info(f"📊 Final stats: {len(students)} students, {len(grades)} grades")
    
    return True

# DAG definition
default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'etl_final_working',
    default_args=default_args,
    description='Working ETL with validation and observability',
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=['etl', 'working', 'final']
)

# Tasks
extract_students_task = PythonOperator(
    task_id='extract_students',
    python_callable=extract_students,
    dag=dag
)

extract_grades_task = PythonOperator(
    task_id='extract_grades',
    python_callable=extract_grades,
    dag=dag
)

validate_students_task = PythonOperator(
    task_id='validate_students',
    python_callable=validate_students,
    dag=dag
)

validate_grades_task = PythonOperator(
    task_id='validate_grades',
    python_callable=validate_grades,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_minio',
    python_callable=load_to_minio,
    dag=dag
)

# Dependencies
[extract_students_task >> validate_students_task,
 extract_grades_task >> validate_grades_task] >> load_task
