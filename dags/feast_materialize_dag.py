from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from io import BytesIO
from minio import Minio

default_args = {
    'owner': 'ml_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin123',
    'secure': False
}

def materialize_features_to_online_store(**context):
    """Материализует признаки для low-latency доступа"""
    client = Minio(**MINIO_CONFIG)
    
    # Загружаем последние признаки
    objects = list(client.list_objects('feature-store', prefix='feature-store/data/', recursive=True))
    if not objects:
        raise Exception("No features found")
    
    latest = max(objects, key=lambda x: x.last_modified)
    response = client.get_object('feature-store', latest.object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    
    # Для каждого студента сохраняем его признаки в формате ключ-значение
    online_store = {}
    for _, row in df.iterrows():
        student_id = int(row['student_id'])
        online_store[f"student_{student_id}"] = {
            'total_events': int(row['total_events']),
            'most_common_action': row['most_common_action'],
            'avg_activity_hour': float(row['avg_activity_hour']),
            'weekend_activity_ratio': float(row['weekend_activity_ratio']),
            'average_grade': float(row['average_grade']),
            'passing_grade': int(row['passing_grade']),
            'materialized_at': datetime.now().isoformat()
        }
    
    # Сохраняем materialized view в MinIO как имитацию online store
    materialized_data = json.dumps(online_store, indent=2, default=str)
    client.put_object(
        'feature-store',
        f'online_store/materialized_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json',
        data=BytesIO(materialized_data.encode('utf-8')),
        length=len(materialized_data),
        content_type='application/json'
    )
    
    # Сохраняем последнюю версию
    client.put_object(
        'feature-store',
        'online_store/latest.json',
        data=BytesIO(materialized_data.encode('utf-8')),
        length=len(materialized_data),
        content_type='application/json'
    )
    
    print(f"✅ Materialized {len(online_store)} student features to online store")
    return len(online_store)

def create_training_dataset(**context):
    """Создаёт готовый датасет для обучения ML модели"""
    client = Minio(**MINIO_CONFIG)
    
    # Загружаем признаки
    objects = list(client.list_objects('feature-store', prefix='feature-store/data/', recursive=True))
    latest = max(objects, key=lambda x: x.last_modified)
    response = client.get_object('feature-store', latest.object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    
    # Загружаем целевые переменные из Gold слоя
    gold_objects = list(client.list_objects('lakehouse', prefix='gold/student_features/version_', recursive=True))
    latest_gold = max(gold_objects, key=lambda x: x.last_modified)
    response = client.get_object('lakehouse', latest_gold.object_name)
    df_gold = pd.read_parquet(BytesIO(response.read()))
    response.close()
    
    # Объединяем
    final_dataset = df.merge(df_gold[['student_id', 'passing_grade']], on='student_id', how='left')
    
    # Сохраняем как CSV для ML
    csv_buffer = BytesIO()
    final_dataset.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    client.put_object(
        'feature-store',
        f'training/dataset_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
        data=csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type='text/csv'
    )
    
    print(f"✅ Training dataset created: {len(final_dataset)} rows, {len(final_dataset.columns)} columns")
    print(f"📊 Features: {list(final_dataset.columns)}")

dag = DAG(
    'feast_materialization_pipeline',
    default_args=default_args,
    description='Feature Store: Materialization and Training Data',
    schedule_interval='@hourly',
    catchup=False,
    tags=['feast', 'feature-store', 'ml']
)

materialize_task = PythonOperator(
    task_id='materialize_features',
    python_callable=materialize_features_to_online_store,
    provide_context=True,
    dag=dag
)

training_task = PythonOperator(
    task_id='create_training_dataset',
    python_callable=create_training_dataset,
    provide_context=True,
    dag=dag
)

materialize_task >> training_task
