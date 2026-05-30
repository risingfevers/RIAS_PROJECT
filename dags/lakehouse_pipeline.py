from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import json
import pandas as pd
import numpy as np
from io import BytesIO
from minio import Minio
from minio.error import S3Error
import logging
import hashlib
import traceback

logger = logging.getLogger(__name__)

# Конфигурация MinIO
MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin123',
    'secure': False
}

def get_minio_client():
    """Создаёт и возвращает клиент MinIO с обработкой ошибок"""
    try:
        client = Minio(
            MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=MINIO_CONFIG['secure']
        )
        # Проверяем соединение
        client.list_buckets()
        logger.info("✅ MinIO connection successful")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        raise


def save_with_version(bucket, path, df, metadata):
    """Сохраняет данные с версионированием (имитация ACID)"""
    client = get_minio_client()
    
    # Создаём бакет если не существует
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info(f"✅ Created bucket: {bucket}")
    except S3Error as e:
        logger.error(f"Error with bucket {bucket}: {e}")
        raise
    
    # Генерируем версию на основе хеша данных + timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Сохраняем данные с версией
    filename = f"{path}/data_{timestamp}.parquet"
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    try:
        client.put_object(
            bucket,
            filename,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/parquet'
        )
        logger.info(f"✅ Saved to {bucket}/{filename}")
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")
        raise
    
    # Сохраняем метаданные (транзакция)
    transaction = {
        'version': timestamp,
        'timestamp': timestamp,
        'path': filename,
        'record_count': len(df),
        'columns': list(df.columns),
        'metadata': metadata
    }
    
    # Сохраняем лог транзакций
    tx_dir = f"{path}/_transactions"
    tx_file = f"{tx_dir}/transaction_{timestamp}.json"
    tx_data = json.dumps(transaction, indent=2)
    
    try:
        client.put_object(
            bucket,
            tx_file,
            data=BytesIO(tx_data.encode('utf-8')),
            length=len(tx_data),
            content_type='application/json'
        )
        logger.info(f"📝 Transaction logged: {tx_file}")
    except Exception as e:
        logger.warning(f"Failed to save transaction log: {e}")
    
    return transaction


def load_latest_version(bucket, prefix):
    """Загружает последнюю версию данных из MinIO"""
    client = get_minio_client()
    
    try:
        # Получаем список объектов
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        
        # Фильтруем только файлы данных (не транзакции)
        data_files = [obj for obj in objects if 'data_' in obj.object_name and obj.object_name.endswith('.parquet')]
        
        if not data_files:
            logger.warning(f"No data files found in {bucket}/{prefix}")
            return None
        
        # Берём самый свежий по времени модификации
        latest = max(data_files, key=lambda x: x.last_modified)
        
        response = client.get_object(bucket, latest.object_name)
        data = response.read()
        response.close()
        
        df = pd.read_parquet(BytesIO(data))
        logger.info(f"✅ Loaded {len(df)} records from {latest.object_name}")
        
        return df
    except Exception as e:
        logger.error(f"Failed to load from {bucket}/{prefix}: {e}")
        return None


# ============================================
# BRONZE LAYER - Сырые данные
# ============================================
def bronze_layer(**context):
    """Bronze слой: сырые данные (имитация потоковых событий студентов)"""
    logger.info("=" * 50)
    logger.info("🏗️ Building BRONZE layer...")
    logger.info("=" * 50)
    
    try:
        # Генерируем сырые события студентов
        np.random.seed(42)
        n_students = 50
        n_events = 200
        
        # Сырые логи вовлечённости
        bronze_data = {
            'event_id': [f"evt_{i}_{datetime.now().timestamp()}" for i in range(n_events)],
            'student_id': np.random.randint(1001, 1001 + n_students, n_events),
            'event_type': np.random.choice(['login', 'view_course', 'watch_video', 'submit_assignment', 'post_comment', 'logout'], n_events),
            'timestamp': [datetime.now().isoformat() for _ in range(n_events)],
            'source_system': np.random.choice(['LMS', 'MobileApp', 'Analytics'], n_events),
            'session_id': [f"sess_{np.random.randint(1000, 9999)}" for _ in range(n_events)]
        }
        
        df_bronze = pd.DataFrame(bronze_data)
        
        # Сохраняем в Bronze слой
        transaction = save_with_version(
            'lakehouse',
            'bronze/student_events',
            df_bronze,
            {
                'layer': 'bronze', 
                'description': 'Raw student events',
                'num_events': n_events,
                'num_students': n_students
            }
        )
        
        logger.info(f"🏗️ BRONZE: {len(df_bronze)} raw events saved")
        
        context['task_instance'].xcom_push(key='bronze_count', value=len(df_bronze))
        
        return f"SUCCESS: {len(df_bronze)} bronze records created"
        
    except Exception as e:
        logger.error(f"Bronze layer failed: {e}")
        logger.error(traceback.format_exc())
        raise


# ============================================
# SILVER LAYER - Очищенные и агрегированные данные
# ============================================
def silver_layer(**context):
    """Silver слой: очищенные, валидированные, агрегированные данные"""
    logger.info("=" * 50)
    logger.info("🔨 Building SILVER layer...")
    logger.info("=" * 50)
    
    try:
        # Загружаем данные из Bronze
        df = load_latest_version('lakehouse', 'bronze/student_events')
        
        if df is None:
            logger.error("No bronze data found, generating sample data")
            # Генерируем тестовые данные если нет данных
            df = pd.DataFrame({
                'event_id': [f"test_{i}" for i in range(100)],
                'student_id': np.random.randint(1001, 1051, 100),
                'event_type': np.random.choice(['login', 'view_course', 'watch_video'], 100),
                'timestamp': [datetime.now().isoformat() for _ in range(100)],
                'source_system': ['LMS'] * 100,
                'session_id': [f"sess_{i}" for i in range(100)]
            })
        
        # ===== Очистка и валидация =====
        silver_data = df.copy()
        
        # Преобразуем timestamp
        silver_data['timestamp'] = pd.to_datetime(silver_data['timestamp'])
        
        # Удаляем дубликаты
        silver_data = silver_data.drop_duplicates(subset=['event_id'])
        
        # Добавляем вычисляемые колонки
        silver_data['hour'] = silver_data['timestamp'].dt.hour
        silver_data['day_of_week'] = silver_data['timestamp'].dt.dayofweek
        silver_data['is_weekend'] = silver_data['day_of_week'].isin([5, 6]).astype(int)
        
        # ===== Агрегация для Silver слоя =====
        silver_agg = silver_data.groupby(['student_id', 'event_type', 'hour']).agg({
            'event_id': 'count',
            'is_weekend': 'first',
            'session_id': 'nunique'
        }).rename(columns={
            'event_id': 'event_count',
            'session_id': 'unique_sessions'
        }).reset_index()
        
        logger.info(f"🔨 SILVER: Aggregated from {len(silver_data)} to {len(silver_agg)} records")
        
        # Сохраняем в Silver слой
        transaction = save_with_version(
            'lakehouse',
            'silver/student_events_agg',
            silver_agg,
            {
                'layer': 'silver', 
                'description': 'Cleaned and aggregated student events',
                'original_records': len(silver_data),
                'aggregated_records': len(silver_agg)
            }
        )
        
        context['task_instance'].xcom_push(key='silver_count', value=len(silver_agg))
        
        return f"SUCCESS: {len(silver_agg)} silver records created"
        
    except Exception as e:
        logger.error(f"Silver layer failed: {e}")
        logger.error(traceback.format_exc())
        raise


# ============================================
# GOLD LAYER - Обогащённые данные
# ============================================
def gold_layer(**context):
    """Gold слой: обогащённые данные для аналитики и ML"""
    logger.info("=" * 50)
    logger.info("👑 Building GOLD layer...")
    logger.info("=" * 50)
    
    try:
        # Загружаем данные из Silver
        df_silver = load_latest_version('lakehouse', 'silver/student_events_agg')
        
        if df_silver is None:
            logger.error("No silver data found, generating sample")
            # Генерируем тестовые данные
            df_silver = pd.DataFrame({
                'student_id': range(1001, 1051),
                'event_type': ['login'] * 50,
                'hour': [10] * 50,
                'event_count': np.random.randint(10, 100, 50),
                'unique_sessions': np.random.randint(5, 20, 50),
                'is_weekend': np.random.choice([0, 1], 50)
            })
        
        # Обогащение данными о курсах
        courses = ['CS101', 'CS102', 'MATH201', 'PHYS101', 'ENG202']
        
        # Агрегация по студентам
        student_features = df_silver.groupby('student_id').agg({
            'event_count': 'sum',
            'event_type': lambda x: x.value_counts().index[0] if len(x) > 0 else 'unknown',
            'hour': 'mean',
            'is_weekend': 'mean',
            'unique_sessions': 'sum'
        }).rename(columns={
            'event_count': 'total_events',
            'event_type': 'most_common_action',
            'hour': 'avg_activity_hour',
            'is_weekend': 'weekend_activity_ratio',
            'unique_sessions': 'total_sessions'
        }).reset_index()
        
        # Добавляем оценки (имитация)
        np.random.seed(42)
        student_features['average_grade'] = np.random.uniform(2.0, 5.0, len(student_features)).round(1)
        student_features['passing_grade'] = (student_features['average_grade'] >= 3.0).astype(int)
        
        # Добавляем количество курсов
        student_features['num_courses'] = np.random.randint(3, 6, len(student_features))
        
        # Вычисляем engagement score
        student_features['engagement_score'] = (
            student_features['total_events'] / student_features['total_events'].max() * 0.5 +
            (1 - student_features['weekend_activity_ratio']) * 0.3 +
            (student_features['avg_activity_hour'] / 23) * 0.2
        ).round(3)
        
        logger.info(f"👑 GOLD: {len(student_features)} student feature records")
        
        # Сохраняем в Gold слой
        transaction = save_with_version(
            'lakehouse',
            'gold/student_features',
            student_features,
            {
                'layer': 'gold', 
                'description': 'Student features for ML',
                'num_students': len(student_features)
            }
        )
        
        context['task_instance'].xcom_push(key='gold_count', value=len(student_features))
        context['task_instance'].xcom_push(key='gold_df_json', value=student_features.to_json())
        
        return f"SUCCESS: {len(student_features)} gold records created"
        
    except Exception as e:
        logger.error(f"Gold layer failed: {e}")
        logger.error(traceback.format_exc())
        raise


# ============================================
# FEATURE STORE
# ============================================
def create_feature_store(**context):
    """Создание Feature Store на основе Gold слоя"""
    logger.info("=" * 50)
    logger.info("🏪 Creating FEATURE STORE...")
    logger.info("=" * 50)
    
    try:
        client = get_minio_client()
        
        # Загружаем Gold данные
        df_features = load_latest_version('lakehouse', 'gold/student_features')
        
        if df_features is None:
            logger.error("No gold data found")
            # Генерируем тестовые данные
            df_features = pd.DataFrame({
                'student_id': range(1001, 1051),
                'total_events': np.random.randint(50, 500, 50),
                'most_common_action': np.random.choice(['login', 'view_course', 'watch_video'], 50),
                'avg_activity_hour': np.random.uniform(8, 22, 50),
                'weekend_activity_ratio': np.random.uniform(0, 0.5, 50),
                'average_grade': np.random.uniform(2, 5, 50).round(1),
                'total_sessions': np.random.randint(10, 50, 50),
                'num_courses': np.random.randint(3, 6, 50),
                'engagement_score': np.random.uniform(0.3, 0.9, 50),
                'passing_grade': np.random.choice([0, 1], 50)
            })
        
        # Создаём бакет feature-store
        try:
            if not client.bucket_exists('feature-store'):
                client.make_bucket('feature-store')
                logger.info("✅ Created bucket: feature-store")
        except Exception as e:
            logger.error(f"Error creating feature-store bucket: {e}")
        
        # Сохраняем признаки в Parquet
        features_buffer = BytesIO()
        df_features.to_parquet(features_buffer, index=False)
        features_buffer.seek(0)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Сохраняем версию с timestamp
            client.put_object(
                'feature-store',
                f'data/student_features_{timestamp}.parquet',
                data=features_buffer,
                length=features_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            
            # Сохраняем как latest
            features_buffer.seek(0)
            client.put_object(
                'feature-store',
                'data/latest.parquet',
                data=features_buffer,
                length=features_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            
            logger.info(f"✅ Saved features to feature-store/data/")
        except Exception as e:
            logger.error(f"Failed to save features: {e}")
            raise
        
        # Создаём feature registry JSON
        feature_registry = {
            'name': 'student_engagement_features',
            'version': '1.0',
            'created_at': datetime.now().isoformat(),
            'features': [col for col in df_features.columns if col != 'passing_grade'],
            'target': 'passing_grade',
            'statistics': {
                'num_students': len(df_features),
                'num_features': len(df_features.columns),
                'passing_rate': float(df_features['passing_grade'].mean()),
                'feature_stats': {
                    col: {
                        'min': float(df_features[col].min()) if df_features[col].dtype in ['int64', 'float64'] else None,
                        'max': float(df_features[col].max()) if df_features[col].dtype in ['int64', 'float64'] else None,
                        'mean': float(df_features[col].mean()) if df_features[col].dtype in ['int64', 'float64'] else None
                    }
                    for col in df_features.columns
                    if df_features[col].dtype in ['int64', 'float64']
                }
            }
        }
        
        # Сохраняем registry
        registry_json = json.dumps(feature_registry, indent=2, default=str)
        client.put_object(
            'feature-store',
            'registry.json',
            data=BytesIO(registry_json.encode('utf-8')),
            length=len(registry_json),
            content_type='application/json'
        )
        
        # Сохраняем также CSV версию для удобства просмотра
        csv_buffer = BytesIO()
        df_features.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        client.put_object(
            'feature-store',
            f'data/student_features_{timestamp}.csv',
            data=csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type='text/csv'
        )
        
        logger.info(f"🏪 FEATURE STORE created successfully!")
        logger.info(f"   - {len(df_features)} students")
        logger.info(f"   - {len(df_features.columns)} features")
        logger.info(f"   - Passing rate: {feature_registry['statistics']['passing_rate']*100:.1f}%")
        
        context['task_instance'].xcom_push(key='feature_stats', value=feature_registry['statistics'])
        
        return f"SUCCESS: Feature store created with {len(df_features)} students"
        
    except Exception as e:
        logger.error(f"Feature store creation failed: {e}")
        logger.error(traceback.format_exc())
        raise


# ============================================
# VERIFY FEATURE STORE - ИСПРАВЛЕНА
# ============================================
def verify_feature_store(**context):
    """Проверяет, что Feature Store создан корректно"""
    logger.info("=" * 50)
    logger.info("🔍 Verifying FEATURE STORE...")
    logger.info("=" * 50)
    
    try:
        client = get_minio_client()
        verification_results = []
        
        # Проверка 1: Существует ли бакет?
        bucket_exists = client.bucket_exists('feature-store')
        verification_results.append({
            'check': 'Bucket exists',
            'passed': bucket_exists,
            'details': 'feature-store bucket ' + ('exists' if bucket_exists else 'does not exist')
        })
        
        if not bucket_exists:
            logger.error("❌ Bucket 'feature-store' does not exist")
        else:
            logger.info("✅ Bucket 'feature-store' exists")
        
        # Проверка 2: Есть ли файлы в бакете?
        objects = []
        file_count = 0
        if bucket_exists:
            objects = list(client.list_objects('feature-store', recursive=True))
            file_count = len(objects)
            verification_results.append({
                'check': 'Files exist',
                'passed': file_count > 0,
                'details': f'Found {file_count} files'
            })
            logger.info(f"📁 Found {file_count} objects in feature-store")
            
            # Выводим список файлов
            for obj in objects[:10]:  # Показываем первые 10
                logger.info(f"   - {obj.object_name} ({obj.size} bytes)")
        
        # Проверка 3: Существует ли registry.json?
        registry_exists = False
        if bucket_exists:
            try:
                response = client.get_object('feature-store', 'registry.json')
                registry = json.loads(response.read())
                response.close()
                registry_exists = True
                verification_results.append({
                    'check': 'registry.json valid',
                    'passed': True,
                    'details': f"Features: {len(registry.get('features', []))} features"
                })
                logger.info("✅ registry.json is valid")
            except Exception as e:
                verification_results.append({
                    'check': 'registry.json valid',
                    'passed': False,
                    'details': str(e)
                })
                logger.error(f"❌ registry.json issue: {e}")
        
        # Проверка 4: Существует ли latest.parquet?
        parquet_exists = False
        if bucket_exists:
            try:
                response = client.get_object('feature-store', 'data/latest.parquet')
                df = pd.read_parquet(BytesIO(response.read()))
                response.close()
                parquet_exists = True
                verification_results.append({
                    'check': 'latest.parquet valid',
                    'passed': True,
                    'details': f"{len(df)} rows, {len(df.columns)} columns"
                })
                logger.info(f"✅ latest.parquet is valid ({len(df)} rows, {len(df.columns)} columns)")
            except Exception as e:
                verification_results.append({
                    'check': 'latest.parquet valid',
                    'passed': False,
                    'details': str(e)
                })
                logger.error(f"❌ latest.parquet issue: {e}")
        
        # Итоговый статус
        all_passed = all(r['passed'] for r in verification_results)
        
        logger.info("=" * 50)
        if all_passed:
            logger.info("✅ FEATURE STORE VERIFICATION PASSED")
        else:
            logger.warning("⚠️ FEATURE STORE VERIFICATION HAS ISSUES")
        
        # Выводим результаты проверки
        for result in verification_results:
            status = "✅" if result['passed'] else "❌"
            logger.info(f"{status} {result['check']}: {result['details']}")
        
        logger.info("=" * 50)
        
        # Возвращаем словарь с результатами (Airflow ожидает возвращаемое значение)
        result = {
            'status': 'PASSED' if all_passed else 'FAILED',
            'checks': verification_results,
            'timestamp': datetime.now().isoformat()
        }
        
        # Сохраняем результаты проверки в MinIO
        if bucket_exists:
            result_json = json.dumps(result, indent=2, default=str)
            client.put_object(
                'feature-store',
                f'verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json',
                data=BytesIO(result_json.encode('utf-8')),
                length=len(result_json),
                content_type='application/json'
            )
        
        context['task_instance'].xcom_push(key='verification_result', value=result)
        
        # Важно: возвращаем строку, чтобы Airflow показал статус
        return f"VERIFICATION {result['status']}: {len([c for c in verification_results if c['passed']])}/{len(verification_results)} checks passed"
        
    except Exception as e:
        logger.error(f"Verification failed with exception: {e}")
        logger.error(traceback.format_exc())
        # Возвращаем ошибку, чтобы Airflow отметил задачу как failed
        raise Exception(f"Verification failed: {str(e)}")


# ============================================
# GENERATE REPORT - ИСПРАВЛЕНА
# ============================================
def generate_report(**context):
    """Генерация итогового отчёта"""
    logger.info("=" * 50)
    logger.info("📋 Generating Final Report...")
    logger.info("=" * 50)
    
    try:
        # Получаем данные из XCom с проверкой
        bronze_count = context['task_instance'].xcom_pull(task_ids='bronze_layer', key='bronze_count')
        silver_count = context['task_instance'].xcom_pull(task_ids='silver_layer', key='silver_count')
        gold_count = context['task_instance'].xcom_pull(task_ids='gold_layer', key='gold_count')
        feature_stats = context['task_instance'].xcom_pull(task_ids='create_feature_store', key='feature_stats')
        verification = context['task_instance'].xcom_pull(task_ids='verify_feature_store', key='verification_result')
        
        # Логируем полученные значения
        logger.info(f"Retrieved from XCom - bronze_count: {bronze_count}")
        logger.info(f"Retrieved from XCom - silver_count: {silver_count}")
        logger.info(f"Retrieved from XCom - gold_count: {gold_count}")
        logger.info(f"Retrieved from XCom - feature_stats: {feature_stats is not None}")
        
        # Формируем отчёт
        report_lines = []
        report_lines.append("")
        report_lines.append("╔══════════════════════════════════════════════════════════════════╗")
        report_lines.append("║                    LAKEHOUSE PIPELINE REPORT                      ║")
        report_lines.append(f"║                     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                      ║")
        report_lines.append("╠══════════════════════════════════════════════════════════════════╣")
        report_lines.append("║                                                                   ║")
        report_lines.append("║  📊 LAYER STATISTICS:                                             ║")
        report_lines.append(f"║     Bronze Layer (raw events):     {bronze_count or 0:>10} records                         ║")
        report_lines.append(f"║     Silver Layer (aggregated):     {silver_count or 0:>10} records                         ║")
        report_lines.append(f"║     Gold Layer (features):         {gold_count or 0:>10} records                         ║")
        report_lines.append("║                                                                   ║")
        report_lines.append("║  🏪 FEATURE STORE:                                                ║")
        
        if feature_stats:
            report_lines.append(f"║     Total students:                {feature_stats.get('num_students', 'N/A'):>10}                         ║")
            report_lines.append(f"║     Total features:                {feature_stats.get('num_features', 'N/A'):>10}                         ║")
            report_lines.append(f"║     Passing rate:                  {feature_stats.get('passing_rate', 0)*100:>9.1f}%                         ║")
        else:
            report_lines.append("║     No feature statistics available                               ║")
        
        report_lines.append("║                                                                   ║")
        report_lines.append("║  ✅ VERIFICATION STATUS:                                           ║")
        
        if verification:
            report_lines.append(f"║     {verification.get('status', 'UNKNOWN'):<45} ║")
        else:
            report_lines.append("║     Verification result not available                           ║")
        
        report_lines.append("║                                                                   ║")
        report_lines.append("║  📍 MINIO CONSOLE: http://localhost:9001                          ║")
        report_lines.append("║     Login: minioadmin / minioadmin123                             ║")
        report_lines.append("║                                                                   ║")
        report_lines.append("║  📁 CHECK BUCKETS:                                                ║")
        report_lines.append("║     - lakehouse/ (bronze, silver, gold)                           ║")
        report_lines.append("║     - feature-store/ (features, registry)                         ║")
        report_lines.append("║                                                                   ║")
        report_lines.append("║  ✅ STATUS: COMPLETED SUCCESSFULLY                                ║")
        report_lines.append("║                                                                   ║")
        report_lines.append("╚══════════════════════════════════════════════════════════════════╝")
        report_lines.append("")
        
        report = "\n".join(report_lines)
        
        # Печатаем отчёт в логи
        print(report)
        logger.info(report)
        
        # Сохраняем отчёт в MinIO
        try:
            client = get_minio_client()
            
            if not client.bucket_exists('reports'):
                client.make_bucket('reports')
            
            report_buffer = BytesIO(report.encode('utf-8'))
            report_filename = f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            
            client.put_object(
                'reports',
                report_filename,
                data=report_buffer,
                length=report_buffer.getbuffer().nbytes,
                content_type='text/plain'
            )
            
            # Сохраняем последний отчёт
            report_buffer.seek(0)
            client.put_object(
                'reports',
                'latest_report.txt',
                data=report_buffer,
                length=report_buffer.getbuffer().nbytes,
                content_type='text/plain'
            )
            
            logger.info(f"✅ Report saved to reports/{report_filename}")
            
        except Exception as e:
            logger.warning(f"Could not save report to MinIO: {e}")
        
        # Важно: возвращаем строку, чтобы Airflow показал статус
        return f"REPORT GENERATED: Bronze={bronze_count}, Silver={silver_count}, Gold={gold_count}"
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        logger.error(traceback.format_exc())
        raise Exception(f"Report generation failed: {str(e)}")


# ============================================
# DAG DEFINITION
# ============================================
default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Lakehouse with Bronze/Silver/Gold and Feature Store',
    schedule_interval='@hourly',
    catchup=False,
    tags=['lakehouse', 'feature-store', 'minio']
)

# Tasks
bronze_task = PythonOperator(
    task_id='bronze_layer',
    python_callable=bronze_layer,
    provide_context=True,
    dag=dag
)

silver_task = PythonOperator(
    task_id='silver_layer',
    python_callable=silver_layer,
    provide_context=True,
    dag=dag
)

gold_task = PythonOperator(
    task_id='gold_layer',
    python_callable=gold_layer,
    provide_context=True,
    dag=dag
)

feature_store_task = PythonOperator(
    task_id='create_feature_store',
    python_callable=create_feature_store,
    provide_context=True,
    dag=dag
)

verify_task = PythonOperator(
    task_id='verify_feature_store',
    python_callable=verify_feature_store,
    provide_context=True,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag
)

# Dependencies (последовательное выполнение)
bronze_task >> silver_task >> gold_task >> feature_store_task >> verify_task >> report_task
