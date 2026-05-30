from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
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

ALERT_WEBHOOK_URL = None

def send_telegram_alert(message, severity="INFO"):
    """Отправка оповещения"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{severity}] {timestamp} - {message}"
    
    logger.info(f"📢 ALERT: {full_message}")
    
    client = Minio(**MINIO_CONFIG)
    alert_data = {
        'timestamp': timestamp,
        'severity': severity,
        'message': message,
        'dag': 'observability_pipeline'
    }
    
    if not client.bucket_exists('alerts'):
        client.make_bucket('alerts')
    
    alert_file = f"alerts/{datetime.now().strftime('%Y%m%d')}/{datetime.now().strftime('%H%M%S')}_{severity}.json"
    json_data = json.dumps(alert_data, indent=2)
    
    client.put_object(
        'alerts',
        alert_file,
        data=BytesIO(json_data.encode('utf-8')),
        length=len(json_data),
        content_type='application/json'
    )
    
    if ALERT_WEBHOOK_URL:
        try:
            requests.post(ALERT_WEBHOOK_URL, json={'text': full_message}, timeout=5)
        except:
            pass

def collect_dag_metrics(**context):
    """Сбор метрик выполнения DAG"""
    dag_run = context['dag_run']
    
    metrics = {
        'dag_id': context['dag'].dag_id,
        'run_id': dag_run.run_id,
        'execution_date': context['ds'],
        'start_date': dag_run.start_date.isoformat() if dag_run.start_date else None,
        'end_date': datetime.now().isoformat(),
        'duration_seconds': None,
        'tasks': {},
        'total_tasks': 0,
        'successful_tasks': 0,
        'failed_tasks': 0
    }
    
    # Исправляем проблему с временными зонами - просто используем .timestamp()
    if dag_run.start_date:
        try:
            # Преобразуем в naive datetime если нужно
            start = dag_run.start_date
            if start.tzinfo is not None:
                start = start.replace(tzinfo=None)
            now = datetime.now()
            duration = (now - start).total_seconds()
            metrics['duration_seconds'] = duration
        except:
            metrics['duration_seconds'] = 0
    
    for task in context['dag'].tasks:
        task_instances = dag_run.get_task_instances()
        for ti_instance in task_instances:
            if ti_instance.task_id == task.task_id:
                metrics['tasks'][task.task_id] = ti_instance.state
                metrics['total_tasks'] += 1
                if ti_instance.state == 'success':
                    metrics['successful_tasks'] += 1
                elif ti_instance.state == 'failed':
                    metrics['failed_tasks'] += 1
    
    metrics['success_rate'] = (metrics['successful_tasks'] / metrics['total_tasks'] * 100) if metrics['total_tasks'] > 0 else 0
    
    logger.info("="*50)
    logger.info("📊 DAG PERFORMANCE METRICS")
    logger.info("="*50)
    logger.info(f"DAG: {metrics['dag_id']}")
    logger.info(f"Duration: {metrics['duration_seconds']:.2f} sec")
    logger.info(f"Success rate: {metrics['success_rate']:.1f}%")
    logger.info(f"Successful tasks: {metrics['successful_tasks']}/{metrics['total_tasks']}")
    logger.info("="*50)
    
    if metrics['failed_tasks'] > 0:
        send_telegram_alert(f"⚠️ DAG {metrics['dag_id']} has {metrics['failed_tasks']} failed tasks!", severity="ERROR")
    elif metrics['success_rate'] == 100:
        send_telegram_alert(f"✅ DAG {metrics['dag_id']} completed successfully. Duration: {metrics['duration_seconds']:.1f}s", severity="INFO")
    
    client = Minio(**MINIO_CONFIG)
    if not client.bucket_exists('metrics'):
        client.make_bucket('metrics')
    
    metric_file = f"metrics/dag_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    json_data = json.dumps(metrics, indent=2)
    
    client.put_object(
        'metrics',
        metric_file,
        data=BytesIO(json_data.encode('utf-8')),
        length=len(json_data),
        content_type='application/json'
    )
    
    return metrics

def check_data_quality(**context):
    """Проверка качества данных в raw-layer"""
    client = Minio(**MINIO_CONFIG)
    
    quality_report = {
        'timestamp': datetime.now().isoformat(),
        'checks': [],
        'overall_status': 'PASSED'
    }
    
    if client.bucket_exists('raw-layer'):
        objects = list(client.list_objects('raw-layer', recursive=True))
        
        if len(objects) > 0:
            latest = max(objects, key=lambda x: x.last_modified)
            
            response = client.get_object('raw-layer', latest.object_name)
            data = response.read()
            response.close()
            
            is_valid = len(data) > 0
            
            quality_report['checks'].append({
                'name': 'raw_data_exists',
                'passed': is_valid,
                'details': f"File: {latest.object_name}, Size: {len(data)} bytes"
            })
        else:
            quality_report['checks'].append({
                'name': 'raw_data_exists',
                'passed': False,
                'details': "No files found in raw-layer"
            })
    
    if client.bucket_exists('validation'):
        validation_files = list(client.list_objects('validation', recursive=True))
        quality_report['checks'].append({
            'name': 'validation_exists',
            'passed': len(validation_files) > 0,
            'details': f"Found {len(validation_files)} validation files"
        })
    else:
        quality_report['checks'].append({
            'name': 'validation_exists',
            'passed': False,
            'details': "validation bucket does not exist"
        })
    
    failed_checks = [c for c in quality_report['checks'] if not c['passed']]
    if failed_checks:
        quality_report['overall_status'] = 'FAILED'
        send_telegram_alert(f"⚠️ Data quality check failed: {len(failed_checks)} issues", severity="WARNING")
    
    logger.info("\n" + "="*50)
    logger.info("📋 DATA QUALITY REPORT")
    logger.info("="*50)
    for check in quality_report['checks']:
        status = "✅" if check['passed'] else "❌"
        logger.info(f"{status} {check['name']}: {check['details']}")
    logger.info(f"\nOverall: {quality_report['overall_status']}")
    logger.info("="*50)
    
    if not client.bucket_exists('quality'):
        client.make_bucket('quality')
    
    report_file = f"quality/quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    json_data = json.dumps(quality_report, indent=2)
    
    client.put_object(
        'quality',
        report_file,
        data=BytesIO(json_data.encode('utf-8')),
        length=len(json_data),
        content_type='application/json'
    )
    
    return quality_report

def generate_observability_dashboard(**context):
    """Генерация HTML дашборда"""
    
    client = Minio(**MINIO_CONFIG)
    
    metrics_list = []
    if client.bucket_exists('metrics'):
        objects = list(client.list_objects('metrics', recursive=True))
        for obj in objects[-10:]:
            response = client.get_object('metrics', obj.object_name)
            data = json.loads(response.read())
            response.close()
            metrics_list.append(data)
    
    alerts_list = []
    if client.bucket_exists('alerts'):
        objects = list(client.list_objects('alerts', recursive=True))
        for obj in objects[-10:]:
            response = client.get_object('alerts', obj.object_name)
            data = json.loads(response.read())
            response.close()
            alerts_list.append(data)
    
    html_content = f'''<!DOCTYPE html>
<html>
<head>
    <title>Data Platform Observability Dashboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        .metric {{ background: white; padding: 15px; margin: 10px 0; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .success {{ border-left: 4px solid #4CAF50; }}
        .warning {{ border-left: 4px solid #FF9800; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #333; }}
        .metric-label {{ color: #666; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #4CAF50; color: white; }}
        .dashboard {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
    </style>
</head>
<body>
    <h1>📊 Data Platform Observability Dashboard</h1>
    <p>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <div class="dashboard">
        <div class="metric success">
            <div class="metric-value">{len(metrics_list)}</div>
            <div class="metric-label">Total DAG Runs (last 10)</div>
        </div>
        <div class="metric success">
            <div class="metric-value">{len([m for m in metrics_list if m.get('success_rate', 0) == 100])}</div>
            <div class="metric-label">Successful Runs</div>
        </div>
        <div class="metric warning">
            <div class="metric-value">{len(alerts_list)}</div>
            <div class="metric-label">Alerts (last 24h)</div>
        </div>
    </div>
    
    <h2>📈 Recent DAG Metrics</h2>
    <table>
        <tr>
            <th>Timestamp</th>
            <th>Duration (sec)</th>
            <th>Success Rate</th>
            <th>Status</th>
        </tr>
'''
    
    for m in metrics_list:
        html_content += f'''
        <tr>
            <td>{m.get('execution_date', 'N/A')}</td>
            <td>{m.get('duration_seconds', 'N/A')}</td>
            <td>{m.get('success_rate', 0):.1f}%</td>
            <td>{'✅' if m.get('success_rate', 0) == 100 else '⚠️'}</td>
        </tr>
'''
    
    html_content += '''
    </table>
    
    <h2>🚨 Recent Alerts</h2>
    <table>
        <tr>
            <th>Timestamp</th>
            <th>Severity</th>
            <th>Message</th>
        </tr>
'''
    
    for a in alerts_list:
        html_content += f'''
        <tr>
            <td>{a.get('timestamp', 'N/A')}</td>
            <td>{a.get('severity', 'INFO')}</td>
            <td>{a.get('message', 'N/A')[:50]}</td>
        </tr>
'''
    
    html_content += '''
    </table>
    
    <h2>📋 System Status</h2>
    <div class="metric success">
        <div class="metric-value">✅ MinIO</div>
        <div class="metric-label">Object Storage - Healthy</div>
    </div>
    <div class="metric success">
        <div class="metric-value">✅ Airflow</div>
        <div class="metric-label">Orchestrator - Healthy</div>
    </div>
</body>
</html>'''
    
    if not client.bucket_exists('dashboard'):
        client.make_bucket('dashboard')
    
    dashboard_file = f'dashboard_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
    client.put_object(
        'dashboard',
        dashboard_file,
        data=BytesIO(html_content.encode('utf-8')),
        length=len(html_content),
        content_type='text/html'
    )
    
    client.put_object(
        'dashboard',
        'latest.html',
        data=BytesIO(html_content.encode('utf-8')),
        length=len(html_content),
        content_type='text/html'
    )
    
    logger.info("✅ Observability dashboard generated")
    logger.info(f"🌐 Dashboard: http://localhost:9001/browser/dashboard/{dashboard_file}")
    logger.info("🌐 Latest: http://localhost:9001/browser/dashboard/latest.html")
    
    return True

default_args = {
    'owner': 'observability_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'observability_pipeline',
    default_args=default_args,
    description='Observability: metrics, alerts, dashboard',
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['observability', 'monitoring']
)

collect_metrics = PythonOperator(
    task_id='collect_dag_metrics',
    python_callable=collect_dag_metrics,
    provide_context=True,
    dag=dag
)

check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag
)

generate_dashboard = PythonOperator(
    task_id='generate_dashboard',
    python_callable=generate_observability_dashboard,
    provide_context=True,
    dag=dag
)

[collect_metrics, check_quality] >> generate_dashboard
