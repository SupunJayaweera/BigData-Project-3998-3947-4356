from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import csv
import os

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Database credentials from environment or hardcoded mapping as per Docker compose
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "traffic_db"
DB_USER = "traffic_user"
DB_PASS = "traffic_password"

def export_table_to_csv(table_name, filter_date, output_path):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cur = conn.cursor()
    
    # Determine the time column based on table name
    time_col = "window_start" if table_name == "congestion_windows" else "event_time"
    
    # Query to fetch current day's data
    query = f"""
        SELECT * FROM {table_name}
        WHERE {time_col} >= '{filter_date} 00:00:00'::timestamp
        AND {time_col} < '{filter_date} 00:00:00'::timestamp + interval '1 day'
    """
    
    # Add an order by for consistent sorting
    if table_name == 'sensor_readings':
        query += " ORDER BY event_time DESC"
    elif table_name == 'congestion_windows':
        query += " ORDER BY window_start DESC"
    elif table_name == 'critical_alerts':
        query += " ORDER BY event_time DESC"

    output_file = os.path.join(output_path, f"{table_name}_{filter_date}.csv")
    
    with open(output_file, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        cur.execute(query)
        
        # Write Headers
        csv_writer.writerow([desc[0] for desc in cur.description])
        
        # Write Data
        rows = cur.fetchall()
        for row in rows:
            csv_writer.writerow(row)
            
    print(f"Exported {len(rows)} records to {output_file}")
    
    cur.close()
    conn.close()

def generate_daily_reports(**kwargs):
    # Airflow execution date (logical date) is usually passing the scheduled date
    # In Airflow 2, context['ds'] gives the YYYY-MM-DD
    execution_date = kwargs['ds']
    
    # Target directory from Volume mapping
    target_dir = "/opt/airflow/reports"
    os.makedirs(target_dir, exist_ok=True)
    
    tables_to_export = [
        "sensor_readings",
        "congestion_windows",
        "critical_alerts"
    ]
    
    for table in tables_to_export:
        export_table_to_csv(table, execution_date, target_dir)

with DAG(
    'daily_traffic_reporting',
    default_args=default_args,
    description='Generate daily traffic CSV reports',
    schedule_interval="0 23 * * *",
    catchup=False,
    tags=['traffic', 'reporting']
) as dag:

    generate_reports_task = PythonOperator(
        task_id='generate_daily_csv_reports',
        python_callable=generate_daily_reports,
        provide_context=True
    )

    generate_reports_task
