import os
import psycopg2
from flask import Flask, jsonify, send_from_directory
from psycopg2.extras import RealDictCursor

app = Flask(__name__, static_folder='public')

DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "traffic_db")
DB_USER = os.environ.get("POSTGRES_USER", "traffic_user")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "traffic_password")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

@app.route('/api/live-traffic')
def api_live_traffic():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Get the latest reading per junction
    cur.execute("""
        SELECT DISTINCT ON (sensor_id) sensor_id, event_time, vehicle_count, avg_speed
        FROM sensor_readings
        ORDER BY sensor_id, event_time DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/api/alerts')
def api_alerts():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT sensor_id, event_time, vehicle_count, avg_speed 
        FROM critical_alerts 
        ORDER BY event_time DESC 
        LIMIT 10;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/api/analytics')
def api_analytics():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT EXTRACT(HOUR FROM event_time) as hour, sensor_id, SUM(vehicle_count) as total_vehicles, AVG(avg_speed) as avg_hourly_speed
        FROM sensor_readings
        WHERE event_time >= date_trunc('day', CURRENT_TIMESTAMP)
        GROUP BY 1, 2
        ORDER BY 1, 2;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/api/police-deployment')
def api_police_deployment():
    # Implementation matches the CSV report logic roughly
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        WITH hourly_stats AS (
            SELECT 
                sensor_id as junction,
                EXTRACT(HOUR FROM event_time) as hour,
                SUM(vehicle_count) as total_vehicle_count,
                AVG(avg_speed) as avg_speed_kmh
            FROM sensor_readings
            WHERE event_time >= date_trunc('day', CURRENT_TIMESTAMP)
            GROUP BY 1, 2
        ),
        peak_hours AS (
            SELECT DISTINCT ON (junction)
                junction,
                hour as peak_hour,
                total_vehicle_count as peak_vehicle_count,
                avg_speed_kmh as peak_avg_speed_kmh
            FROM hourly_stats
            ORDER BY junction, total_vehicle_count DESC
        ),
        alerts AS (
            SELECT sensor_id as junction, COUNT(*) as critical_alert_count
            FROM critical_alerts
            WHERE event_time >= date_trunc('day', CURRENT_TIMESTAMP)
            GROUP BY 1
        )
        SELECT 
            p.junction,
            LPAD(p.peak_hour::text, 2, '0') || ':00' as peak_hour,
            p.peak_vehicle_count,
            ROUND(p.peak_avg_speed_kmh::numeric, 1) as peak_avg_speed_kmh,
            COALESCE(a.critical_alert_count, 0) as critical_alert_count,
            ROUND((p.peak_vehicle_count * (1.0/NULLIF(p.peak_avg_speed_kmh, 0)) * (1 + COALESCE(a.critical_alert_count, 0)))::numeric, 2) as deployment_score
        FROM peak_hours p
        LEFT JOIN alerts a ON p.junction = a.junction
        ORDER BY deployment_score DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)