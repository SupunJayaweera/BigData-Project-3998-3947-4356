import json
import time
import random
import logging
import signal
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure Logger based on ui-context.md standards
class CustomFormatter(logging.Formatter):
    def format(self, record):
        # YYYY-MM-DDTHH:MM:SS [LEVEL] producer — {message}
        log_time = datetime.fromtimestamp(record.created).strftime('%Y-%m-%dT%H:%M:%S')
        return f"{log_time} [{record.levelname}] producer — {record.getMessage()}"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(CustomFormatter())
logger.addHandler(sh)

# Constants
KAFKA_BROKER = 'localhost:9092'  # External port mapped in docker-compose
TOPIC_RAW = 'traffic-raw'
TOPIC_CRITICAL = 'traffic-critical'

SENSORS = ["J1_KOLLUPITIYA", "J2_BAMBALAPITIYA", "J3_TOWN_HALL", "J4_BORELLA"]

producer = None
running = True

def signal_handler(sig, frame):
    global running
    logger.info("KeyboardInterrupt received. Shutting down producer...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

def get_producer():
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            retries=3
        )
        logger.info(f"Smart City Traffic Producer started. Sending to {KAFKA_BROKER}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka at {KAFKA_BROKER}: {e}")
        sys.exit(1)

def delivery_report(err, msg, sensor_id):
    if err is not None:
        logger.error(f"[DELIVERY FAILED] {sensor_id} — {err}")
    else:
        # Note: kafka-python's send returns a future, and we retrieve record metadata
        # We handle this correctly in the future callback
        pass

def on_send_success(record_metadata, sensor_id):
    logger.info(f"[DELIVERED] {sensor_id} @ offset {record_metadata.offset}")

def on_send_error(excp, sensor_id):
    logger.error(f"[DELIVERY FAILED] {sensor_id} — {excp}")

def generate_telemetry(sensor_id, is_critical):
    # Base ranges for normal traffic
    vehicle_count = random.randint(10, 80)
    avg_speed = round(random.uniform(15.0, 60.0), 2)
    
    if is_critical:
        # Critical traffic: dangerously slow (avg_speed < 10 km/h)
        vehicle_count = random.randint(90, 150)
        avg_speed = round(random.uniform(2.0, 9.9), 2)
        
    now_iso = datetime.now(timezone.utc).isoformat()
    
    payload = {
        "sensor_id": sensor_id,
        "timestamp": now_iso,
        "vehicle_count": vehicle_count,
        "avg_speed_kmh": avg_speed
    }
    
    return payload

def main():
    global producer
    producer = get_producer()
    
    cycle_num = 1
    while running:
        sensors_sent = 0
        for sensor_id in SENSORS:
            if not running:
                break
                
            # 5% chance of critical anomaly injection
            is_critical = random.random() < 0.05
            payload = generate_telemetry(sensor_id, is_critical)
            
            if is_critical:
                logger.warning(f"[CRITICAL INJECTED] [{sensor_id}] vc={payload['vehicle_count']} spd={payload['avg_speed_kmh']} km/h")
                # Also explicitly write to critical topic per architecture
                producer.send(TOPIC_CRITICAL, key=sensor_id, value=payload).add_callback(
                    on_send_success, sensor_id=sensor_id
                ).add_errback(
                    on_send_error, sensor_id=sensor_id
                )
            
            logger.info(f"[{sensor_id}] vc={payload['vehicle_count']} spd={payload['avg_speed_kmh']} km/h -> {TOPIC_RAW}")
            
            producer.send(TOPIC_RAW, key=sensor_id, value=payload).add_callback(
                on_send_success, sensor_id=sensor_id
            ).add_errback(
                on_send_error, sensor_id=sensor_id
            )
            
            sensors_sent += 1
            
        if running:
            logger.info(f"--- Cycle {cycle_num} complete. {sensors_sent}/{len(SENSORS)} sensors sent. Sleeping 1s ---")
            cycle_num += 1
            time.sleep(1)
            
    if producer:
        logger.info("Flushing producer before exit...")
        producer.flush()
        producer.close()
        logger.info("Producer exited safely.")

if __name__ == "__main__":
    main()
