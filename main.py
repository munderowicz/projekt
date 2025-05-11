import json
import requests
import sqlite3
import time
import sys
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# Konfiguracja
KAFKA_BOOTSTRAP_SERVERS = '172.18.0.3:9092'
HYDRO_TOPIC = 'imgw-hydro-data'
DATABASE_NAME = 'imgw_hydro_data.db'
API_URL = 'https://danepubliczne.imgw.pl/api/data/hydro2/'

def wait_for_kafka(max_retries=5, delay=5):
    """Czeka na dostępność brokera Kafka"""
    for i in range(max_retries):
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            producer.close()
            return True
        except NoBrokersAvailable:
            print(f"⏳ Próba {i+1}/{max_retries} - Broker Kafka niedostępny, czekam {delay}s...")
            time.sleep(delay)
    return False

def create_database():
    """Tworzy bazę danych SQLite"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hydro_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            station_name TEXT,
            river TEXT,
            water_level REAL,
            water_status TEXT,
            measurement_date TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def fetch_hydro_data():
    """Pobiera dane z API IMGW"""
    try:
        response = requests.get(API_URL, headers={'Accept': 'application/json'}, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Błąd podczas pobierania danych: {e}")
        return None

def process_and_save_data(data):
    """Zapisuje dane do SQLite"""
    if not data:
        return

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    for record in data:
        try:
            cursor.execute('''
                INSERT INTO hydro_data 
                (station_id, station_name, river, water_level, water_status, measurement_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                record.get('id_stacji'),
                record.get('stacja'),
                record.get('rzeka'),
                float(record.get('stan_wody')) if record.get('stan_wody') else None,
                record.get('stan_wody_status'),
                record.get('data_pomiaru')
            ))
        except Exception as e:
            print(f"⚠️ Błąd podczas zapisu rekordu: {e}")
    
    conn.commit()
    conn.close()
    print(f"💾 Zapisano {len(data)} rekordów do bazy danych")

def kafka_producer():
    """Wysyła dane do Kafka"""
    if not wait_for_kafka():
        print("❌ Nie można połączyć się z brokerem Kafka")
        return

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    data = fetch_hydro_data()
    if data:
        producer.send(HYDRO_TOPIC, value=data)
        producer.flush()
        print(f"📤 Wysłano {len(data)} rekordów do topiku '{HYDRO_TOPIC}'")

def kafka_consumer():
    """Odbiera dane z Kafka i zapisuje do bazy"""
    if not wait_for_kafka():
        print("❌ Nie można połączyć się z brokerem Kafka")
        return

    consumer = KafkaConsumer(
        HYDRO_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("📥 Konsument uruchomiony – oczekiwanie na dane...")
    for message in consumer:
        try:
            data = message.value
            if isinstance(data, list):
                print(f"✅ Odebrano {len(data)} rekordów")
                process_and_save_data(data)
            else:
                print("⚠️ Otrzymano dane w nieoczekiwanym formacie:", type(data))
        except json.JSONDecodeError as e:
            print(f"❌ Błąd dekodowania JSON: {e}")
        except Exception as e:
            print(f"❌ Inny błąd: {e}")

if __name__ == '__main__':
    create_database()

    # Tryb działania z linii poleceń: python script.py producer
    mode = sys.argv[1] if len(sys.argv) > 1 else 'consumer'

    if mode == 'producer':
        kafka_producer()
    elif mode == 'consumer':
        kafka_consumer()
    else:
        print("⚠️ Nieznany tryb. Użyj 'producer' lub 'consumer'.")
