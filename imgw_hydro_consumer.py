import json
import sqlite3
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from alert_visualizer import create_alert_map_with_list, get_alerts_from_data

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Zmień na odpowiedni adres
HYDRO_TOPIC = 'imgw-hydro-data'
DATABASE_NAME = 'imgw_hydro_data.db'

def wait_for_kafka(max_retries=5, delay=5):
    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            consumer.close()
            return True
        except NoBrokersAvailable:
            print(f"⏳ Próba {i+1}/{max_retries} - Kafka niedostępna, czekam {delay}s...")
            time.sleep(delay)
    return False

def create_database():
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
            wojewodztwo TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def process_and_save_data(data):
    if not data:
        return

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    for record in data:
        try:
            cursor.execute('''
                INSERT INTO hydro_data 
                (station_id, station_name, river, water_level, water_status, measurement_date, wojewodztwo)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                record.get('id_stacji'),
                record.get('stacja'),
                record.get('rzeka'),
                float(record.get('stan_wody')) if record.get('stan_wody') else None,
                record.get('stan_wody_status'),
                record.get('data_pomiaru'),
                record.get('wojewodztwo', 'nieznane')
            ))
        except Exception as e:
            print(f"⚠️ Błąd zapisu rekordu: {e}")

    conn.commit()
    
    # Generuj alerty i mapę
    alerts = get_alerts_from_data(data)
    if alerts:
        print(f"⚠️ Wykryto {len(alerts)} alertów hydrologicznych!")
        create_alert_map_with_list(alerts)
    
    conn.close()
    print(f"💾 Zapisano {len(data)} rekordów do bazy danych.")

def kafka_consumer():
    if not wait_for_kafka():
        print("❌ Nie udało się połączyć z brokerem Kafka.")
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
                print(f"✅ Odebrano {len(data)} rekordów.")
                process_and_save_data(data)
            else:
                print("⚠️ Nieoczekiwany format danych:", type(data))
        except Exception as e:
            print(f"❌ Błąd przetwarzania wiadomości: {e}")

if __name__ == '__main__':
    create_database()
    kafka_consumer()
