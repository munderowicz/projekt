import json
import sqlite3
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
HYDRO_TOPIC = 'imgw-hydro-data'
DATABASE_NAME = 'imgw_hydro_data.db'

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
            flow REAL,
            latitude REAL,
            longitude REAL,
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
                (station_id, station_name, river, water_level, water_status, flow, 
                 latitude, longitude, measurement_date, wojewodztwo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record.get('id_stacji'),
                record.get('stacja'),
                record.get('rzeka'),
                float(record.get('stan_wody')) if record.get('stan_wody') else None,
                record.get('stan_wody_status'),
                float(record.get('przeplyw')) if record.get('przeplyw') else None,
                float(record.get('latitude')) if record.get('latitude') else None,
                float(record.get('longitude')) if record.get('longitude') else None,
                record.get('data_pomiaru'),
                record.get('wojewodztwo', 'nieznane')
            ))
        except Exception as e:
            print(f"Błąd zapisu rekordu: {e}")

    conn.commit()
    conn.close()
    print(f"Zapisano {len(data)} rekordów do bazy danych")

def kafka_consumer():
    consumer = KafkaConsumer(
        HYDRO_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    
    print("Konsument uruchomiony - oczekiwanie na dane...")
    for message in consumer:
        try:
            data = message.value
            if isinstance(data, list):
                print(f"Odebrano {len(data)} rekordów")
                process_and_save_data(data)
                # Generuj wizualizację po każdym otrzymaniu danych
                from alert_visualizer import generate_visualizations
                generate_visualizations()
        except Exception as e:
            print(f"Błąd przetwarzania wiadomości: {e}")

if __name__ == '__main__':
    create_database()
    kafka_consumer()
