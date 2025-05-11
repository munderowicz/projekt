import json
import requests
import csv
import time
import sys
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# Konfiguracja
KAFKA_BOOTSTRAP_SERVERS = '172.18.0.3:9092'
HYDRO_TOPIC = 'imgw-hydro-data'
CSV_FILE = 'hydro_data.csv'
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

def init_csv_file():
    """Inicjalizuje plik CSV z nagłówkami"""
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow([
            'id_stacji', 'stacja', 'rzeka', 
            'stan_wody', 'stan_wody_status', 'data_pomiaru', 'timestamp'
        ])

def fetch_hydro_data():
    """Pobiera dane z API IMGW"""
    try:
        response = requests.get(API_URL, headers={'Accept': 'application/json'}, timeout=10)
        response.encoding = 'utf-8'  # Ustawienie kodowania UTF-8 dla odpowiedzi
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Błąd podczas pobierania danych: {e}")
        return None

def process_and_save_data(data):
    """Zapisuje dane do pliku CSV z uwzględnieniem polskich znaków"""
    if not data:
        return

    with open(CSV_FILE, mode='a', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file, delimiter=';')
        
        for record in data:
            try:
                # Przygotowanie danych - zachowanie polskich znaków
                writer.writerow([
                    record.get('id_stacji', ''),
                    record.get('stacja', ''),
                    record.get('rzeka', ''),
                    float(record.get('stan_wody')) if record.get('stan_wody') else '',
                    record.get('stan_wody_status', ''),
                    record.get('data_pomiaru', ''),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ])
            except Exception as e:
                print(f"⚠️ Błąd podczas zapisu rekordu: {e}")
    
    print(f"💾 Zapisano {len(data)} rekordów do pliku CSV")

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
    """Odbiera dane z Kafka i zapisuje do CSV"""
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
    init_csv_file()

    # Tryb działania z linii poleceń: python script.py producer
    mode = sys.argv[1] if len(sys.argv) > 1 else 'consumer'

    if mode == 'producer':
        kafka_producer()
    elif mode == 'consumer':
        kafka_consumer()
    else:
        print("⚠️ Nieznany tryb. Użyj 'producer' lub 'consumer'.")
