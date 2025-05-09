import json
import requests
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Zmień na odpowiedni adres
HYDRO_TOPIC = 'imgw-hydro-data'
API_URL = 'https://danepubliczne.imgw.pl/api/data/hydro2/'

def wait_for_kafka(max_retries=5, delay=5):
    for i in range(max_retries):
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            producer.close()
            return True
        except NoBrokersAvailable:
            print(f"⏳ Próba {i+1}/{max_retries} - Kafka niedostępna, czekam {delay}s...")
            time.sleep(delay)
    return False

def fetch_hydro_data():
    try:
        response = requests.get(API_URL, headers={'Accept': 'application/json'}, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Błąd pobierania danych: {e}")
        return None

def kafka_producer():
    if not wait_for_kafka():
        print("❌ Nie udało się połączyć z brokerem Kafka.")
        return

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        data = fetch_hydro_data()
        if data:
            producer.send(HYDRO_TOPIC, value=data)
            producer.flush()
            print(f"📤 Wysłano {len(data)} rekordów do topiku '{HYDRO_TOPIC}'.")
        time.sleep(3600)  # Wysyłaj dane co godzinę

if __name__ == '__main__':
    kafka_producer()
