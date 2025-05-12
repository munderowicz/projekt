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
    """Inicjalizuje plik CSV z nagłówkami zgodnymi z API hydro2"""
    fieldnames = [
        'kod_stacji', 'nazwa_stacji', 'lon', 'lat',
        'stan', 'stan_data', 'przeplyw', 'przeplyw_data',
        'timestamp'
    ]
    
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()

def fetch_hydro_data():
    """Pobiera dane z API IMGW hydro2"""
    try:
        response = requests.get(API_URL, headers={'Accept': 'application/json'}, timeout=10)
        response.encoding = 'utf-8'
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Błąd podczas pobierania danych: {e}")
        return None

def process_and_save_data(data):
    """Zapisuje dane do pliku CSV"""
    if not data:
        return

    fieldnames = [
        'kod_stacji', 'nazwa_stacji', 'lon', 'lat',
        'stan', 'stan_data', 'przeplyw', 'przeplyw_data',
        'timestamp'
    ]

    with open(CSV_FILE, mode='a', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';')
        
        for record in data:
            try:
                # Przygotowanie rekordu z uwzględnieniem poprawnej nazwy 'przeplyw' (w API jest 'przelyw')
                row = {
                    'kod_stacji': record.get('kod_stacji'),
                    'nazwa_stacji': record.get('nazwa_stacji'),
                    'lon': record.get('lon'),
                    'lat': record.get('lat'),
                    'stan': record.get('stan'),
                    'stan_data': record.get('stan_data'),
                    'przeplyw': record.get('przelyw'),  # Uwaga na różnicę w nazwie pola!
                    'przeplyw_data': record.get('przeplyw_data'),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                writer.writerow(row)
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


### KOD NA MAPE
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
 
# Wczytaj dane z pliku CSV
data = pd.read_csv('hydro_data.csv', delimiter=';')
 
# Wybieramy stacje z poziomem ostrzegawczym (450 ≤ stan < 500)
warning_data = data[data['stan'].between(450, 499, inclusive='left')]
 
# Wybieramy stacje z poziomem alarmowym (stan ≥ 500)
alarm_data = data[data['stan'] >= 500]
 
# Przekształcamy dane na współrzędne geograficzne (Point geometry)
warning_geometry = [Point(xy) for xy in zip(warning_data['lon'], warning_data['lat'])]
alarm_geometry = [Point(xy) for xy in zip(alarm_data['lon'], alarm_data['lat'])]
 
# Tworzymy GeoDataFrame dla stacji ostrzegawczych
warning_gdf = gpd.GeoDataFrame(warning_data, geometry=warning_geometry, crs="EPSG:4326")
 
# Tworzymy GeoDataFrame dla stacji alarmowych
alarm_gdf = gpd.GeoDataFrame(alarm_data, geometry=alarm_geometry, crs="EPSG:4326")
 
# Wczytujemy mapę Polski (boundary)
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
poland = world[world.name == "Poland"]
 
# Rysujemy mapę Polski
fig, ax = plt.subplots(figsize=(10, 10))
 
# Wyświetlamy granice Polski
poland.plot(ax=ax, color='lightgray')
 
# Rysujemy stacje z poziomem ostrzegawczym (pomarańczowe punkty)
warning_gdf.plot(ax=ax, marker='o', color='orange', markersize=5, label='Poziom Ostrzegawczy')
 
# Rysujemy stacje z poziomem alarmowym (czerwone punkty)
alarm_gdf.plot(ax=ax, marker='o', color='red', markersize=5, label='Poziom Alarmowy')
 
# Dodajemy legendę
plt.legend()
 
# Dodajemy tytuł
plt.title('Stacje z poziomem ostrzegawczym i alarmowym na mapie Polski')
 
# Wyświetlamy mapę
plt.show()
