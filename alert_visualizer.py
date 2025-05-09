import folium
import json
from collections import defaultdict
from jinja2 import Template
import os
import sqlite3

ALERT_THRESHOLD = 500  # próg ostrzeżenia (mm)
DATABASE_NAME = 'imgw_hydro_data.db'

def get_alerts_from_data(data):
    alerts = []
    for record in data:
        try:
            level = float(record.get('stan_wody') or 0)
            if level > ALERT_THRESHOLD:
                alerts.append({
                    "station_id": record.get("id_stacji"),
                    "station_name": record.get("stacja"),
                    "river": record.get("rzeka"),
                    "level": level,
                    "date": record.get("data_pomiaru"),
                    "wojewodztwo": record.get("wojewodztwo", "nieznane")
                })
        except Exception as e:
            print(f"⚠️ Błąd przetwarzania rekordu alertu: {e}")
    return alerts

def get_alerts_from_database():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT station_id, station_name, river, water_level, measurement_date, wojewodztwo
        FROM hydro_data 
        WHERE water_level > ? 
        ORDER BY measurement_date DESC
    ''', (ALERT_THRESHOLD,))
    
    alerts = []
    for row in cursor.fetchall():
        alerts.append({
            "station_id": row[0],
            "station_name": row[1],
            "river": row[2],
            "level": row[3],
            "date": row[4],
            "wojewodztwo": row[5]
        })
    conn.close()
    return alerts

def create_alert_map_with_list(alerts=None, wojewodztwa_geojson='wojewodztwa-polski.geojson', output_file='alert_map.html'):
    if alerts is None:
        alerts = get_alerts_from_database()
    
    if not alerts:
        print("🟢 Brak aktualnych alertów hydrologicznych.")
        return

    # Zlicz alerty per województwo
    woj_alerts = defaultdict(int)
    for alert in alerts:
        woj = alert.get("wojewodztwo")
        woj_alerts[woj] += 1

    # Inicjalizacja mapy
    m = folium.Map(location=[52.1, 19.2], zoom_start=6)

    # Warstwa województw z kolorowaniem według liczby alertów
    folium.Choropleth(
        geo_data=wojewodztwa_geojson,
        data=woj_alerts,
        columns=["Województwo", "Alerty"],
        key_on="feature.properties.nazwa",
        fill_color='YlOrRd',
        nan_fill_color="white",
        legend_name="Liczba alertów"
    ).add_to(m)

    # Dodaj markery dla każdej stacji z alertem
    for alert in alerts:
        folium.Marker(
            location=[alert.get('latitude', 52.1), alert.get('longitude', 19.2)],  # W prawdziwej implementacji potrzebne byłyby współrzędne
            popup=f"{alert['station_name']}: {alert['level']} mm",
            icon=folium.Icon(color='red', icon='exclamation-triangle')
        ).add_to(m)

    # Zapisz mapę jako HTML
    map_html_path = "map_tmp.html"
    m.save(map_html_path)

    # Wczytaj wygenerowaną mapę
    with open(map_html_path, "r", encoding="utf-8") as f:
        map_html = f.read()

    # Stwórz HTML z mapą i tabelą alertów obok
    html_template = Template("""
    <html>
    <head>
        <meta charset="utf-8"/>
        <title>Alerty Hydrologiczne</title>
        <style>
            body { font-family: sans-serif; margin: 0; display: flex; }
            #map { width: 70%; height: 100vh; }
            #alerts { width: 30%; padding: 1em; overflow-y: scroll; background: #f9f9f9; }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 8px; border: 1px solid #ccc; text-align: left; }
            th { background-color: #eee; }
            .alert-row { background-color: #ffdddd; }
        </style>
    </head>
    <body>
        <div id="map">{{ map_html | safe }}</div>
        <div id="alerts">
            <h2>⚠️ Alerty hydrologiczne ({{ alerts|length }})</h2>
            <p>Próg ostrzeżenia: {{ threshold }} mm</p>
            <table>
                <thead>
                    <tr>
                        <th>Województwo</th>
                        <th>Stacja</th>
                        <th>Rzeka</th>
                        <th>Poziom wody (mm)</th>
                        <th>Data</th>
                    </tr>
                </thead>
                <tbody>
                {% for a in alerts %}
                    <tr class="alert-row">
                        <td>{{ a.wojewodztwo }}</td>
                        <td>{{ a.station_name }}</td>
                        <td>{{ a.river }}</td>
                        <td><strong>{{ a.level }}</strong></td>
                        <td>{{ a.date }}</td>
                    </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """)

    final_html = html_template.render(map_html=map_html, alerts=alerts, threshold=ALERT_THRESHOLD)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(final_html)

    os.remove(map_html_path)
    print(f"📍 Zapisano mapę alertów z listą do pliku: {output_file}")
