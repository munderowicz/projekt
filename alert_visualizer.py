import folium
import json
from collections import defaultdict
from jinja2 import Template
import os

ALERT_THRESHOLD = 500  # próg ostrzeżenia (mm)

# Lista testowa — zastąp danymi z IMGW
# alerts = get_alerts_from_data(data)
# Zakładamy, że każda stacja ma "wojewodztwo"

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

def create_alert_map_with_list(alerts, wojewodztwa_geojson='wojewodztwa-polski.geojson', output_file='alert_map.html'):
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
        </style>
    </head>
    <body>
        <div id="map">{{ map_html | safe }}</div>
        <div id="alerts">
            <h2>⚠️ Alerty hydrologiczne ({{ alerts|length }})</h2>
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
                    <tr>
                        <td>{{ a.wojewodztwo }}</td>
                        <td>{{ a.station_name }}</td>
                        <td>{{ a.river }}</td>
                        <td>{{ a.level }}</td>
                        <td>{{ a.date }}</td>
                    </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """)

    final_html = html_template.render(map_html=map_html, alerts=alerts)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(final_html)

    os.remove(map_html_path)
    print(f"📍 Zapisano mapę alertów z listą do pliku: {output_file}")
