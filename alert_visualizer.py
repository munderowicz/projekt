import folium
import json
import sqlite3
from jinja2 import Template
import os
from datetime import datetime
import pandas as pd
import plotly.express as px
from collections import defaultdict

DATABASE_NAME = 'imgw_hydro_data.db'
ALERT_THRESHOLD = 500  # próg ostrzeżenia (mm)

def get_current_data():
    """Pobiera najnowsze dane z bazy"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM hydro_data 
        WHERE timestamp = (SELECT MAX(timestamp) FROM hydro_data)
    ''')
    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return data

def generate_visualizations():
    data = get_current_data()
    if not data:
        print("Brak danych do wizualizacji")
        return

    # Dane o czasie pobrania
    last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Przygotowanie danych do mapy
    map_data = []
    for record in data:
        try:
            lat = float(record.get('latitude')) if record.get('latitude') else None
            lon = float(record.get('longitude')) if record.get('longitude') else None
            if lat and lon:
                map_data.append({
                    "station_id": record.get("station_id"),
                    "station_name": record.get("station_name"),
                    "river": record.get("river"),
                    "water_level": float(record.get("water_level")) if record.get("water_level") else None,
                    "flow": float(record.get("flow")) if record.get("flow") else None,
                    "lat": lat,
                    "lon": lon
                })
        except Exception as e:
            print(f"Błąd przetwarzania rekordu: {e}")

    # Stacje z przepływem (flow)
    flow_stations = sorted(
        [r for r in data if r.get('flow') is not None],
        key=lambda x: float(x['flow']) if x['flow'] else 0
    )

    # Generowanie mapy
    m = folium.Map(location=[52.0, 19.0], zoom_start=6)
    
    # Dodanie markerów dla każdej stacji
    for station in map_data:
        popup_content = f"""
        <b>{station['station_name']}</b><br>
        Rzeka: {station['river']}<br>
        Stan wody: {station['water_level']} cm<br>
        Przepływ: {station['flow']} m³/s
        """
        folium.Marker(
            location=[station['lat'], station['lon']],
            popup=popup_content,
            icon=folium.Icon(color='blue', icon='tint')
        ).add_to(m)

    # Generowanie wykresu
    df = pd.DataFrame(map_data)
    fig = px.bar(
        df, 
        x='station_name', 
        y='water_level',
        title='Stan wody w stacjach pomiarowych',
        labels={'station_name': 'Stacja', 'water_level': 'Stan wody (cm)'}
    )
    plot_html = fig.to_html(full_html=False)

    # Generowanie HTML
    html_template = Template("""
    <html>
    <head>
        <meta charset="utf-8"/>
        <title>Monitor hydrologiczny IMGW</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { font-family: sans-serif; margin: 0; padding: 20px; }
            .container { display: flex; flex-direction: column; gap: 20px; }
            .header { display: flex; justify-content: space-between; align-items: center; }
            .map-container { height: 600px; border: 1px solid #ddd; border-radius: 5px; }
            .plot-container { border: 1px solid #ddd; border-radius: 5px; padding: 10px; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; }
            th, td { padding: 8px; border: 1px solid #ddd; text-align: left; }
            th { background-color: #f2f2f2; }
            button { padding: 8px 16px; background-color: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; }
            button:hover { background-color: #45a049; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Monitor hydrologiczny IMGW</h1>
                <div>
                    <p>Ostatnia aktualizacja: {{ last_update }}</p>
                    <button onclick="window.location.reload();">Odśwież dane</button>
                </div>
            </div>
            
            <div class="map-container" id="map">{{ map_html | safe }}</div>
            
            <div class="plot-container">
                {{ plot_html | safe }}
            </div>
            
            <h2>Stacje z pomiarem przepływu</h2>
            <table>
                <thead>
                    <tr>
                        <th>Stacja</th>
                        <th>Rzeka</th>
                        <th>Przepływ (m³/s)</th>
                        <th>Stan wody (cm)</th>
                    </tr>
                </thead>
                <tbody>
                    {% for station in flow_stations %}
                    <tr>
                        <td>{{ station.station_name }}</td>
                        <td>{{ station.river }}</td>
                        <td>{{ station.flow }}</td>
                        <td>{{ station.water_level }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """)

    # Zapisz mapę jako tymczasowy HTML
    map_html_path = "map_temp.html"
    m.save(map_html_path)
    with open(map_html_path, "r", encoding="utf-8") as f:
        map_html = f.read()
    os.remove(map_html_path)

    final_html = html_template.render(
        last_update=last_update,
        map_html=map_html,
        plot_html=plot_html,
        flow_stations=flow_stations
    )

    with open("hydro_visualization.html", "w", encoding="utf-8") as f:
        f.write(final_html)

    print("Wygenerowano wizualizację w pliku hydro_visualization.html")

if __name__ == '__main__':
    generate_visualizations()
