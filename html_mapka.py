import os
import csv
from jinja2 import Template
from datetime import datetime
import shutil

def classify_water_levels(data):
    """Klasyfikuje stany wód na podstawie wartości w kolumnie 'stan'"""
    alarm_state = []
    warning_state = []
    normal_state = []
    
    for row in data:
        try:
            if row['stan'] is not None:
                level = float(row['stan'])
                if level >= 500:
                    alarm_state.append(row)
                elif 450 <= level < 500:
                    warning_state.append(row)
                else:
                    normal_state.append(row)
        except (ValueError, TypeError):
            continue
    
    return alarm_state, warning_state, normal_state

def generate_html_from_csv(csv_file='hydro_data.csv', output_file='hydro_table.html', last_updated_file='last_updated.txt'):
    # Sprawdzamy datę ostatniej aktualizacji
    last_updated = None
    if os.path.exists(last_updated_file):
        with open(last_updated_file, 'r') as f:
            last_updated = f.read().strip()

    # Wczytaj dane z pliku CSV
    data = []
    with open(csv_file, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            # Konwersja pustych wartości na None dla lepszego wyświetlania
            cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
            data.append(cleaned_row)

    # Klasyfikuj stany wód
    alarm_state, warning_state, normal_state = classify_water_levels(data)

    # Sprawdzanie, czy dane są nowe
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if last_updated != current_timestamp:
        with open(last_updated_file, 'w') as f:
            f.write(current_timestamp)
    
    # Szablon HTML z tabelami danych
    html_template = Template("""
    <!DOCTYPE html>
    <html lang="pl">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dane hydrologiczne IMGW (hydro2)</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }
            h1, h2 {
                color: #2c3e50;
                text-align: center;
            }
            .table-container {
                overflow-x: auto;
                margin: 20px 0;
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            table {
                width: 100%;
                border-collapse: collapse;
                font-size: 0.9em;
                margin-bottom: 20px;
            }
            th, td {
                padding: 10px 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }
            th {
                background-color: #3498db;
                color: white;
                position: sticky;
                top: 0;
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;
            }
            tr:hover {
                background-color: #e6f7ff;
            }
            .footer {
                text-align: center;
                margin-top: 20px;
                color: #7f8c8d;
                font-size: 0.9em;
            }
            .null-value {
                color: #999;
                font-style: italic;
            }
            .coords {
                font-family: monospace;
            }
            .alarm {
                background-color: #ffdddd;
            }
            .alarm th {
                background-color: #ff4444;
            }
            .warning {
                background-color: #fff3cd;
            }
            .warning th {
                background-color: #ffc107;
            }
            .summary {
                display: flex;
                justify-content: space-around;
                margin-bottom: 20px;
            }
            .summary-box {
                padding: 15px;
                border-radius: 8px;
                text-align: center;
                font-weight: bold;
                color: white;
            }
            .alarm-summary {
                background-color: #ff4444;
            }
            .warning-summary {
                background-color: #ffc107;
            }
            .normal-summary {
                background-color: #28a745;
            }
            #refresh-button {
                position: fixed;
                top: 20px;
                right: 20px;
                padding: 15px 30px;
                background-color: #3498db;
                color: white;
                font-size: 16px;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            }
            #refresh-button:hover {
                background-color: #2980b9;
            }
            .null-value {
                font-style: italic;
            }
        </style>
    </head>
    <body>
        <h1>Dane hydrologiczne IMGW (hydro2)</h1>

        <div class="summary">
            <div class="summary-box alarm-summary">
                Stany alarmowe (≥500): {{ alarm_state|length }}
            </div>
            <div class="summary-box warning-summary">
                Stany ostrzegawcze (450-499): {{ warning_state|length }}
            </div>
            <div class="summary-box normal-summary">
                Stany normalne (<450): {{ normal_state|length }}
            </div>
        </div>

        <h2>⚠️ Stany alarmowe (≥500)</h2>
        <div class="table-container alarm">
            <table>
                <thead>
                    <tr>
                        <th>Kod stacji</th>
                        <th>Nazwa stacji</th>
                        <th>Współrzędne</th>
                        <th>Stan wody</th>
                        <th>Data pomiaru stanu</th>
                        <th>Przepływ</th>
                        <th>Data pomiaru przepływu</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in alarm_state %}
                    <tr>
                        <td>{{ row['kod_stacji'] }}</td>
                        <td>{{ row['nazwa_stacji'] }}</td>
                        <td>{{ row['lon'] }}, {{ row['lat'] }}</td>
                        <td>{{ row['stan'] }}</td>
                        <td>{{ row['stan_data'] }}</td>
                        <td>{{ row['przeplyw'] }}</td>
                        <td>{{ row['przeplyw_data'] }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <h2>⚠ Stany ostrzegawcze (450-499)</h2>
        <div class="table-container warning">
            <table>
                <thead>
                    <tr>
                        <th>Kod stacji</th>
                        <th>Nazwa stacji</th>
                        <th>Współrzędne</th>
                        <th>Stan wody</th>
                        <th>Data pomiaru stanu</th>
                        <th>Przepływ</th>
                        <th>Data pomiaru przepływu</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in warning_state %}
                    <tr>
                        <td>{{ row['kod_stacji'] }}</td>
                        <td>{{ row['nazwa_stacji'] }}</td>
                        <td>{{ row['lon'] }}, {{ row['lat'] }}</td>
                        <td>{{ row['stan'] }}</td>
                        <td>{{ row['stan_data'] }}</td>
                        <td>{{ row['przeplyw'] }}</td>
                        <td>{{ row['przeplyw_data'] }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <h2>Wszystkie stacje</h2>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Kod stacji</th>
                        <th>Nazwa stacji</th>
                        <th>Współrzędne</th>
                        <th>Stan wody</th>
                        <th>Data pomiaru stanu</th>
                        <th>Przepływ</th>
                        <th>Data
