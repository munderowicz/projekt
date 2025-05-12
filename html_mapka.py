import os
import csv
from jinja2 import Template
from datetime import datetime
from flask import Flask, render_template_string

app = Flask(__name__)

# Funkcja klasyfikująca stany wód
def classify_water_levels(data):
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

# Funkcja generująca HTML z danymi
def generate_html_from_csv(csv_file='hydro_data.csv', output_file='hydro_table.html', last_updated_file='last_updated.txt'):
    # Wczytanie danych z pliku CSV
    data = []
    with open(csv_file, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
            data.append(cleaned_row)

    # Klasyfikacja stanów wód
    alarm_state, warning_state, normal_state = classify_water_levels(data)

    # Zapis daty ostatniej aktualizacji
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(last_updated_file, 'w') as f:
        f.write(current_timestamp)

    # Szablon HTML
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
        </style>
    </head>
    <body>
        <h1>Dane hydrologiczne IMGW (hydro2)</h1>

        <h2>⚠️ Stany alarmowe (≥500)</h2>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Kod stacji</th>
                        <th>Nazwa stacji</th>
                        <th>Współrzędne</th>
                        <th>Stan wody</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in alarm_state %}
                    <tr>
                        <td>{{ row['kod_stacji'] }}</td>
                        <td>{{ row['nazwa_stacji'] }}</td>
                        <td>{{ row['lon'] }}, {{ row['lat'] }}</td>
                        <td>{{ row['stan'] }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div id="refresh-button">
            <form action="/refresh" method="post">
                <button type="submit">Odśwież dane</button>
            </form>
        </div>

        <div class="footer">
            Ostatnia aktualizacja: {{ timestamp }}
        </div>
    </body>
    </html>
    """)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Renderowanie HTML
    final_html = html_template.render(
        data=data,
        alarm_state=alarm_state,
        warning_state=warning_state,
        normal_state=normal_state,
        timestamp=timestamp
    )

    return final_html

# Trasa główna strony
@app.route('/')
def index():
    html_content = generate_html_from_csv()
    return render_template_string(html_content)

# Trasa do odświeżenia danych
@app.route('/refresh', methods=['POST'])
def refresh():
    html_content = generate_html_from_csv()
    return render_template_string(html_content)

if __name__ == '__main__':
    app.run(debug=True)
