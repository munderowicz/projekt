import csv
from jinja2 import Template
from datetime import datetime

def generate_html_from_csv(csv_file='hydro_data.csv', output_file='hydro_table.html'):
    # Wczytaj dane z pliku CSV
    data = []
    with open(csv_file, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            # Konwersja pustych wartości na None dla lepszego wyświetlania
            cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
            data.append(cleaned_row)

    # Szablon HTML z tabelą danych
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
            h1 {
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
        </style>
    </head>
    <body>
        <h1>Dane hydrologiczne IMGW (hydro2)</h1>
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
                        <th>Data pomiaru przepływu</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in data %}
                    <tr>
                        <td>{{ row['kod_stacji'] if row['kod_stacji'] is not none else '<span class="null-value">brak</span>'|safe }}</td>
                        <td>{{ row['nazwa_stacji'] if row['nazwa_stacji'] is not none else '<span class="null-value">brak</span>'|safe }}</td>
                        <td class="coords">
                            {% if row['lon'] is not none and row['lat'] is not none %}
                            {{ "%.6f"|format(row['lon']|float) }}, {{ "%.6f"|format(row['lat']|float) }}
                            {% else %}
                            <span class="null-value">brak</span>
                            {% endif %}
                        </td>
                        <td>{{ row['stan'] if row['stan'] is not none else '<span class="null-value">brak</span>'|safe }}</td>
                        <td>{{ row['stan_data'] if row['stan_data'] is not none else '<span class="null-value">brak</span>'|safe }}</td>
                        <td>{{ row['przeplyw'] if row['przeplyw'] is not none else '<span class="null-value">brak</span>'|safe }}</td>
                        <td>{{ row['przeplyw_data'] if row['przeplyw_data'] is not none else '<span class="null-value">brak</span>'|safe }}</td>
                        <td>{{ row['timestamp'] if row['timestamp'] is not none else '<span class="null-value">brak</span>'|safe }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        <div class="footer">
            Ostatnia aktualizacja: {{ timestamp }} | Liczba rekordów: {{ data|length }}
        </div>
    </body>
    </html>
    """)

    # Generuj HTML
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    final_html = html_template.render(
        data=data,
        timestamp=timestamp
    )

    # Zapisz do pliku
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(final_html)
    
    print(f"✅ Wygenerowano plik HTML: {output_file}")

if __name__ == '__main__':
    generate_html_from_csv()
