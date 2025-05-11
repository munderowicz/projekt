import csv
from jinja2 import Template
from datetime import datetime

def generate_html_from_csv(csv_file='hydro_data.csv', output_file='hydro_table.html'):
    # Wczytaj dane z pliku CSV z uwzględnieniem polskich znaków
    data = []
    with open(csv_file, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            # Konwersja pustych stringów na None dla lepszego wyświetlania
            cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
            data.append(cleaned_row)

    # Szablon HTML z tabelą
    html_template = Template("""
    <!DOCTYPE html>
    <html lang="pl">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dane hydrologiczne IMGW</title>
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
            }
            th, td {
                padding: 12px 15px;
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
        </style>
    </head>
    <body>
        <h1>Dane hydrologiczne IMGW</h1>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        {% for header in headers %}
                        <th>{{ header }}</th>
                        {% endfor %}
                    </tr>
                </thead>
                <tbody>
                    {% for row in data %}
                    <tr>
                        {% for header in headers %}
                        <td {% if row[header] is none %}class="null-value"{% endif %}>
                            {{ row[header] if row[header] is not none else 'brak danych' }}
                        </td>
                        {% endfor %}
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
    headers = ['id_stacji', 'stacja', 'rzeka', 'stan_wody', 'stan_wody_status', 'data_pomiaru', 'timestamp']
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    final_html = html_template.render(
        headers=headers,
        data=data,
        timestamp=timestamp
    )

    # Zapisz do pliku z kodowaniem UTF-8
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(final_html)
    
    print(f"✅ Wygenerowano plik HTML: {output_file}")

if __name__ == '__main__':
    generate_html_from_csv()
