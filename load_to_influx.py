import pandas as pd
from influxdb import InfluxDBClient

# Параметры подключения к InfluxDB
INFLUX_HOST = 'localhost'
INFLUX_PORT = 8086
# INFLUX_USER = 'admin'
# INFLUX_PASSWORD = 'admin'
INFLUX_DATABASE = 'requests_data'

# Путь к CSV файлу
CSV_FILE = 'data/result_2024_07.csv'
ARTIFACTS_DIR = 'artifacts'

def aggregate_by_hour(data):
    """Агрегирует данные по часу для каждого URL"""
    # Используем колонки из CSV: server_time и url_clear
    data['server_time'] = pd.to_datetime(data['server_time'])
    data['hour'] = data['server_time'].dt.floor('h')
    
    # Группировка по часу и URL
    hourly_stats = data.groupby(['hour', 'url_clear']).size().reset_index(name='request_count')
    hourly_stats = hourly_stats.rename(columns={'url_clear': 'url'})
    return hourly_stats

def create_influx_points(hourly_data):
    """Создает точки данных для InfluxDB в JSON формате"""
    points = []
    for _, row in hourly_data.iterrows():
        point = {
            "measurement": "url_requests",
            "time": row['hour'].isoformat(),
            "tags": {"url": row['url']},
            "fields": {
                "count": int(row['request_count'])
            }
        }
        points.append(point)
    return points

def main():
    # Чтение CSV файла
    print(f"Чтение данных из {CSV_FILE}...")
    try:
        df = pd.read_csv(CSV_FILE)
    except FileNotFoundError:
        print(f"Ошибка: файл {CSV_FILE} не найден")
        return
    except Exception as e:
        print(f"Ошибка при чтении CSV: {e}")
        return
    
    # Проверка наличия необходимых колонок
    required_columns = ['server_time', 'url_clear']
    if not all(col in df.columns for col in required_columns):
        print(f"Ошибка: CSV должен содержать колонки: {required_columns}")
        return
    
    # Агрегация данных
    print("Агрегация данных по часу...")
    hourly_data = aggregate_by_hour(df)
    
    # Запись агрегированных данных в txt файл
    print("Запись агрегированных данных в файл...")
    import os
    csv_base = os.path.splitext(os.path.basename(CSV_FILE))[0]
    txt_file = f"{ARTIFACTS_DIR}/{csv_base}.txt"
    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write("hour,url,request_count\n")
        for _, row in hourly_data.iterrows():
            f.write(f"{row['hour']},{row['url']},{row['request_count']}\n")
    
    # Подключение к InfluxDB
    print("Подключение к InfluxDB...")
    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        # username=INFLUX_USER,
        # password=INFLUX_PASSWORD
    )
    
    # Создание базы данных если не существует
    databases = client.get_list_database()
    if not any(db['name'] == INFLUX_DATABASE for db in databases):
        client.create_database(INFLUX_DATABASE)
        print(f"Создана база данных {INFLUX_DATABASE}")
    
    client.switch_database(INFLUX_DATABASE)
    
    # Запись данных в формате JSON
    points = create_influx_points(hourly_data)
    print(f"Запись {len(points)} точек данных...")
    client.write_points(points)
    
    print("Загрузка данных завершена!")
    client.close()

if __name__ == "__main__":
    main()
