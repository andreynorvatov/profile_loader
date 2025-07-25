import csv
from influxdb import InfluxDBClient

# Параметры подключения к InfluxDB
INFLUX_HOST = 'localhost'
INFLUX_PORT = 8086
INFLUX_DATABASE = 'requests_data'

def get_all_data():
    """Получает все данные из InfluxDB"""
    print("Подключение к InfluxDB...")
    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT
    )
    
    client.switch_database(INFLUX_DATABASE)
    
    # Получение всех данных
    print("Получение всех данных...")
    result = client.query('SELECT * FROM "url_requests"')
    
    points = list(result.get_points())
    print(f"Получено {len(points)} записей")
    
    client.close()
    return points

def get_data_by_date_range(start_date=None, end_date=None):
    """Получает данные за указанный период"""
    print("Подключение к InfluxDB...")
    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT
    )
    
    client.switch_database(INFLUX_DATABASE)
    
    # Формирование условия WHERE
    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE time >= '{start_date}' AND time <= '{end_date}'"
    elif start_date:
        where_clause = f"WHERE time >= '{start_date}'"
    elif end_date:
        where_clause = f"WHERE time <= '{end_date}'"
    
    # Получение данных
    query = f'SELECT * FROM "url_requests" {where_clause}'
    print(f"Выполнение запроса: {query}")
    result = client.query(query)
    
    points = list(result.get_points())
    print(f"Получено {len(points)} записей")
    
    client.close()
    return points

def get_data_by_url(url_filter=None):
    """Получает данные по URL"""
    print("Подключение к InfluxDB...")
    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT
    )
    
    client.switch_database(INFLUX_DATABASE)
    
    # Формирование условия WHERE
    where_clause = ""
    if url_filter:
        where_clause = f"WHERE url = '{url_filter}'"
    
    # Получение данных
    query = f'SELECT * FROM "url_requests" {where_clause}'
    print(f"Выполнение запроса: {query}")
    result = client.query(query)
    
    points = list(result.get_points())
    print(f"Получено {len(points)} записей")
    
    client.close()
    return points

def save_to_csv(points, filename='output.csv'):
    """Сохраняет данные в CSV файл"""
    if not points:
        print("Нет данных для сохранения")
        return
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['time', 'url', 'count'])
        
        for point in points:
            time_str = point.get('time', '')
            url = point.get('url', '')
            count = point.get('count', 0)
            writer.writerow([time_str, url, count])
    
    print(f"Данные сохранены в {filename}")

import argparse

def main():
    parser = argparse.ArgumentParser(description='Получение данных из InfluxDB')
    parser.add_argument('--all', action='store_true', help='Получить все данные')
    parser.add_argument('--start', help='Дата начала (YYYY-MM-DDTHH:MM:SSZ)')
    parser.add_argument('--end', help='Дата окончания (YYYY-MM-DDTHH:MM:SSZ)')
    parser.add_argument('--url', help='Фильтр по URL')
    parser.add_argument('--output', default='output.csv', help='Имя выходного CSV файла')
    
    args = parser.parse_args()
    
    if args.url:
        points = get_data_by_url(args.url)
    elif args.start or args.end:
        points = get_data_by_date_range(args.start, args.end)
    else:
        points = get_all_data()
    
    save_to_csv(points, f"artifacts/{args.output}")

if __name__ == "__main__":
    main()
