from influxdb import InfluxDBClient
import argparse
import os
from dotenv import load_dotenv

# Параметры подключения к InfluxDB
# INFLUX_HOST = 'localhost'
# INFLUX_PORT = 8086
# INFLUX_DATABASE = 'requests_data'

# Загрузка переменных окружения из .env файла
load_dotenv()


INFLUX_HOST = '10.126.145.27'
INFLUX_PORT = 8086
INFLUX_DATABASE = 'requests_data'
INFLUX_USER = os.getenv('INFLUX_USER')
INFLUX_PASSWORD = os.getenv('INFLUX_PASSWORD')
# MEASUREMENT = "url_requests"
MEASUREMENT = "nginx_requests"

def delete_all_data():
    """Удаляет все данные из базы"""
    print("Подключение к InfluxDB...")
    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER,
        password=INFLUX_PASSWORD
    )
    
    client.switch_database(INFLUX_DATABASE)
    
    # Удаление всех данных из measurement (без удаления самой measurement)
    print("Удаление всех данных...")
    client.query(f'DELETE FROM "{MEASUREMENT}"')
    
    print("Все данные удалены!")
    client.close()

def delete_by_date_range(start_date=None, end_date=None):
    """Удаляет данные за указанный период"""
    print("Подключение к InfluxDB...")
    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER,
        password=INFLUX_PASSWORD
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
    
    # Удаление данных
    query = f'DELETE FROM "{MEASUREMENT}" {where_clause}'
    print(f"Выполнение запроса: {query}")
    client.query(query)
    
    print("Данные удалены за указанный период!")
    client.close()

def main():


    parser = argparse.ArgumentParser(description='Удаление данных из InfluxDB')
    parser.add_argument('--all', action='store_true', help='Удалить все данные')
    parser.add_argument('--start', help='Дата начала (YYYY-MM-DDTHH:MM:SSZ)')
    parser.add_argument('--end', help='Дата окончания (YYYY-MM-DDTHH:MM:SSZ)')
    
    args = parser.parse_args()
    
    if args.all:
        delete_all_data()
    elif args.start or args.end:
        delete_by_date_range(args.start, args.end)
    else:
        print("Используйте --all для удаления всех данных или --start/--end для удаления по диапазону")

if __name__ == "__main__":
    main()
