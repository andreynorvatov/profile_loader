# from prepare_data import prepare_hourly_data, create_influx_points, CSV_FILE
from nginx_old_aggregate import prepare_hourly_data, create_influx_points, CSV_FILE
from nginx_new_aggregate import prepare_hourly_data, create_influx_points, CSV_FILE

from influx_client import InfluxDBClientWrapper

def main():
    # Подготовка данных
    hourly_data = prepare_hourly_data(CSV_FILE)
    if hourly_data is None:
        return
    
    # Отправка данных в InfluxDB
    with InfluxDBClientWrapper() as client:
        points = create_influx_points(hourly_data)
        client.write_points(points)
        print("Загрузка данных завершена!")

if __name__ == "__main__":
    main()

