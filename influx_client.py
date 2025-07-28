from influxdb import InfluxDBClient

# Параметры подключения к InfluxDB
INFLUX_HOST = 'localhost'
INFLUX_PORT = 8086
# INFLUX_USER = 'admin'
# INFLUX_PASSWORD = 'admin'
INFLUX_DATABASE = 'requests_data'

class InfluxDBClientWrapper:
    def __init__(self, host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DATABASE):
        self.host = host
        self.port = port
        self.database = database
        self.client = None
    
    def connect(self):
        """Подключение к InfluxDB"""
        print("Подключение к InfluxDB...")
        self.client = InfluxDBClient(
            host=self.host,
            port=self.port,
            # username=INFLUX_USER,
            # password=INFLUX_PASSWORD
        )
        
        # # Создание базы данных если не существует
        # databases = self.client.get_list_database()
        # if not any(db['name'] == self.database for db in databases):
        #     self.client.create_database(self.database)
        #     print(f"Создана база данных {self.database}")
        
        self.client.switch_database(self.database)
    
    def write_points(self, points):
        """Запись точек данных в InfluxDB"""
        if not self.client:
            raise RuntimeError("Сначала подключитесь к InfluxDB")
        
        print(f"Запись {len(points)} точек данных...")
        self.client.write_points(points)
    
    def close(self):
        """Закрытие соединения с InfluxDB"""
        if self.client:
            self.client.close()
            print("Соединение с InfluxDB закрыто")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
