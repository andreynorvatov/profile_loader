import pandas as pd
import os
from datetime import datetime

def aggregate_hourly_data(input_file='data/2412-cleansed.csv', output_file='artifacts/2412_hourly_aggregated.csv'):
    """Агрегирует данные из CSV файла по часам по полям timestamp, request_method, request."""
    
    # Чтение CSV файла
    df = pd.read_csv(input_file, usecols=['timestamp', 'request_method', 'request'])
    
    # Преобразование timestamp в datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Округление до часа
    df['hour'] = df['timestamp'].dt.floor('H')
    
    # Группировка по часам, request_method и request
    aggregated = df.groupby(['hour', 'request_method', 'request']).size().reset_index(name='count')
    
    # Сортировка по времени
    aggregated = aggregated.sort_values('hour')
    
    # Создание директории artifacts если не существует
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Сохранение в CSV
    aggregated.to_csv(output_file, index=False)
    print(f"Агрегированные данные сохранены в: {output_file}")
    
    return aggregated

if __name__ == "__main__":
    aggregate_hourly_data()
