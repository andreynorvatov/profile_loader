import pandas as pd
import os
import re

ARTIFACTS_DIR = 'artifacts'

def mask_url(url):
    """Маскирует чувствительные данные в URL
    
    Заменяет:
    - UUID и идентификаторы на ### только в URL содержащих 'oiv'
    - Параметры с числовыми значениями на ###
    - Даты в формате 2024-01-01 на ####-##-##
    """
    # Маскируем UUID в пути только если URL содержит 'oiv' (включая в регулярное выражение)
    url = re.sub(r'(?=.*oiv).*/[0-9a-fA-F-]{8,}/', '/###/', url)
    
    # Всегда маскируем числовые параметры и даты
    url = re.sub(r'\d{4}-\d{2}-\d{2}', '####-##-##', url)
    url = re.sub(r'([?&][^=]+=)\d+', r'\1###', url)

    # Маскируем короткие UUID/идентификаторы из 8-16 hex-символов
    url = re.sub(r'(?<![0-9a-fA-F])[0-9a-fA-F]{8,16}(?![0-9a-fA-F])', '_####', url)

    # Маскируем хэши после # в URL вида aissd.mos.ru/socket/sockJs/iframe.html#
    url = re.sub(r'(aissd\.mos\.ru/socket/sockJs/iframe\.html#)[a-zA-Z0-9-]+', r'\1_####', url)

    # # Маскируем параметры в /oib/auth-npa/internal/login?12= на ?##=
    # url = re.sub(r'(/oib/auth-npa/internal/login\?)\d+(=)', r'\1##\2', url)

    # Маскируем все числовые значения на #
    url = re.sub(r'\d+', '#', url)

    return url

def aggregate_by_hour(data):
    """Агрегирует данные по часу для каждого URL"""
    # Используем колонки из CSV: server_time и url_clear
    data['server_time'] = pd.to_datetime(data['server_time'])
    data['hour'] = data['server_time'].dt.floor('h')
    
    # Маскирование URL перед группировкой
    data['url_clear'] = data['url_clear'].apply(mask_url)
    
    # Группировка по часу и URL
    hourly_stats = data.groupby(['hour', 'url_clear']).size().reset_index(name='request_count')
    hourly_stats = hourly_stats.rename(columns={'url_clear': 'url'})
    return hourly_stats

def prepare_hourly_data(csv_file):
    """Подготавливает агрегированные данные из CSV файла"""
    print(f"Чтение данных из {csv_file}...")
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Ошибка: файл {csv_file} не найден")
        return None
    except Exception as e:
        print(f"Ошибка при чтении CSV: {e}")
        return None
    
    # Проверка наличия необходимых колонок
    required_columns = ['server_time', 'url_clear']
    if not all(col in df.columns for col in required_columns):
        print(f"Ошибка: CSV должен содержать колонки: {required_columns}")
        return None
    
    # Агрегация данных
    print("Агрегация данных по часу...")
    hourly_data = aggregate_by_hour(df)
    
    # Запись агрегированных данных в txt файл
    print("Запись агрегированных данных в файл...")
    csv_base = os.path.splitext(os.path.basename(csv_file))[0]
    txt_file = f"{ARTIFACTS_DIR}/{csv_base}.txt"
    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write("hour,url,request_count\n")
        for _, row in hourly_data.iterrows():
            f.write(f"{row['hour']},{row['url']},{row['request_count']}\n")
    
    return hourly_data

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


# Путь к CSV файлу по умолчанию
CSV_FILE = 'data/result_2024-07_2025-07.csv'

def main():
    """Точка входа для запуска скрипта напрямую"""
    prepare_hourly_data(CSV_FILE)


if __name__ == "__main__":
    main()


