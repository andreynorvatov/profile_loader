import pandas as pd
import os
import re
import pandas as pd

ARTIFACTS_DIR = 'artifacts'

def mask_url(url: str) -> str:
    """Маскирует чувствительные данные в URL одним регулярным выражением."""
    if pd.isna(url):
        return ''
    url = str(url)

    url = re.sub(r'/npa/api/principals/switch-to-user/([^/?]+)', '/npa/api/principals/switch-to-user/{user}', url)

    url = re.sub(r'/npa/api/dashboard/search/fulltext\?text=(\d+)', '/npa/api/dashboard/search/fulltext?text={id}', url)
    url = re.sub(r'documentPackageId=(\d+)', 'documentPackageId={id}', url)
    url = re.sub(r'pointId=(\d+)', 'pointId={id}', url)
    url = re.sub(r'previousPointId=(\d+)', 'previousPointId={id}', url)
    url = re.sub(r'documentTypeId=(\d+)', 'documentTypeId={id}', url)
    url = re.sub(r'agreement-route-user-phase-template/for-document-package/(\d+)', 'agreement-route-user-phase-template/for-document-package/{num}', url)
    url = re.sub(r'/documents/route/(\d+)', '/documents/route/{num}', url)
    url = re.sub(r'track_activity=false&_=(\d+)', 'track_activity=false&_={num}', url)
    url = re.sub(r'agreements/by-document-package/(\d+)', 'agreements/by-document-package/{num}', url)

    url = re.sub(r'tokenId=([^&]+)', 'tokenId={token}', url)
    url = re.sub(r'documentPackageNumber=[^&]+', 'documentPackageNumber={number}', url)
    url = re.sub(r'organizations=[^&]+', 'organizations={numbers_list}', url)
    url = re.sub(r'revisionDate=[^&]+', 'revisionDate={date}', url)
    url = re.sub(r'revisionDate=[^&]+', 'revisionDate={date}', url)
    url = re.sub(r'compareRevisionDate=[^&]+', 'compareRevisionDate={date}', url)
    url = re.sub(r'last_name=[^&]+', 'last_name={data}', url)
    url = re.sub(r'changedSince=[^&]+', 'changedSince={data}', url)
    url = re.sub(r'/dashboard/search/by-document\?payload=[^&]+', '/dashboard/search/by-document?payload={payload}', url)
    url = re.sub(r'dashboard/search/by-parameter\?packageName=[^&]+', 'dashboard/search/by-parameter?packageName={package_name}', url)
    url = re.sub(r'dashboard/search/by-parameter\?registrationNumber=[^&]+', 'dashboard/search/by-parameter?registrationNumber={reg_num}', url)
    url = re.sub(r'dashboard/search/folder\?folderId=[^&]+', 'dashboard/search/folder\folderId={id}', url)

    url = re.sub(r'ticket=ST-([^,]+)', 'ticket={ticket_id}', url)
    url = re.sub(r'criteria=([^,]+)', 'criteria={url_encoded_data}', url)
    url = re.sub(r'return_uri=([^,]+)', 'return_uri={return_uri}', url)
    url = re.sub(r'ext-api/mka2/view\?number=([^,]+)', 'ext-api/mka2/view?number={number}', url)
    url = re.sub(r'/oib/auth-npa/internal/login([^,]+)', '/oib/auth-npa/internal/login{data}', url)
    url = re.sub(r'/aissd-access/login\?service=([^,]+)', '/aissd-access/login?service={service}', url)
    url = re.sub(r'/aissd-access/recovery-password\?service=([^,]+)', '/aissd-access/recovery-password?service={service}', url)
    url = re.sub(r'/main/dashboard\?search=([^,]+)', '/main/dashboard?search={data}', url)
    url = re.sub(r'user-last-activity/ping/([^,]+)', 'user-last-activity/ping/{ping_id}', url)
    url = re.sub(r'JWT-AUTH-TOKEN=([^,]+)', 'JWT-AUTH-TOKEN={token}', url)



    # Общие
    # ISO формате с URL-encoded символами
    url = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}%3A\d{2}%3A\d{2}\.\d{3}%2B\d{2}%3A\d{2}', '{date}', url)
    # Даты в формате 2024-03-24T05:26:18.792Z
    url = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z', '{date}', url)
    # Даты
    url = re.sub(r'\d{4}-\d{2}-\d{2}', '{date}', url)

    # URL-encoded
    url = re.sub(r'%[0-9A-F]{2}(?:%[0-9A-F]{2})+', '{url_encoded_data}', url)

    # UUID и идентификаторы
    url = re.sub(r'[0-9a-fA-F-]{36}', '{uuid}', url)
    url = re.sub(r'(?<=[/_-])\d+(?=[/_-]|$)', '{uuid}', url)

    # Все цифры
    # url = re.sub(r'\d+', '{num}', url)
    
    return url


def filter_requests(df):
    """Фильтрует DataFrame, удаляя строки по заданным условиям."""
    # Удаляем строки, заканчивающиеся на ".js" или ".css", либо начинающиеся с "/socket/sockJs/"
    mask = ~df['request'].str.endswith(('.js', '.css', '.png', '.json', '.woff', '.svg', '.jpg', '.ico')) & ~df['request'].str.startswith('/socket/sockJs/')
    return df[mask]


def prepare_hourly_data(input_file, output_file=None):
    """Агрегирует данные из CSV файла по часам по полям timestamp, request_method, request."""
    
    # Чтение CSV файла
    try:
        # df = pd.read_csv(input_file, engine='python', delimiter=';')
        df = pd.read_csv(input_file, engine='python')
        df = df[['timestamp', 'request_method', 'request']]
    except FileNotFoundError:
        print(f"Ошибка: файл {input_file} не найден")
        return None
    except Exception as e:
        print(f"Ошибка при чтении CSV: {e}")
        return None

    # Применение фильтрации
    df = filter_requests(df)
    
    # Преобразование timestamp в datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Округление до часа
    df['hour'] = df['timestamp'].dt.floor('h')
    
    # Маскирование URL перед группировкой
    df['request'] = df['request'].apply(mask_url)

    # Формирование итогового url как "request_method-request"
    df['url'] = df['request_method'] + '-' + df['request']

    # Группировка по часам и url
    aggregated = df.groupby(['hour', 'url']).size().reset_index(name='count')

    # Сортировка по времени
    aggregated = aggregated.sort_values('hour')

    # Создание директории artifacts если не существует
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

    # Определение имени выходного файла
    if output_file is None:
        csv_base = os.path.splitext(os.path.basename(input_file))[0]
        output_file = f"{ARTIFACTS_DIR}/{csv_base}_hourly_aggregated.txt"

    # Запись агрегированных данных в txt файл
    print("Запись агрегированных данных в файл...")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("hour,url,count\n")
        for _, row in aggregated.iterrows():
            f.write(f"{row['hour']},{row['url']},{row['count']}\n")

    print(f"Агрегированные данные сохранены в: {output_file}")
    return aggregated

def create_influx_points(hourly_data):
    """Создает точки данных для InfluxDB в JSON формате"""
    points = []
    for _, row in hourly_data.iterrows():
        point = {
            "measurement": "nginx_requests",
            "time": row['hour'].isoformat(),
            "tags": {"url": row['url']},
            "fields": {
                "count": int(row['count'])
            }
        }
        points.append(point)
    return points


# CSV_FILE = 'data/2412-cleansed.csv'
# CSV_FILE = 'data/graylog-2025-01-11-2025-01-16.csv'
CSV_FILE = 'data/graylog-2025-01-19-2025-01-27.csv'

if __name__ == "__main__":
    prepare_hourly_data(CSV_FILE)
