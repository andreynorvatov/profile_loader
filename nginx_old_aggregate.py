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
    url = re.sub(r'sending/by-document-package-id/([^/?]+)', 'sending/by-document-package-id/{id}', url)

    url = re.sub(r'documentPackageId=(\d+)', 'documentPackageId={id}', url)
    url = re.sub(r'pointId=(\d+)', 'pointId={id}', url)
    url = re.sub(r'orgId=(\d+)', 'orgId={id}', url)
    url = re.sub(r'id=(\d+)', 'id={id}', url)
    url = re.sub(r'previousPointId=(\d+)', 'previousPointId={id}', url)
    url = re.sub(r'documentTypeId=(\d+)', 'documentTypeId={id}', url)
    url = re.sub(r'agreement-route-user-phase-template/for-document-package/(\d+)', 'agreement-route-user-phase-template/for-document-package/{num}', url)
    url = re.sub(r'/documents/route/(\d+)', '/documents/route/{num}', url)
    url = re.sub(r'track_activity=false&_=(\d+)', 'track_activity=false&_={num}', url)
    url = re.sub(r'document-package/(\d+)', 'document-package/{num}', url)
    url = re.sub(r'position_id=(\d+)', 'position_id={id}', url)
    url = re.sub(r'organisation_id=(\d+)', 'organisation_id={id}', url)
    url = re.sub(r'organizationId=(\d+)', 'organizationId={id}', url)
    url = re.sub(r'employeeId=(\d+)', 'employeeId={id}', url)
    url = re.sub(r'userId=(\d+)', 'userId={id}', url)
    url = re.sub(r'version=(\d+)', 'version={num}', url)
    # url = re.sub(r'user-send-activity&_=(\d+)', 'user-send-activity&_={num}', url)
    url = re.sub(r'&_=(\d+)', '&_={num}', url)
    url = re.sub(r'folderId=(\d+)', 'folderId={num}', url)
    url = re.sub(r'document_package_id=(\d+)', 'document_package_id={num}', url)
    url = re.sub(r'registration-date/open-date\?projectType=(\d+)', 'registration-date/open-date?projectType={num}', url)
    url = re.sub(r'docIds=(\d+)', 'docIds={num}', url)
    url = re.sub(r'phaseTypeId=(\d+)', 'phaseTypeId={num}', url)
    url = re.sub(r't=(\d+)', 't={num}', url)


    url = re.sub(r'tokenId=([^&]+)', 'tokenId={token}', url)
    url = re.sub(r'/npa/api/dashboard/search/fulltext\?text=([^&]+)', '/npa/api/dashboard/search/fulltext?text={id}', url)
    url = re.sub(r'documentPackageNumber=[^&]+', 'documentPackageNumber={num}', url)
    url = re.sub(r'packageNumber=[^&]+', 'packageNumber={num}', url)
    url = re.sub(r'organizations=[^&]+', 'organizations={numbers_list}', url)
    url = re.sub(r'revisionDate=[^&]+', 'revisionDate={date}', url)
    url = re.sub(r'revisionDate=[^&]+', 'revisionDate={date}', url)
    url = re.sub(r'compareRevisionDate=[^&]+', 'compareRevisionDate={date}', url)
    url = re.sub(r'first_name=[^&]+', 'first_name={data}', url)
    url = re.sub(r'last_name=[^&]+', 'last_name={data}', url)
    url = re.sub(r'firstName=[^&]+', 'firstName={data}', url)
    url = re.sub(r'lastName=[^&]+', 'lastName={data}', url)
    url = re.sub(r'middleName=[^&]+', 'middleName={data}', url)
    url = re.sub(r'changedSince=[^&]+', 'changedSince={data}', url)
    # url = re.sub(r'/dashboard/search/by-document\?payload=[^&]+', '/dashboard/search/by-document?payload={payload}', url)
    url = re.sub(r'payload=[^&]+', 'payload={payload}', url)
    url = re.sub(r'dashboard/search/by-parameter\?packageName=[^&]+', 'dashboard/search/by-parameter?packageName={name}', url)
    url = re.sub(r'dashboard/search/by-parameter\?attachmentName=[^&]+', 'dashboard/search/by-parameter?attachmentName={name}', url)
    # url = re.sub(r'dashboard/search/by-parameter\?registrationNumber=[^&]+', 'dashboard/search/by-parameter?registrationNumber={num}', url)
    url = re.sub(r'registrationNumber=[^&]+', 'registrationNumber={num}', url)
    url = re.sub(r'packageDescription=[^&]+', 'packageDescription={name}', url)
    # url = re.sub(r'dashboard/search/folder\?folderId=[^&]+', 'dashboard/search/folder/folderId={id}', url)
    url = re.sub(r'type=governmentResolution&stage=negotiation&name=[^&]+', 'type=governmentResolution&stage=negotiation&name={name}', url)
    url = re.sub(r'document/find\?name=[^&]+', 'document/find?name={name}', url)
    url = re.sub(r'searchParam=[^&]+', 'searchParam={name}', url)
    url = re.sub(r'position_name=[^&]+', 'position_name={name}', url)
    url = re.sub(r'/document/find?payload=[^&]+', '/document/find?payload={data}', url)
    url = re.sub(r'/aissd-access/change-password\?service=[^&]+', '/aissd-access/change-password?service={data}', url)
    url = re.sub(r'startDate=[^&]+', 'startDate={date}', url)
    url = re.sub(r'endDate=[^&]+', 'endDate={date}', url)
    url = re.sub(r'TARGET=[^&]+', 'TARGET={url}', url)
    url = re.sub(r'parent=[^&]+', 'parent={id}', url)

    url = re.sub(r'ticket=ST-([^,]+)', 'ticket={id}', url)
    url = re.sub(r'criteria=([^,]+)', 'criteria={url_encoded_data}', url)
    url = re.sub(r'return_uri=([^,]+)', 'return_uri={return_uri}', url)
    url = re.sub(r'ext-api/mka2/view\?number=([^,]+)', 'ext-api/mka2/view?number={number}', url)
    url = re.sub(r'/oib/auth-npa/internal/login([^,]+)', '/oib/auth-npa/internal/login{data}', url)
    url = re.sub(r'/aissd-access/login\?service=([^,]+)', '/aissd-access/login?service={data}', url)
    url = re.sub(r'/aissd-access/recovery-password\?service=([^,]+)', '/aissd-access/recovery-password?service={service}', url)
    url = re.sub(r'/main/dashboard\?search=([^,]+)', '/main/dashboard?search={data}', url)
    url = re.sub(r'user-last-activity/ping/([^,]+)', 'user-last-activity/ping/{ping_id}', url)
    url = re.sub(r'JWT-AUTH-TOKEN=([^,]+)', 'JWT-AUTH-TOKEN={token}', url)
    url = re.sub(r'storageId=internal&service=([^,]+)', 'storageId=internal&service={data}', url)
    url = re.sub(r'searchText=([^,]+)', 'searchText={data}', url)

    url = re.sub(r'(?:[?&]name=)([^&]+)', '&name={data}', url)
    url = re.sub(r'Select3/([^?]+)', 'Select3/{data}', url)


    # Общие
    # ISO формате с URL-encoded символами
    url = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}%3A\d{2}%3A\d{2}\.\d{3}%2B\d{2}%3A\d{2}', '{date}', url)
    # Даты в формате 2024-03-24T05:26:18.792Z
    url = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z', '{date}', url)
    # Даты
    url = re.sub(r'\d{4}-\d{2}-\d{2}', '{date}', url)

    # URL-encoded
    # url = re.sub(r'%[0-9A-F]{2}(?:%[0-9A-F]{2})+', '{url_encoded_data}', url)

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
