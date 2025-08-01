import pandas as pd
import re
from nginx_old_aggregate import mask_url


def read_csv_file(file_path):
    """Читает CSV файл и возвращает DataFrame с колонкой source."""
    df = pd.read_csv(file_path, low_memory=False)
    return df

def filter_sources(df):
    """Убирает строки с указанными значениями в колонке source."""
    mask = ~df['source'].isin([
        "fluent-bit-ds-5gsqx",
        "fluent-bit-ds-9ljht",
        "fluent-bit-ds-d2pc6",
        "fluent-bit-ds-g9vk6",
        "fluent-bit-ds-j2fpl",
        "fluent-bit-ds-lwwvs",
        "fluent-bit-ds-r9mql",
        "p.aissd.mos.ru",
        "rancher.aissd.mos.ru",

    ])
    return df[mask]

def filter_by_keywords(df, keywords):
    """
    Возвращает строки, в которых request_uri содержит хотя бы одно из указанных ключевых слов.
    Добавляет колонку 'filter_matched' с найденным ключевым словом.
    keywords: список строк или одна строка
    """
    if isinstance(keywords, str):
        keywords = [keywords]
    pattern = '|'.join(keywords)
    mask = df['request_uri'].str.contains(pattern, na=False, case=False)
    df_filtered = df[mask].copy()
    df_filtered['filter_matched'] = df_filtered['request_uri'].str.extract(
        f'({"|".join(keywords)})', flags=re.IGNORECASE, expand=False
    )
    return df_filtered


def filter_requests(df):
    """Фильтрует DataFrame, удаляя строки по заданным условиям."""
    # Удаляем строки, заканчивающиеся на ".js" или ".css", либо начинающиеся с "/socket/sockJs/"
    mask = (
        ~df['request_uri'].astype(str).str.endswith(('.js', '.css', '.png', '.json', '.woff', '.svg', '.jpg', '.ico', '/websocket')) &
        ~df['request_uri'].astype(str).str.startswith('/socket/sockJs/')
    )
    return df[mask]


def prepare_hourly_data(input_file, output_file=None):
    """Агрегирует данные из CSV файла по часам по полям timestamp, request_method, request_uri, status."""
    df = read_csv_file(input_file)
    df = filter_sources(df)
    df = filter_requests(df)
    return prepare_hourly_data_from_df(df, output_file)

def prepare_hourly_data_from_df(df, output_file=None):
    """Агрегирует данные из DataFrame по часам по полям timestamp, request_method, request_uri, status."""
    # Копируем, чтобы не изменять исходный DataFrame
    df_agg = df[['timestamp', 'request_method', 'request_uri', 'status']].copy()
    # Маскируем request_uri
    df_agg['masked_uri'] = df_agg['request_uri'].apply(mask_url)
    # Создаем колонку url в формате request_method-masked_uri
    df_agg['url'] = df_agg['request_method'] + '-' + df_agg['masked_uri']
    # Приводим timestamp к datetime и округляем до часа
    df_agg['timestamp'] = pd.to_datetime(df_agg['timestamp'], errors='coerce')
    df_agg['hour'] = df_agg['timestamp'].dt.floor('h')
    # Группируем и считаем количество
    aggregated = (
        df_agg
        .groupby(['hour', 'url', 'status'])
        .size()
        .reset_index(name='count')
    )
    aggregated['status'] = aggregated['status'].astype('Int64')

    if output_file:
        aggregated.to_csv(output_file, sep=';', decimal=',', index=False)
        print(f"Агрегированные данные сохранены в: {output_file}")

    return aggregated

def count_unique_sources(df):
    """Возвращает таблицу с уникальными значениями и их количеством."""
    return df['source'].value_counts()

def create_influx_points(hourly_data):
    pass


CSV_FILE = 'data/nginx_row_data.csv'

def main():
    # Шаг 1: Чтение и подсчет уникальных значений source
    df = read_csv_file(CSV_FILE)
    sources_table = count_unique_sources(df)
    print("Таблица уникальных значений source и их количество (до фильтрации):")
    print(sources_table)

    # Шаг 2: Фильтрация по источнику (sources)
    df_filtered = filter_sources(df)
    sources_table_filtered = count_unique_sources(df_filtered)
    print("\nТаблица уникальных значений source и их количество (после фильтрации):")
    print(sources_table_filtered)

    # Шаг 3: Фильтрация по запросам (requests)
    df_filtered = filter_requests(df_filtered)

    # Шаг 4: Агрегация по часу с предварительно отфильтрованными данными
    df_hourly = prepare_hourly_data_from_df(df_filtered, 'artifacts/hourly_aggregated.csv')

    # Шаг 5: Сбор уникальных URL и их количества запросов
    url_counts = (
        df_hourly.groupby('url')['count']
        .sum()
        .reset_index(name='total_requests')
        .sort_values(by='url')
    )
    url_counts.to_csv(
        'artifacts/unique_urls.csv',
        sep=';',
        decimal=',',
        index=False
    )
    print("\nУникальные URL и количество запросов сохранены в artifacts/unique_urls.csv")

    # Запись итогового DataFrame в Excel
    url_counts.to_excel(
        'artifacts/unique_urls.xlsx',
        sheet_name='unique_urls',
        index=False
    )
    print("\nИтоговый DataFrame сохранён в Excel: artifacts/unique_urls.xlsx")

    # Шаг 6: Сбор статистики по Import и Export (для определения размера файлов)
    df_import = filter_by_keywords(df, ['import', 'export'])
    print("\nТаблица import/export (только строки с 'import' или 'export' в request_uri) - Done")
    # Явно задаем тип decimal для указанных колонок
    decimal_cols = ['upstream_connect_time', 'upstream_response_time', 'request_time']
    for col in decimal_cols:
        if col in df_import.columns:
            df_import[col] = pd.to_numeric(df_import[col], errors='coerce')
    # Записываем результат в CSV с разделителем ; и десятичной запятой
    df_import.to_csv(
        'artifacts/sources_table_import_export.csv',
        sep=';',
        decimal=',',
        index=False
    )

if __name__ == '__main__':
    main()

