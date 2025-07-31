import os
import glob
import pandas as pd


def read_scenario_data(scenario_dir='data/scenario'):
    """
    Читает все CSV-файлы из указанной директории и объединяет их в один DataFrame.
    
    Args:
        scenario_dir (str): Путь к директории с файлами сценариев
        
    Returns:
        pd.DataFrame: Объединенный DataFrame со всеми данными из файлов
    """
    # Проверяем существование директории
    if not os.path.exists(scenario_dir):
        raise FileNotFoundError(f"Директория {scenario_dir} не найдена")
    
    # Ищем все CSV файлы в директории
    csv_files = glob.glob(os.path.join(scenario_dir, "*.csv"))
    
    if not csv_files:
        print(f"В директории {scenario_dir} не найдено CSV файлов")
        return pd.DataFrame()
    
    print(f"Найдено {len(csv_files)} файлов для обработки")
    
    # Читаем и объединяем все файлы
    dataframes = []
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, low_memory=False)
            df['source_file'] = os.path.basename(file_path)  # Добавляем информацию о файле-источнике
            dataframes.append(df)
            print(f"Обработан файл: {os.path.basename(file_path)} ({len(df)} записей)")
        except Exception as e:
            print(f"Ошибка при чтении файла {file_path}: {e}")
    
    if not dataframes:
        return pd.DataFrame()
    
    # Объединяем все DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"\nВсего объединено записей: {len(combined_df)}")
    
    return combined_df


def main():
    """Тестовая функция для проверки работы скрипта."""
    df = read_scenario_data()
    if not df.empty:
        print("\nПервые 5 строк объединенных данных:")
        print(df.head())
        print("\nИнформация о колонках:")
        print(df.info())
        print("\nКоличество записей по файлам:")
        print(df['source_file'].value_counts())


if __name__ == '__main__':
    main()
