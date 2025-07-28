import pandas as pd
import os

# Переменные для фильтрации
INPUT_FILE = 'artifacts/result_2024-07_2025-07.txt'
START_DATE = '2025-07-01'
END_DATE = '2025-07-31'
TEMPLATE_FILE = 'template/profile_template.xlsx'

def filter_by_date_range(txt_file, start_date=None, end_date=None):
    """Фильтрует данные из txt файла по заданному диапазону дат"""
    print(f"Чтение данных из {txt_file}...")
    
    try:
        df = pd.read_csv(txt_file, on_bad_lines='skip')
    except FileNotFoundError:
        print(f"Ошибка: файл {txt_file} не найден")
        return None
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return None
    
    # Преобразуем колонку hour в datetime
    df['hour'] = pd.to_datetime(df['hour'])
    
    # Фильтрация по диапазону
    if start_date:
        start_dt = pd.to_datetime(start_date)
        df = df[df['hour'] >= start_dt]
    
    if end_date:
        end_dt = pd.to_datetime(end_date)
        df = df[df['hour'] <= end_dt]
    
    print(f"Отфильтровано {len(df)} записей")
    return df

def save_filtered_data(df, output_file):
    """Сохраняет отфильтрованные данные в новый файл"""
    if df is None or df.empty:
        print("Нет данных для сохранения")
        return
    
    df.to_csv(output_file, index=False)
    print(f"Данные сохранены в {output_file}")

def main():
    # Фильтрация данных
    filtered_data = filter_by_date_range(INPUT_FILE, START_DATE, END_DATE)
    
    if filtered_data is not None:
        # Сохранение в Excel файл с заданным именем
        input_base = INPUT_FILE.replace('.txt', '')
        # start_str = START_DATE.replace('-', '') if START_DATE else 'all'
        start_str = START_DATE if START_DATE else 'all'
        # end_str = END_DATE.replace('-', '') if END_DATE else 'all'
        end_str = END_DATE if END_DATE else 'all'
        output_file = f"{input_base}_{start_str}_{end_str}.txt"
    
        if filtered_data is not None and filtered_data.empty:
            return

        # Сохранение в txt файл с тем же форматом таблицы
        new_filename = f"profile_{start_str}_{end_str}.txt"
        destination = os.path.join('artifacts', new_filename)

        with open(destination, 'w', encoding='utf-8') as f:
            # Записываем заголовки
            f.write("OPERATION_CODE,OPERATION_COUNT,DATE,HOUR\n")
            
            # Записываем данные в формате OPERATION_CODE, OPERATION_COUNT, DATE, HOUR
            for _, row in filtered_data.iterrows():
                hour_str = str(row['hour'])
                date_part = hour_str[:10]
                hour_part = hour_str[11:13]
                
                f.write(f"{str(row['url'])},{int(row['request_count'])},{date_part},{hour_part}\n")
        
        print(f"Данные сохранены в txt файл: {destination}")

if __name__ == "__main__":
    main()
