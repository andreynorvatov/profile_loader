import os
import pandas as pd
from nginx_old_aggregate import mask_url

INPUT_FILE_COMBINED = 'artifacts/scenario_combined_h.xlsx'
EXTRACTED_FILE = 'artifacts/scenario_extracted_data.xlsx'
MASKED_FILE = 'artifacts/scenario_masked.xlsx'


def extract_data_sheet_to_new_file(input_file=INPUT_FILE_COMBINED, output_file=EXTRACTED_FILE):
    """
    Читает лист 'data' из файла scenario_combined_h.xlsx и сохраняет его в новый Excel файл.
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Файл {input_file} не найден.")

    df = pd.read_excel(input_file, sheet_name='data')

    output_path = output_file
    df.to_excel(output_path, index=False)
    print(f"Данные из листа 'data' сохранены в {output_path}")

def mask_requests_column(input_file=EXTRACTED_FILE,
                         output_file=MASKED_FILE,
                         column='Запрос'):
    """
    Читает Excel-файл, применяет mask_url к указанной колонке
    и сохраняет результат в новый файл.
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Файл {input_file} не найден.")

    df = pd.read_excel(input_file)
    if column not in df.columns:
        raise ValueError(f"Колонка '{column}' не найдена в файле.")

    df['masked_url'] = df[column].astype(str).apply(mask_url)
    df.to_excel(output_file, index=False)
    print(f"Файл с маскированными URL сохранён: {output_file}")

if __name__ == "__main__":
    df = pd.read_excel(INPUT_FILE_COMBINED, sheet_name='data')
    df['masked_url'] = df['Запрос'].astype(str).apply(mask_url)
    df.to_excel(MASKED_FILE, index=False)
    print(f"Итоговый файл с masked_url сохранён: {MASKED_FILE}")
