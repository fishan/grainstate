import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')
perten_file = input_dir / 'perten_data_shifted.csv'
output_file = output_dir / 'perten_data_classified.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
perten_df = pd.read_csv(perten_file)

# Функция для определения статуса зерна
def classify_grain(row):
    grain = row['Grain'].lower() if isinstance(row['Grain'], str) else ''
    moisture = row['%mois']
    
    if grain == 'raps':
        if moisture > 9.5:
            return 'wet'
        elif 7 <= moisture <= 9.5:
            return 'dry'
        else:
            return 'overdry'  # Влажность <7% → пересушено
    else:
        if moisture > 14.5:
            return 'wet'
        elif 12 <= moisture <= 14.5:
            return 'dry'
        else:
            return 'overdry'  # Влажность <12% → пересушено

# Применяем функцию к данным
perten_df['Grain_Status'] = perten_df.apply(classify_grain, axis=1)

# Перемещаем колонки Date и Time в начало
columns_order = ['Date', 'Time', 'Grain', '%mois', 'TW', 'Temp', 'Grain_Status']
perten_df = perten_df[columns_order]

# Сохраняем результат
perten_df.to_csv(output_file, index=False)
print(f"Данные с классификацией сохранены в {output_file}")