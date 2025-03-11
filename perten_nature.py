import pandas as pd
from pathlib import Path

# Путь к файлу
input_file = Path('./input/perten_data.csv')
output_file = Path('./output/processed_perten_data.csv')

# Загрузка данных
df = pd.read_csv(input_file)

# Переименование колонок с добавлением префикса perten_
rename_columns = {
    'Grain': 'perten_Grain',
    '%mois': 'perten_Moisture',
    'TW': 'perten_Nature',
    'Temp': 'perten_Temperature'
}
df = df.rename(columns=rename_columns)

# Масштабирование и округление значений Nature (TW)
df['perten_Nature'] = (df['perten_Nature'] / 100).round(4)

# Приведение даты и времени к формату без миллисекунд
df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
df = df.dropna(subset=['DateTime'])  # Удаляем некорректные временные метки
df['Date'] = df['DateTime'].dt.strftime('%d-%m-%Y')
df['Time'] = df['DateTime'].dt.strftime('%H:%M:%S')
df = df.drop(columns=['DateTime'])

# Сортировка по времени
df = df.sort_values(by=['Date', 'Time'])

# Сохранение результата
df.to_csv(output_file, index=False)
print(f"Данные обработаны и сохранены в {output_file}")