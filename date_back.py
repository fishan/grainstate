import pandas as pd
from pathlib import Path
from datetime import timedelta

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')
perten_file = input_dir / 'perten_data_v1.csv'
output_file = output_dir / 'perten_data_shifted.csv'

# Создаем выходную директорию 
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
perten_df = pd.read_csv(perten_file)

# Преобразование времени в datetime
def parse_time(row):
    return pd.to_datetime(row['Date'] + ' ' + row['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

perten_df['DateTime'] = perten_df.apply(parse_time, axis=1)

# Сдвиг времени на 10667 секунд назад
time_shift = timedelta(seconds=10667)
perten_df['DateTime'] = perten_df['DateTime'] - time_shift

# Разделяем DateTime обратно на Date и Time
perten_df['Date'] = perten_df['DateTime'].dt.strftime('%d-%m-%Y')
perten_df['Time'] = perten_df['DateTime'].dt.strftime('%H:%M:%S')

# Удаляем временной столбец
perten_df = perten_df.drop(columns=['DateTime'])

# Сохраняем результат
perten_df.to_csv(output_file, index=False)
print(f"Сдвинутые данные сохранены в {output_file}")