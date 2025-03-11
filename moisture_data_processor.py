import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Путь к файлу
input_file = Path('./input/moisture_data.csv')
output_file = Path('./output/moisture_data_processed.csv')

# Загрузка данных
df = pd.read_csv(input_file)

# Удаляем ненужные колонки
df = df.drop(columns=['result_id', 'created_at', 'status', 'processed_at', 'processed_by'], errors='ignore')

# Обработка временной метки (timestamp)
def process_timestamp(ts_str):
    try:
        # Удаляем миллисекунды
        ts_str = ts_str.split('.')[0]
        # Парсим как UTC и добавляем 2 часа (местное время)
        ts_utc = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
        ts_local = ts_utc + timedelta(hours=2)  # Ваш часовой пояс +2
        return {
            'Date': ts_local.strftime('%d-%m-%Y'),
            'Time': ts_local.strftime('%H:%M:%S')
        }
    except:
        return {'Date': '', 'Time': ''}

# Применяем обработку к столбцу timestamp
df['DateTime'] = df['timestamp'].apply(lambda x: process_timestamp(x))
df['Date'] = df['DateTime'].apply(lambda x: x['Date'])
df['Time'] = df['DateTime'].apply(lambda x: x['Time'])
df = df.drop(columns=['timestamp', 'DateTime'])

# Переименовываем колонки
df = df.rename(columns={
    'grain_type': 'Grain',
    'moisture': 'Moisture',
    'temperature': 'Temperature',
    'nature': 'Nature'
})

# Сортировка по времени
df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S')
df = df.sort_values(by='DateTime')

# Упорядочиваем колонки: Date и Time в начале
columns_order = ['Date', 'Time'] + [col for col in df.columns if col not in ['Date', 'Time']]
df = df[columns_order]

# Удаляем временной столбец DateTime
df = df.drop(columns=['DateTime'])

# Сохранение результата
df.to_csv(output_file, index=False)
print(f"Данные обработаны и сохранены в {output_file}")