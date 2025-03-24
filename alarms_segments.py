import pandas as pd
from pathlib import Path
from datetime import timedelta, datetime

# Пути к файлам
input_dir = Path('final_data')
alarms_file = input_dir / 'alarms_optimized.csv'
alarms_segments_file = input_dir / 'alarms_segments.csv'

# Читаем данные
alarms_df = pd.read_csv(alarms_file)
alarms_df['DateTime'] = pd.to_datetime(alarms_df['Date'] + ' ' + alarms_df['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce') + timedelta(hours=3)
alarms_df = alarms_df.dropna(subset=['DateTime'])

# Преобразуем 'true'/'false' в 1/0
alarm_cols = ['MIDDLE_LEVEL_ALARM', 'HIGH_LEVEL_ALARM', 'BURNER_HIGH_ALARM', 'HOPPER_FULL_ALARM', 
              'LOW_AIR_PRESSURE_ALARM', 'GENERAL_ALARM', 'AIR_OVERHEATED']
for col in alarm_cols:
    alarms_df[col] = alarms_df[col].map({'true': 1, 'false': 0}).fillna(0).astype(int)

# Функция для создания отрезков
def process_alarms(df):
    segments = []
    
    for col in alarm_cols:
        active = False
        start_time = None
        prev_time = None
        
        for index, row in df.iterrows():
            if row[col] == 1 and not active:
                start_time = row['DateTime']
                active = True
            elif row[col] == 0 and active:
                end_time = prev_time if prev_time else row['DateTime']
                duration = (end_time - start_time).total_seconds() / 60  # в минутах
                segments.append({'Alarm_Type': col, 'Start': start_time, 'End': end_time, 'Duration': duration})
                active = False
            prev_time = row['DateTime']
        
        if active:  # Если аларм не закончился
            end_time = df['DateTime'].iloc[-1]
            duration = (end_time - start_time).total_seconds() / 60
            segments.append({'Alarm_Type': col, 'Start': start_time, 'End': end_time, 'Duration': duration})
    
    return pd.DataFrame(segments)

# Создаём таблицу сегментов
alarms_segments = process_alarms(alarms_df)

# Сохраняем в CSV
if not alarms_segments.empty:
    alarms_segments.to_csv(alarms_segments_file, index=False)
    print(f"Сегменты алармов сохранены в {alarms_segments_file}")
    print(alarms_segments.head())
else:
    print("Нет данных для создания сегментов алармов.")

# Пример вывода первых 5 строк
print(f"Первые 5 строк из {alarms_segments_file}:")
print(alarms_segments.head().to_string())