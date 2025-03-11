import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')
alarms_file = input_dir / 'alarms.csv'
output_file = output_dir / 'alarms_optimized.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
alarms_df = pd.read_csv(alarms_file)

# Преобразование времени в datetime
alarms_df['DateTime'] = pd.to_datetime(
    alarms_df['Date'] + ' ' + alarms_df['Time'],
    format='%d-%m-%Y %H:%M:%S',
    errors='coerce'
)

# Очищаем данные от некорректных временных меток
alarms_df = alarms_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)

# Группируем данные по дате и минуте
alarms_df['MinuteKey'] = alarms_df['DateTime'].dt.strftime('%d-%m-%Y %H:%M')
grouped = alarms_df.groupby('MinuteKey', group_keys=False)

# Функция для объединения данных в группе
def combine_group(group):
    combined_row = {
        'Date': group['MinuteKey'].iloc[0][:10],  # Извлекаем дату из MinuteKey
        'Time': group['MinuteKey'].iloc[0][11:] + ':00',  # Извлекаем время и округляем до минуты
        'DROPS_SCORE': group['DROPS_SCORE'].dropna().iloc[0] if not group['DROPS_SCORE'].isna().all() else ''
    }
    
    # Обрабатываем аварийные сигналы
    for col in ['MIDDLE_LEVEL_ALARM', 'HIGH_LEVEL_ALARM', 'BURNER_HIGH_ALARM', 
               'HOPPER_FULL_ALARM', 'LOW_AIR_PRESSURE_ALARM', 'GENERAL_ALARM', 'AIR_OVERHEATED']:
        combined_row[col] = 'true' if group[col].any() else 'false'
    
    return pd.Series(combined_row)

# Применяем функцию к каждой группе
optimized_df = grouped.apply(combine_group).reset_index(drop=True)

# Упорядочиваем колонки: Date и Time в начале
columns_order = ['Date', 'Time', 'DROPS_SCORE'] + [
    'MIDDLE_LEVEL_ALARM', 'HIGH_LEVEL_ALARM', 'BURNER_HIGH_ALARM',
    'HOPPER_FULL_ALARM', 'LOW_AIR_PRESSURE_ALARM', 'GENERAL_ALARM', 'AIR_OVERHEATED'
]
optimized_df = optimized_df[columns_order]

# Сохраняем результат
optimized_df.to_csv(output_file, index=False)
print(f"Оптимизированные данные сохранены в {output_file}")