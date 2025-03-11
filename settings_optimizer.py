import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')
settings_file = input_dir / 'settings.csv'
output_file = output_dir / 'settings_optimized.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
settings_df = pd.read_csv(settings_file)

# Преобразование времени в datetime
def parse_time(row):
    return pd.to_datetime(row['Date'] + ' ' + row['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

settings_df['DateTime'] = settings_df.apply(parse_time, axis=1)

# Очищаем данные от некорректных временных меток
settings_df = settings_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)

# Группируем данные по дате и минуте
settings_df['MinuteKey'] = settings_df['DateTime'].dt.strftime('%d-%m-%Y %H:%M')  # Ключ для группировки (до минуты)
grouped = settings_df.groupby('MinuteKey', group_keys=False)

# Функция для объединения данных в группе
def combine_group(group):
    combined_row = {}
    for col in group.columns:
        if col in ['DateTime', 'MinuteKey']:
            continue  # Пропускаем служебные колонки
        
        # Для UPPER_FAN_SET_HZ и LOWER_FAN_SET_HZ делим на 100 с округлением до 1 знака после запятой
        if col in ['UPPER_FAN_SET_HZ', 'LOWER_FAN_SET_HZ']:
            non_empty_values = group[col].dropna()
            if not non_empty_values.empty:
                combined_row[col] = round(non_empty_values.iloc[0] / 100, 1)  # Первое непустое значение
            else:
                combined_row[col] = ''
        
        # Для остальных колонок берем первое непустое значение
        else:
            non_empty_values = group[col].dropna()
            combined_row[col] = non_empty_values.iloc[0] if not non_empty_values.empty else ''
    
    # Добавляем MinuteKey для последующего использования
    combined_row['MinuteKey'] = group['MinuteKey'].iloc[0]
    return pd.Series(combined_row)

# Применяем функцию к каждой группе
optimized_df = grouped.apply(combine_group).reset_index(drop=True)

# Восстанавливаем Date и Time из MinuteKey
optimized_df['Date'] = optimized_df['MinuteKey'].str[:10]
optimized_df['Time'] = optimized_df['MinuteKey'].str[11:] + ':00'  # Добавляем секунды ":00"

# Удаляем временные колонки
optimized_df = optimized_df.drop(columns=['MinuteKey'])

# Сохраняем результат
optimized_df.to_csv(output_file, index=False)
print(f"Оптимизированные данные сохранены в {output_file}")