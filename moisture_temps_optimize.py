import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')
moistures_file = input_dir / 'moistures_temps.csv'
output_file = output_dir / 'moistures_temps_optimized.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
moistures_df = pd.read_csv(moistures_file)

# Преобразование времени в datetime
def parse_time(row):
    return pd.to_datetime(row['Date'] + ' ' + row['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

moistures_df['DateTime'] = moistures_df.apply(parse_time, axis=1)

# Очищаем данные от некорректных временных меток
moistures_df = moistures_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)

# Группируем данные по дате и минуте
moistures_df['MinuteKey'] = moistures_df['DateTime'].dt.strftime('%d-%m-%Y %H:%M')  # Ключ для группировки (до минуты)
grouped = moistures_df.groupby('MinuteKey', group_keys=False)  # group_keys=False устраняет предупреждение

# Функция для объединения данных в группе
def combine_group(group):
    combined_row = {}
    for col in group.columns:
        if col in ['DateTime', 'MinuteKey']:
            continue  # Пропускаем служебные колонки
        # Берем первое непустое значение в группе
        non_empty_values = group[col].dropna()
        if not non_empty_values.empty:
            combined_row[col] = non_empty_values.iloc[0]
        else:
            combined_row[col] = ''  # Если все значения пустые, оставляем пустым
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