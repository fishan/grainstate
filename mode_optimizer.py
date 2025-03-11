import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')
mode_file = input_dir / 'mode.csv'
output_file = output_dir / 'mode_optimized.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
mode_df = pd.read_csv(mode_file)

# Преобразование времени в datetime
def parse_time(row):
    return pd.to_datetime(row['Date'] + ' ' + row['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

mode_df['DateTime'] = mode_df.apply(parse_time, axis=1)

# Очищаем данные от некорректных временных меток
mode_df = mode_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)

# Группируем данные по дате и минуте
mode_df['MinuteKey'] = mode_df['DateTime'].dt.strftime('%d-%m-%Y %H:%M')  # Ключ для группировки (до минуты)
grouped = mode_df.groupby('MinuteKey', group_keys=False)

# Функция для объединения данных в группе
def combine_group(group):
    combined_row = {}
    for col in group.columns:
        if col in ['DateTime', 'MinuteKey']:
            continue  # Пропускаем служебные колонки
        # Для булевых колонок берем последнее непустое значение в минуте
        if col in ['FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL']:
            non_empty_values = group[col].dropna()
            if not non_empty_values.empty:
                combined_row[col] = non_empty_values.iloc[-1]  # Берем последнее значение в минуте
            else:
                combined_row[col] = ''
        # Для DROPS_SCORE берем первое непустое значение
        elif col == 'DROPS_SCORE':
            non_empty_values = group[col].dropna()
            combined_row[col] = non_empty_values.iloc[0] if not non_empty_values.empty else ''
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