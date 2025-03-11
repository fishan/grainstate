import pandas as pd
import isodate  # Для парсинга строки в формате ISO 8601

# Загрузка данных из CSV-файла
df = pd.read_csv('dryer_data.csv')

# Преобразование столбца 'timestamp' в формат datetime
# Указываем format='mixed', чтобы Pandas сам определил формат
df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')

# Разделение на столбцы 'Date' и 'Time'
df['Date'] = df['timestamp'].dt.strftime('%d-%m-%Y')  # Формат dd-mm-yyyy
df['Time'] = df['timestamp'].dt.strftime('%H:%M:%S')  # Формат HH:MM:SS

# Удаление исходного столбца 'timestamp'
df = df.drop(columns=['timestamp'])

# Удаление столбца 'measured'
df = df.drop(columns=['measured'])

# Функция для преобразования времени в секунды
def convert_to_seconds(iso_duration):
    try:
        # Удаляем лишние кавычки и парсим строку
        duration = isodate.parse_duration(iso_duration.strip('"'))
        # Возвращаем общее количество секунд
        return int(duration.total_seconds())
    except (ValueError, AttributeError):
        # Если формат некорректен, возвращаем None
        return None

# Применяем функцию к столбцу DROPS_SET_TIMER
df['DROPS_SET_TIMER'] = df['DROPS_SET_TIMER'].apply(convert_to_seconds)

# Переупорядочиваем столбцы, чтобы 'Date' и 'Time' были в начале
column_order = ['Date', 'Time'] + [col for col in df.columns if col not in ['Date', 'Time']]
df = df[column_order]

# Сохранение изменений обратно в CSV-файл
df.to_csv('dryer_data_updated.csv', index=False)