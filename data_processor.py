
import pandas as pd

# Путь к вашему файлу (замените на актуальный путь)
file_path = 'moisture_Perten_2024.txt'

# Список для хранения данных
data = []

# Откройте файл и прочитайте его строка за строкой
with open(file_path, 'r') as file:
    for line in file:
        # Разделите строку по пробелам или табуляции, игнорируя пустые значения
        values = [x for x in line.strip().split() if x]
        
        # Пропустите столбец ID (N/A)
        if len(values) > 4:
            date = values[0]
            time = values[1]
            grain = values[2]
            mois = values[4]
            tw = values[5]
            temp = values[6]
            
            # Добавьте данные в список
            data.append([date, time, grain, mois, tw, temp])

# Создайте DataFrame из данных
df = pd.DataFrame(data, columns=['Date', 'Time', 'Grain', '%mois', 'TW', 'Temp'])

# Сохраните результат в новый CSV-файл
output_file_path = 'perten_data.csv'
df.to_csv(output_file_path, index=False)

print(f"Данные успешно сохранены в файл: {output_file_path}")
