import pandas as pd
from typing import List

def split_dataframe_by_time(df: pd.DataFrame, column: str, min_chunk_size: int, max_chunk_size: int) -> List[pd.DataFrame]:
    """
    Разбивает датафрейм на чанки по временной колонке.
    
    :param df: Исходный датафрейм
    :param column: Название колонки с датами
    :param min_chunk_size: Минимальный желаемый размер чанка
    :param max_chunk_size: Максимальный желаемый размер чанка
    :return: Список датафреймов (чанков)
    """
    # Получаем уникальные значения времени
    unique_times = df[column].unique()
    
    # Если количество уникальных значений меньше или равно max_chunk_size,
    # возвращаем весь датафрейм как один чанк
    if len(unique_times) <= max_chunk_size:
        return [df]
    
    chunks = []  # Список для хранения чанков
    current_chunk = []  # Текущий формируемый чанк
    unique_in_chunk = set()  # Множество уникальных значений времени в текущем чанке
    
    for _, row in df.iterrows():
        current_time = row[column]
        
        # Если текущее время новое для чанка
        if current_time not in unique_in_chunk:
            # Если достигнут максимальный размер чанка, сохраняем его и начинаем новый
            if len(unique_in_chunk) >= max_chunk_size:
                chunks.append(pd.DataFrame(current_chunk))
                current_chunk = []
                unique_in_chunk = set()
            unique_in_chunk.add(current_time)
        
        current_chunk.append(row)
        
        # Проверяем условия для завершения чанка
        if len(unique_in_chunk) >= max_chunk_size and len(current_chunk) > 1:
            # Получаем следующее значение времени, если оно есть
            next_time = df.loc[df.index > row.name, column].iloc[0] if row.name < df.index[-1] else None
            # Если следующего значения нет или оно новое, завершаем текущий чанк
            if next_time is None or next_time not in unique_in_chunk:
                chunks.append(pd.DataFrame(current_chunk))
                current_chunk = []
                unique_in_chunk = set()
    
    # Добавляем оставшиеся данные в последний чанк, если они есть
    if current_chunk:
        chunks.append(pd.DataFrame(current_chunk))
    
    return chunks

# Тестирование
dfs = pd.date_range(
    "2023-01-01 00:00:00", 
    "2023-01-01 00:00:05", 
    freq="s"
)
df = pd.DataFrame({"dt": dfs.repeat(3)})

print("Исходный датафрейм:")
print(df)
print()

test_cases = [(1, 2)]

for min_size, max_size in test_cases:
    chunks = split_dataframe_by_time(df, 'dt', min_size, max_size)
    print(f"Для размера чанка между {min_size} и {max_size}:")
    print(f"Количество чанков: {len(chunks)}")
    for i, chunk in enumerate(chunks):
        print(f"Чанк {i + 1}:")
        print(chunk)
        print()
    print("-" * 50)