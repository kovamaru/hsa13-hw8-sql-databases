import csv
import os
import time
import multiprocessing
from faker import Faker

# Configuration
NUM_USERS = 40_000_000            # Загальна кількість користувачів
CSV_FILE = "/app/shared/users.csv"  # Шлях до файлу у спільному томі (доступному обом контейнерам)
NUM_PROCESSES = max(1, int(multiprocessing.cpu_count() * 0.9))
BATCH_SIZE = 500_000             # Записуємо у CSV пакетами по 500К рядків

fake = Faker()
Faker.seed(42)

def generate_unique_email(index):
  """Генерує унікальний email із використанням індексу та випадкового рядка."""
  random_part = fake.lexify(text="????").lower()  # випадковий рядок з 4 букв
  return f"{random_part}{index}@example.com"

def count_existing_records():
  """Рахуємо, скільки рядків вже є у CSV-файлі (без заголовка)."""
  if not os.path.exists(CSV_FILE):
    return 0
  with open(CSV_FILE, "r") as f:
    return sum(1 for _ in f) - 1  # віднімаємо заголовок

def generate_users(start, end, queue):
  """Генеруємо користувачів з унікальними email та надсилаємо їх у чергу пакетами."""
  print(f"🟢 Process {start}-{end} started...")
  users = []
  for i in range(start, end):
    name = fake.name()
    email = generate_unique_email(i)
    dob = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d")
    users.append([name, email, dob])
    if len(users) >= BATCH_SIZE:
      queue.put(users)
      print(f"✅ Process {start}-{end} sent {len(users)} users to queue")
      users = []  # Очищаємо список для наступного батчу
  if users:
    queue.put(users)
    print(f"✅ Process {start}-{end} sent {len(users)} remaining users to queue")
  print(f"✅ Process {start}-{end} finished generating users.")

def write_csv(queue, append_mode):
  """Записуємо користувачів із черги у CSV-файл."""
  mode = "a" if append_mode else "w"
  print(f"🟢 Writing data to {CSV_FILE} (mode: {mode})...")
  with open(CSV_FILE, mode, newline="") as f:
    writer = csv.writer(f)
    if not append_mode:
      writer.writerow(["name", "email", "date_of_birth"])
    while True:
      users = queue.get()
      if users is None:
        break
      print(f"🟢 Writing batch of {len(users)} users to CSV...")
      writer.writerows(users)
  print(f"✅ CSV file successfully written to {CSV_FILE}!")

def generate_csv():
  """Головна функція: перевіряє наявність даних і генерує відсутніх користувачів."""
  print("🟢 Checking if users.csv already exists...")
  existing_users = count_existing_records()
  print(f"🟢 Found {existing_users} existing records in users.csv")
  if existing_users >= NUM_USERS:
    print(f"✅ users.csv already contains {existing_users} users. Skipping generation.")
    return
  missing_users = NUM_USERS - existing_users
  print(f"🟢 Need to generate {missing_users} users...")
  start_time = time.time()

  with multiprocessing.Manager() as manager:
    queue = manager.Queue()
    processes = []
    chunk_size = missing_users // NUM_PROCESSES
    for i in range(existing_users, NUM_USERS, chunk_size):
      p = multiprocessing.Process(target=generate_users, args=(i, min(i + chunk_size, NUM_USERS), queue))
      processes.append(p)
      p.start()

    writer_process = multiprocessing.Process(target=write_csv, args=(queue, existing_users > 0))
    writer_process.start()

    for p in processes:
      p.join()
    queue.put(None)  # Сигнал для зупинки запису
    writer_process.join()

  end_time = time.time()
  print(f"✅ CSV file updated successfully in {round(end_time - start_time, 2)} seconds!")

if __name__ == "__main__":
  generate_csv()