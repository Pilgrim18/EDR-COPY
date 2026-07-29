# EDR-COPY: СУБД База Даних ЄДР (Єдиний Державний Реєстр)

Автоматизована система завантаження, паралельного імпорту та індексації даних **Єдиного державного реєстру юридичних осіб, фізичних осіб-підприємців та громадських формувань** з щотижневих дампів відкритих даних на [data.gov.ua](https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7) у СУБД PostgreSQL.

---

## 🔥 Особливості та Оптимізації

1. **Нормалізована SQL Схема**: Реляційна структура з 13 таблиць, яка повністю відповідає офіційним XSD-визначенням реєстрів ЮО, ФОП та відокремлених підрозділів (FSU).
2. **Паралельний Чанкінг XML (Chunked Stream Parsing)**: Автоматичний розпил великих XML-файлів (до 5.1GB) на незалежні частини за тегами `<SUBJECT>` з можливістю паралельної обробки у `N` процесах без завантаження в RAM (пам'ять < 500MB).
3. **Гнучкий Паралелізм (`NUM_WORKERS`)**: Налаштування кількості воркерів через конфігураційний параметр у `.env` або прапорець `--workers` командного рядка.
4. **Високопродуктивний Bulk Load**: Використання бінарного/текстового протоколу PostgreSQL `COPY` замість поверхових INSERT (до 100,000+ записів/сек).
5. **Оптимізована Індексація**: Створення B-tree індексів за кодом ЄДРПОУ, ПІБ/Найменуванням, станом діяльності та зовнішніми ключами після завершення завантаження.
6. **Повна Автоматизація та Моніторинг**: Включено скрипти завантаження з джерела (`download.py`) та моніторингу СУБД (`monitor.py`).

---

## 📁 Структура Проекту

```text
EDR-COPY/
├── .env                      # Локальні реквізити та конфігурація (ігнорується git)
├── .env.example              # Шаблон конфігурації
├── .gitignore                # Правила ігнорування (дампи ZIP, .env, .venv)
├── schema.sql                # DDL схема таблиць та функція створення індексів
├── download.py               # Автоматичне завантаження щотижневих дампів з data.gov.ua
├── import_edr.py             # Основний скрипт паралельного парсингу та імпорту в СУБД
├── monitor.py                # Моніторинг кількості записів та стану індексів СУБД
├── config.py                 # Модуль завантаження параметрів з .env
├── requirements.txt          # Залежності Python (psycopg3, lxml, tqdm, python-dotenv)
└── schemas/                  # Офіційні XSD-схеми реєстру
    ├── uo_schema/UO_schema.xsd
    ├── fop_schema/FOP_schema.xsd
    └── fsu_schema/FSU_schema.xsd
```

---

## 🚀 Повна Інструкція Користувача (Крок за Кроком)

### Крок 1: Клонування репозиторію та підготовка середовища Python 3.12

```bash
# 1. Клонувати репозиторій
git clone https://github.com/Pilgrim18/EDR-COPY.git
cd EDR-COPY

# 2. Створити віртуальне оточення Python 3.12
/opt/homebrew/bin/python3.12 -m venv .venv
source .venv/bin/activate

# 3. Встановити необхідні залежності
pip install -r requirements.txt
```

---

### Крок 2: Конфігурація підключення та параметрів (`.env`)

Створіть файл `.env` на основі шаблону `.env.example`:

```bash
cp .env.example .env
```

Відредагуйте параметри у файлі `.env`:

```ini
# Реквізити підключення до PostgreSQL
DB_HOST=192.168.0.46
DB_PORT=5432
DB_NAME=edr
DB_USER=edr
DB_PASSWORD=your_password_here

# Директорія для зберігання дампів XML
DATA_DIR=./data

# Розмір пачки записів для PostgreSQL COPY
BATCH_SIZE=10000

# Кількість паралельних воркерів (0 = авто-визначення за процесорними ядрами CPU, або вкажіть e.g. 4, 8, 12, 16)
NUM_WORKERS=8

# Актуальні посилання на оригінальний щотижневий дамп ЄДР з data.gov.ua
UO_DATA_URL=https://data.gov.ua/dataset/03cc1239-3988-4451-aa0d-aadb77448714/resource/d40cc921-39bb-44fd-be06-dc02589f45c6/download/uo-zip.zip
FOP_DATA_URL=https://data.gov.ua/dataset/03cc1239-3988-4451-aa0d-aadb77448714/resource/c262938f-cce7-4489-a805-2fd7c5a44e0b/download/fop-zip.zip
FSU_DATA_URL=https://data.gov.ua/dataset/03cc1239-3988-4451-aa0d-aadb77448714/resource/2b7eeee3-6c29-4ba8-8c35-1618974830eb/download/fsu-zip.zip
```

---

### Крок 3: Ініціалізація схеми PostgreSQL

Створіть таблиці СУБД та PL/pgSQL функцію індексації:

```bash
python3 -c "
from dotenv import load_dotenv
import os, psycopg

load_dotenv()
with open('schema.sql', encoding='utf-8') as f:
    sql = f.read()

conn = psycopg.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
with conn.cursor() as cur:
    cur.execute(sql)
conn.commit()
conn.close()
print('Схема бази даних успішно створена!')
"
```

---

### Крок 4: Автоматичне вивантаження оригінальних файлів з data.gov.ua

Запустіть скрипт `download.py` для завантаження останніх архівів дампу реєстру:

```bash
python3 download.py
```
*Скрипт автоматично вивантажить `uo.zip`, `fop.zip` та `fsu.zip` у папку `./data/` із підтримкою індикатора прогресу.*

---

### Крок 5: Запуск паралельного імпорту та автоматичного індексування

Запустіть конвеєр імпорту:

```bash
# Використовувати налаштування NUM_WORKERS з .env:
python3 import_edr.py

# Або явне вказання кількості паралельних воркерів (наприклад, 12 процесів):
python3 import_edr.py --workers 12
```

**Що відбувається під час імпорту:**
1. Скрипт швидко імпортує відокремлені підрозділи `fsu.zip`.
2. Великі архіви `uo.zip` та `fop.zip` паралельно діляться на рівні чанки.
3. `N` воркерів у режимі потоку завантажують понад **44 мільйони записів** у PostgreSQL через `COPY`.
4. По завершенню вставки автоматично викликається `create_edr_indexes()`, яка будує 19 B-tree індексів.

---

### Крок 6: Перевірка та моніторинг результатів

Для перегляду статистики кількості завантажених записів та стану створених індексів запустіть:

```bash
python3 monitor.py
```

Приклад виводу `monitor.py`:
```text
=== EDR Database Stats (192.168.0.46:5432/edr) ===
uo_subjects              :    2,013,803
fop_subjects             :    6,988,872
fsu_subjects             :          879
founders                 :    2,921,542
beneficiaries            :      802,809
signers                  :    2,797,516
branches                 :       51,678
members                  :       24,061
predecessors             :       74,013
assignees                :       73,873
termination_started      :    2,014,682
bankruptcy_readjustment  :    2,013,803
exchange_data            :   24,676,897
========================================
Total Database Rows       :   44,454,428
Custom Indexes Created    : 19
Indexes: ['idx_uo_edrpou', 'idx_fsu_edrpou', 'idx_branches_code', 'idx_fop_name', 'idx_uo_name', 'idx_fsu_name', 'idx_founders_record', 'idx_beneficiaries_record', 'idx_signers_record', 'idx_members_record', 'idx_branches_record', 'idx_predecessors_record', 'idx_assignees_record', 'idx_termination_record', 'idx_bankruptcy_record', 'idx_exchange_record', 'idx_uo_stan', 'idx_fop_stan', 'idx_fsu_stan']
```

---

## 🛠️ Рекомендовані Налаштування СУБД PostgreSQL (`postgresql.conf`)

Для забезпечення максимальної швидкодії під час паралельного завантаження десятків мільйонів записів через `COPY` та миттєвого виконання паралельних запитів рекомендується встановити наступні конфігурації у `postgresql.conf`:

### 1. Налаштування З'єднань та Паралелізму
```ini
# Максимальна кількість підключень (з запасом під воркери + клієнти)
max_connections = 100

# Паралельні воркери для побудови індексів (CREATE INDEX) та запитів
max_parallel_workers_per_gather = 4
max_parallel_maintenance_workers = 4
max_parallel_workers = 8
```

### 2. Пам'ять (Memory Buffers)
*(Розраховано для сервера з 16–32 GB RAM)*
```ini
# Кеш сторінок бази даних (25% від RAM сервера)
shared_buffers = 4GB..8GB

# Оперативна пам'ять на кожну операцію сортування/хешування
work_mem = 64MB

# Пам'ять для масових операцій (CREATE INDEX, VACUUM)
maintenance_work_mem = 1GB..2GB

# Оцінка дискового кешу для планувальника запитів
effective_cache_size = 12GB..24GB
```

### 3. WAL (Write-Ahead Logging) & Checkpoints (для прискорення масового `COPY`)
```ini
# Збільшення розміру WAL-файлів для уникнення частих контрольних точок під час імпорту
max_wal_size = 16GB
min_wal_size = 2GB

# Згладжування I/O навантаження контрольних точок
checkpoint_completion_target = 0.9
checkpoint_timeout = 15min

# На час первинного масового імпорту можна тимчасово вимкнути для прискорення запису в 2-3 рази:
# synchronous_commit = off   #(після завершення імпорту обов'язково повернути в 'on')
```

### 4. Оптимізатор та Дискова Підсистема (SSD / NVMe)
```ini
random_page_cost = 1.1          # Для швидких SSD/NVMe накопичувачів
effective_io_concurrency = 200    # Паралельні операції зчитування з диска
```

---

## 🔍 Приклади SQL-Запитів Пошуку

### 1. Пошук Юридичної Особи за кодом ЄДРПОУ (Час виконання < 1ms)
```sql
SELECT name, short_name, opf, stan, registration
FROM uo_subjects
WHERE edrpou = '33444728';
```

### 2. Пошук ФОП за ПІБ
```sql
SELECT name, stan, farmer, registration
FROM fop_subjects
WHERE name LIKE 'ШЕВЧЕНКО ТАРАС%'
LIMIT 10;
```

### 3. Пошук Бенефіціарів та Засновників Компанії
```sql
SELECT s.name AS company, s.edrpou, f.founder_info, b.beneficiary_info
FROM uo_subjects s
LEFT JOIN founders f ON f.subject_record = s.record AND f.subject_type = 'uo'
LEFT JOIN beneficiaries b ON b.subject_record = s.record AND b.subject_type = 'uo'
WHERE s.edrpou = '14359609';
```
