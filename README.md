# EDR-COPY: СУБД База Даних ЄДР (Єдиний Державний Реєстр)

Автоматизована система імпорту та індексації даних Єдиного державного реєстру юридичних осіб, фізичних осіб-підприємців та громадських формувань (з щотижневих дампів відкритих даних на [data.gov.ua](https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7)) у СУБД PostgreSQL.

## Особливості та Оптимізації

1. **Нормалізована SQL Схема**: Реляційна структура з 13 таблиць, що повністю покриває XSD-визначення реєстрів ЮО, ФОП та відокремлених підрозділів (FSU).
2. **Потоковий XML Парсинг**: Потокова обробка елементів `<SUBJECT>` без повного розпакування XML та завантаження в RAM (пам'ять < 500MB).
3. **Мультипроцесінг**: Одночасна обробка файлів UO, FOP та FSU у 3 паралельних процесах.
4. **Високопродуктивний Bulk Load**: Використання бінарного/текстового протоколу PostgreSQL `COPY` замість поверхових INSERT (до 100 000 записів/сек).
5. **Оптимізована Індексація**: Автоматичне створення індексів за ЄДРПОУ, ПІБ/Найменуванням, станом діяльності та зовнішніми ключами після імпорту.
6. **Гнучка конфігурація**: Збереження та завантаження всіх параметрів підключення через `.env` (`python-dotenv`).

---

## Структура Проекту

```text
EDR-COPY/
├── .env                      # Локальні реквізити та налаштування (не додається в git)
├── .env.example              # Шаблон конфігурації
├── .gitignore                # Правила ігнорування (дампи ZIP, .env, .venv)
├── schema.sql                # DDL схема таблиць та функція створення індексів
├── download.py               # Автоматичне завантаження щотижневих дампів з data.gov.ua
├── import_edr.py             # Основний скрипт потокового парсингу та імпорту в Postgres
├── config.py                 # Модуль завантаження налаштувань з .env
├── requirements.txt          # Залежності Python
└── schemas/                  # Офіційні XSD-схеми реєстру
    ├── uo_schema/UO_schema.xsd
    ├── fop_schema/FOP_schema.xsd
    └── fsu_schema/FSU_schema.xsd
```

---

## Швидкий Запуск

### 1. Налаштування середовища (Python 3.12)

```bash
/opt/homebrew/bin/python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Конфігурація підключення (`.env`)

Створіть `.env` на основі `.env.example`:

```ini
DB_HOST=192.168.0.46
DB_PORT=5432
DB_NAME=edr
DB_USER=edr
DB_PASSWORD=your_password_here

DATA_DIR=./data
BATCH_SIZE=10000
```

### 3. Створення Схеми БД

```bash
python3 -c "
from dotenv import load_dotenv
import os, psycopg
load_dotenv()
with open('schema.sql') as f:
    sql = f.read()
conn = psycopg.connect(
    host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')
)
with conn.cursor() as cur:
    cur.execute(sql)
conn.commit()
conn.close()
print('Схема успішно створена')
"
```

### 4. Завантаження даних та імпорт

```bash
# Завантаження щотижневого дампу
python3 download.py

# Запуск мультипроцесного імпорту та індексації
python3 import_edr.py
```

---

## Приклади Пошукових Запитів

### Пошук Юридичної Особи за кодом ЄДРПОУ

```sql
SELECT name, short_name, opf, stan, registration
FROM uo_subjects
WHERE edrpou = '14359609';
```

### Пошук ФОП за ПІБ

```sql
SELECT name, stan, farmer, registration
FROM fop_subjects
WHERE name LIKE 'ДУПЛІЙ СЕРГІЙ%';
```

### Пошук Бенефіціарів та Засновників Компанії

```sql
SELECT s.name as company, s.edrpou, f.founder_info, b.beneficiary_info
FROM uo_subjects s
LEFT JOIN founders f ON f.subject_record = s.record AND f.subject_type = 'uo'
LEFT JOIN beneficiaries b ON b.subject_record = s.record AND b.subject_type = 'uo'
WHERE s.edrpou = '33444728';
```
