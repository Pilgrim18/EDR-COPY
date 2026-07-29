"""
config.py - Configuration loader for EDR project using dotenv
"""
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "192.168.0.46")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "edr")
DB_USER = os.getenv("DB_USER", "edr")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATA_DIR = os.getenv("DATA_DIR", "./data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))

# Worker parallelism setting (0 = auto CPU count, or N explicit workers)
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "0"))
if NUM_WORKERS <= 0:
    NUM_WORKERS = os.cpu_count() or 4

UO_DATA_URL = os.getenv("UO_DATA_URL")
FOP_DATA_URL = os.getenv("FOP_DATA_URL")
FSU_DATA_URL = os.getenv("FSU_DATA_URL")
