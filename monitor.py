#!/usr/bin/env python3
"""
monitor.py - Monitor script for EDR PostgreSQL database tables and import status
"""
import os
import sys
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "192.168.0.46")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "edr")
DB_USER = os.getenv("DB_USER", "edr")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

TABLES = [
    'uo_subjects',
    'fop_subjects',
    'fsu_subjects',
    'founders',
    'beneficiaries',
    'signers',
    'branches',
    'members',
    'predecessors',
    'assignees',
    'termination_started',
    'bankruptcy_readjustment',
    'exchange_data'
]

def main():
    try:
        conn = psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print(f"=== EDR Database Stats ({DB_HOST}:{DB_PORT}/{DB_NAME}) ===")
        total_rows = 0
        with conn.cursor() as cur:
            for tbl in TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                cnt = cur.fetchone()[0]
                total_rows += cnt
                print(f"{tbl:25s}: {cnt:>12,d}")
            
            # Check indexes
            cur.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE schemaname = 'public' AND indexname LIKE 'idx_%';
            """)
            indexes = cur.fetchall()
            print("=" * 40)
            print(f"Total Database Rows       : {total_rows:>12,d}")
            print(f"Custom Indexes Created    : {len(indexes)}")
            if indexes:
                print(f"Indexes: {[idx[0] for idx in indexes]}")
        conn.close()
    except Exception as e:
        print(f"Error querying database: {e}")

if __name__ == "__main__":
    main()
