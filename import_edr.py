#!/usr/bin/env python3
"""
import_edr.py - Parallel stream-based ETL pipeline for Ukrainian EDR dataset into PostgreSQL.
Supports configurable process parallelism (NUM_WORKERS) via .env or --workers CLI flag.
"""
import io
import os
import sys
import time
import argparse
import zipfile
import traceback
import multiprocessing as mp
from lxml import etree
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "192.168.0.46")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "edr")
DB_USER = os.getenv("DB_USER", "edr")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DATA_DIR = os.getenv("DATA_DIR", "./data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))

# Read NUM_WORKERS setting
DEFAULT_WORKERS = int(os.getenv("NUM_WORKERS", "0"))
if DEFAULT_WORKERS <= 0:
    DEFAULT_WORKERS = os.cpu_count() or 4

def get_db_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

class ChunkStream(io.RawIOBase):
    """Virtual stream wrapper for reading byte chunks from uncompressed zip files."""
    def __init__(self, zfile, xml_name, start, end):
        self.f = zfile.open(xml_name)
        self.f.seek(start)
        self.pos = start
        self.end = end
        self.prefix = b'<DATA>' if start > 0 else b''
        self.suffix = b'</DATA>'
        self.prefix_sent = False
        self.suffix_sent = False

    def readinto(self, b):
        if not self.prefix_sent and self.prefix:
            n = len(self.prefix)
            b[:n] = self.prefix
            self.prefix_sent = True
            return n
        
        rem = self.end - self.pos
        if rem <= 0:
            if not self.suffix_sent:
                n = len(self.suffix)
                b[:n] = self.suffix
                self.suffix_sent = True
                return n
            return 0

        chunk_len = min(len(b), rem)
        data = self.f.read(chunk_len)
        if not data:
            return 0
        self.pos += len(data)
        b[:len(data)] = data
        return len(data)

def find_xml_chunk_boundaries(zip_path, num_chunks):
    """Calculate byte boundaries aligned on <SUBJECT> tags."""
    boundaries = []
    with zipfile.ZipFile(zip_path, 'r') as z:
        xml_name = [f for f in z.namelist() if f.lower().endswith('.xml')][0]
        size = z.getinfo(xml_name).file_size
        if num_chunks <= 1 or size < 10 * 1024 * 1024:
            return [(0, size)]
        
        chunk_size = size // num_chunks
        with z.open(xml_name) as f:
            pos = 0
            for i in range(num_chunks):
                start = pos
                if i == num_chunks - 1:
                    end = size
                else:
                    target = start + chunk_size
                    f.seek(target)
                    buf = f.read(64 * 1024)
                    sub_idx = buf.find(b'<SUBJECT>')
                    if sub_idx != -1:
                        end = target + sub_idx
                    else:
                        end = target
                boundaries.append((start, end))
                pos = end
    return boundaries

def copy_batch(cur, table_name, columns, data_rows):
    """Bulk load data rows into PostgreSQL table using COPY protocol."""
    if not data_rows:
        return
    
    buf = io.StringIO()
    for row in data_rows:
        cleaned_vals = []
        for val in row:
            if val is None:
                cleaned_vals.append(r"\N")
            else:
                s = str(val).replace("\\", "\\\\").replace("\t", " ").replace("\r", " ").replace("\n", " ")
                cleaned_vals.append(s)
        buf.write("\t".join(cleaned_vals) + "\n")
    
    buf.seek(0)
    col_str = ", ".join(columns)
    sql = f"COPY {table_name} ({col_str}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
    with cur.copy(sql) as copy:
        copy.write(buf.getvalue())

def text_or_none(elem, path):
    child = elem.find(path)
    if child is not None and child.text:
        t = child.text.strip()
        return t if t else None
    return None

def process_uo_chunk(args):
    zip_path, start_bytes, end_bytes, chunk_id = args
    try:
        proc_name = f"UO-Worker-{chunk_id}"
        conn = get_db_connection()
        cur = conn.cursor()

        uo_cols = ["record", "name", "short_name", "opf", "edrpou", "stan", "founding_document_num",
                   "executive_power_name", "executive_power_code", "purpose", "superior_management",
                   "authorized_capital", "statute", "registration", "managing_paper",
                   "terminated_info", "termination_cancel_info"]
        
        founders_cols = ["subject_type", "subject_record", "founder_info"]
        beneficiaries_cols = ["subject_type", "subject_record", "beneficiary_info"]
        signers_cols = ["subject_type", "subject_record", "signer_info"]
        members_cols = ["subject_record", "member_info"]
        branches_cols = ["subject_record", "code", "name", "signer", "create_date"]
        predecessors_cols = ["subject_type", "subject_record", "name", "code"]
        assignees_cols = ["subject_record", "name", "code"]
        term_cols = ["subject_type", "subject_record", "op_date", "reason", "sbj_state", "signer_name", "creditor_req_end_date"]
        bankrupt_cols = ["subject_record", "op_date", "reason", "sbj_state", "head_name"]
        exchange_cols = ["subject_type", "subject_record", "branch_code", "tax_payer_type", "start_date", "start_num", "end_date", "end_num"]

        batches = {col: [] for col in ["uo", "founders", "beneficiaries", "signers", "members", "branches", 
                                       "predecessors", "assignees", "term", "bankrupt", "exchange"]}
        
        count = 0
        t0 = time.time()

        with zipfile.ZipFile(zip_path, 'r') as z:
            xml_filename = [f for f in z.namelist() if f.lower().endswith('.xml')][0]
            stream = ChunkStream(z, xml_filename, start_bytes, end_bytes)
            context = etree.iterparse(stream, events=('end',), tag='SUBJECT', recover=True)
            for event, elem in context:
                record = text_or_none(elem, "RECORD")
                if not record:
                    elem.clear()
                    continue

                exec_name = None
                exec_code = None
                exec_elem = elem.find("EXECUTIVE_POWER")
                if exec_elem is not None:
                    exec_name = text_or_none(exec_elem, "NAME")
                    exec_code = text_or_none(exec_elem, "CODE")

                batches["uo"].append((
                    record,
                    text_or_none(elem, "NAME"),
                    text_or_none(elem, "SHORT_NAME"),
                    text_or_none(elem, "OPF"),
                    text_or_none(elem, "EDRPOU"),
                    text_or_none(elem, "STAN"),
                    text_or_none(elem, "FOUNDING_DOCUMENT_NUM"),
                    exec_name,
                    exec_code,
                    text_or_none(elem, "PURPOSE"),
                    text_or_none(elem, "SUPERIOR_MANAGEMENT"),
                    text_or_none(elem, "AUTHORIZED_CAPITAL"),
                    text_or_none(elem, "STATUTE"),
                    text_or_none(elem, "REGISTRATION"),
                    text_or_none(elem, "MANAGING_PAPER"),
                    text_or_none(elem, "TERMINATED_INFO"),
                    text_or_none(elem, "TERMINATION_CANCEL_INFO")
                ))

                founders_elem = elem.find("FOUNDERS")
                if founders_elem is not None:
                    for f_item in founders_elem.findall("FOUNDER"):
                        if f_item.text and f_item.text.strip():
                            batches["founders"].append(("uo", record, f_item.text.strip()))

                ben_elem = elem.find("BENEFICIARIES")
                if ben_elem is not None:
                    for b_item in ben_elem.findall("BENEFICIARY"):
                        if b_item.text and b_item.text.strip():
                            batches["beneficiaries"].append(("uo", record, b_item.text.strip()))

                sig_elem = elem.find("SIGNERS")
                if sig_elem is not None:
                    for s_item in sig_elem.findall("SIGNER"):
                        if s_item.text and s_item.text.strip():
                            batches["signers"].append(("uo", record, s_item.text.strip()))

                mem_elem = elem.find("MEMBERS")
                if mem_elem is not None:
                    for m_item in mem_elem.findall("MEMBER"):
                        if m_item.text and m_item.text.strip():
                            batches["members"].append((record, m_item.text.strip()))

                br_elem = elem.find("BRANCHES")
                if br_elem is not None:
                    for b_item in br_elem.findall("BRANCH"):
                        b_code = text_or_none(b_item, "CODE")
                        batches["branches"].append((
                            record,
                            b_code,
                            text_or_none(b_item, "NAME"),
                            text_or_none(b_item, "SIGNER"),
                            text_or_none(b_item, "CREATE_DATE")
                        ))
                        br_ex = b_item.find("EXCHANGE_DATA")
                        if br_ex is not None:
                            for ex_ans in br_ex.findall("EXCHANGE_ANSWER"):
                                batches["exchange"].append((
                                    "branch", record, b_code,
                                    text_or_none(ex_ans, "TAX_PAYER_TYPE"),
                                    text_or_none(ex_ans, "START_DATE"),
                                    text_or_none(ex_ans, "START_NUM"),
                                    text_or_none(ex_ans, "END_DATE"),
                                    text_or_none(ex_ans, "END_NUM")
                                ))

                pred_elem = elem.find("PREDECESSORS")
                if pred_elem is not None:
                    for p_item in pred_elem.findall("PREDECESSOR"):
                        batches["predecessors"].append((
                            "uo", record,
                            text_or_none(p_item, "NAME"),
                            text_or_none(p_item, "CODE")
                        ))

                ass_elem = elem.find("ASSIGNEES")
                if ass_elem is not None:
                    for a_item in ass_elem.findall("ASSIGNEE"):
                        batches["assignees"].append((
                            record,
                            text_or_none(a_item, "NAME"),
                            text_or_none(a_item, "CODE")
                        ))

                term_elem = elem.find("TERMINATION_STARTED_INFO")
                if term_elem is not None:
                    batches["term"].append((
                        "uo", record,
                        text_or_none(term_elem, "OP_DATE"),
                        text_or_none(term_elem, "REASON"),
                        text_or_none(term_elem, "SBJ_STATE"),
                        text_or_none(term_elem, "SIGNER_NAME"),
                        text_or_none(term_elem, "CREDITOR_REQ_END_DATE")
                    ))

                bank_elem = elem.find("BANKRUPTCY_READJUSTMENT_INFO")
                if bank_elem is not None:
                    batches["bankrupt"].append((
                        record,
                        text_or_none(bank_elem, "OP_DATE"),
                        text_or_none(bank_elem, "REASON"),
                        text_or_none(bank_elem, "SBJ_STATE"),
                        text_or_none(bank_elem, "BANKRUPTCY_READJUSTMENT_HEAD_NAME")
                    ))

                ex_elem = elem.find("EXCHANGE_DATA")
                if ex_elem is not None:
                    for ex_ans in ex_elem.findall("EXCHANGE_ANSWER"):
                        batches["exchange"].append((
                            "uo", record, None,
                            text_or_none(ex_ans, "TAX_PAYER_TYPE"),
                            text_or_none(ex_ans, "START_DATE"),
                            text_or_none(ex_ans, "START_NUM"),
                            text_or_none(ex_ans, "END_DATE"),
                            text_or_none(ex_ans, "END_NUM")
                        ))

                count += 1

                if len(batches["uo"]) >= BATCH_SIZE:
                    copy_batch(cur, "uo_subjects", uo_cols, batches["uo"])
                    copy_batch(cur, "founders", founders_cols, batches["founders"])
                    copy_batch(cur, "beneficiaries", beneficiaries_cols, batches["beneficiaries"])
                    copy_batch(cur, "signers", signers_cols, batches["signers"])
                    copy_batch(cur, "members", members_cols, batches["members"])
                    copy_batch(cur, "branches", branches_cols, batches["branches"])
                    copy_batch(cur, "predecessors", predecessors_cols, batches["predecessors"])
                    copy_batch(cur, "assignees", assignees_cols, batches["assignees"])
                    copy_batch(cur, "termination_started", term_cols, batches["term"])
                    copy_batch(cur, "bankruptcy_readjustment", bankrupt_cols, batches["bankrupt"])
                    copy_batch(cur, "exchange_data", exchange_cols, batches["exchange"])
                    conn.commit()
                    for key in batches:
                        batches[key].clear()

                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        if batches["uo"]:
            copy_batch(cur, "uo_subjects", uo_cols, batches["uo"])
            copy_batch(cur, "founders", founders_cols, batches["founders"])
            copy_batch(cur, "beneficiaries", beneficiaries_cols, batches["beneficiaries"])
            copy_batch(cur, "signers", signers_cols, batches["signers"])
            copy_batch(cur, "members", members_cols, batches["members"])
            copy_batch(cur, "branches", branches_cols, batches["branches"])
            copy_batch(cur, "predecessors", predecessors_cols, batches["predecessors"])
            copy_batch(cur, "assignees", assignees_cols, batches["assignees"])
            copy_batch(cur, "termination_started", term_cols, batches["term"])
            copy_batch(cur, "bankruptcy_readjustment", bankrupt_cols, batches["bankrupt"])
            copy_batch(cur, "exchange_data", exchange_cols, batches["exchange"])
            conn.commit()

        cur.close()
        conn.close()
        print(f"[{proc_name}] UO chunk {chunk_id} import complete: {count:,} subjects in {time.time()-t0:.1f}s")
        return count
    except Exception as err:
        print(f"Error in UO chunk {chunk_id}: {err}\n{traceback.format_exc()}")
        raise RuntimeError(f"UO Chunk {chunk_id} Error: {err}")

def process_fop_chunk(args):
    zip_path, start_bytes, end_bytes, chunk_id = args
    try:
        proc_name = f"FOP-Worker-{chunk_id}"
        conn = get_db_connection()
        cur = conn.cursor()

        fop_cols = ["record", "name", "stan", "farmer", "estate_manager", "registration",
                    "terminated_info", "termination_cancel_info"]
        exchange_cols = ["subject_type", "subject_record", "branch_code", "tax_payer_type",
                         "start_date", "start_num", "end_date", "end_num"]

        batches = {"fop": [], "exchange": []}
        count = 0
        t0 = time.time()

        with zipfile.ZipFile(zip_path, 'r') as z:
            xml_filename = [f for f in z.namelist() if f.lower().endswith('.xml')][0]
            stream = ChunkStream(z, xml_filename, start_bytes, end_bytes)
            context = etree.iterparse(stream, events=('end',), tag='SUBJECT', recover=True)
            for event, elem in context:
                record = text_or_none(elem, "RECORD")
                if not record:
                    elem.clear()
                    continue

                batches["fop"].append((
                    record,
                    text_or_none(elem, "NAME"),
                    text_or_none(elem, "STAN"),
                    text_or_none(elem, "FARMER"),
                    text_or_none(elem, "ESTATE_MANAGER"),
                    text_or_none(elem, "REGISTRATION"),
                    text_or_none(elem, "TERMINATED_INFO"),
                    text_or_none(elem, "TERMINATION_CANCEL_INFO")
                ))

                ex_elem = elem.find("EXCHANGE_DATA")
                if ex_elem is not None:
                    for ex_ans in ex_elem.findall("EXCHANGE_ANSWER"):
                        batches["exchange"].append((
                            "fop", record, None,
                            text_or_none(ex_ans, "TAX_PAYER_TYPE"),
                            text_or_none(ex_ans, "START_DATE"),
                            text_or_none(ex_ans, "START_NUM"),
                            text_or_none(ex_ans, "END_DATE"),
                            text_or_none(ex_ans, "END_NUM")
                        ))

                count += 1

                if len(batches["fop"]) >= BATCH_SIZE:
                    copy_batch(cur, "fop_subjects", fop_cols, batches["fop"])
                    copy_batch(cur, "exchange_data", exchange_cols, batches["exchange"])
                    conn.commit()
                    batches["fop"].clear()
                    batches["exchange"].clear()

                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        if batches["fop"]:
            copy_batch(cur, "fop_subjects", fop_cols, batches["fop"])
            copy_batch(cur, "exchange_data", exchange_cols, batches["exchange"])
            conn.commit()

        cur.close()
        conn.close()
        print(f"[{proc_name}] FOP chunk {chunk_id} import complete: {count:,} subjects in {time.time()-t0:.1f}s")
        return count
    except Exception as err:
        print(f"Error in FOP chunk {chunk_id}: {err}\n{traceback.format_exc()}")
        raise RuntimeError(f"FOP Chunk {chunk_id} Error: {err}")

def process_fsu(zip_path):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM fsu_subjects")
        if cur.fetchone()[0] > 0:
            print("[FSU-Worker] FSU table already populated. Skipping.")
            cur.close()
            conn.close()
            return 0

        fsu_cols = ["record", "name", "short_name", "type_subject", "type_branch", "edrpou", "stan",
                    "founding_document", "registration", "terminated_info", "termination_cancel_info"]
        founders_cols = ["subject_type", "subject_record", "founder_info"]
        beneficiaries_cols = ["subject_type", "subject_record", "beneficiary_info"]
        signers_cols = ["subject_type", "subject_record", "signer_info"]
        predecessors_cols = ["subject_type", "subject_record", "name", "code"]
        term_cols = ["subject_type", "subject_record", "op_date", "reason", "sbj_state", "signer_name", "creditor_req_end_date"]
        exchange_cols = ["subject_type", "subject_record", "branch_code", "tax_payer_type", "start_date", "start_num", "end_date", "end_num"]

        batches = {col: [] for col in ["fsu", "founders", "beneficiaries", "signers", "predecessors", "term", "exchange"]}
        count = 0
        t0 = time.time()

        with zipfile.ZipFile(zip_path, 'r') as z:
            xml_filename = [f for f in z.namelist() if f.lower().endswith('.xml')][0]
            with z.open(xml_filename) as f:
                context = etree.iterparse(f, events=('end',), tag='SUBJECT', recover=True)
                for event, elem in context:
                    record = text_or_none(elem, "RECORD")
                    if not record:
                        elem.clear()
                        continue

                    batches["fsu"].append((
                        record,
                        text_or_none(elem, "NAME"),
                        text_or_none(elem, "SHORT_NAME"),
                        text_or_none(elem, "TYPE_SUBJECT"),
                        text_or_none(elem, "TYPE_BRANCH"),
                        text_or_none(elem, "EDRPOU"),
                        text_or_none(elem, "STAN"),
                        text_or_none(elem, "FOUNDING_DOCUMENT"),
                        text_or_none(elem, "REGISTRATION"),
                        text_or_none(elem, "TERMINATED_INFO"),
                        text_or_none(elem, "TERMINATION_CANCEL_INFO")
                    ))

                    founders_elem = elem.find("FOUNDERS")
                    if founders_elem is not None:
                        for f_item in founders_elem.findall("FOUNDER"):
                            if f_item.text and f_item.text.strip():
                                batches["founders"].append(("fsu", record, f_item.text.strip()))

                    ben_elem = elem.find("BENEFICIARIES")
                    if ben_elem is not None:
                        for b_item in ben_elem.findall("BENEFICIARY"):
                            if b_item.text and b_item.text.strip():
                                batches["beneficiaries"].append(("fsu", record, b_item.text.strip()))

                    sig_elem = elem.find("SIGNERS")
                    if sig_elem is not None:
                        for s_item in sig_elem.findall("SIGNER"):
                            if s_item.text and s_item.text.strip():
                                batches["signers"].append(("fsu", record, s_item.text.strip()))

                    pred_elem = elem.find("PREDECESSORS")
                    if pred_elem is not None:
                        for p_item in pred_elem.findall("PREDECESSOR"):
                            batches["predecessors"].append((
                                "fsu", record,
                                text_or_none(p_item, "NAME"),
                                text_or_none(p_item, "CODE")
                            ))

                    term_elem = elem.find("TERMINATION_STARTED_INFO")
                    if term_elem is not None:
                        batches["term"].append((
                            "fsu", record,
                            text_or_none(term_elem, "OP_DATE"),
                            text_or_none(term_elem, "REASON"),
                            text_or_none(term_elem, "SBJ_STATE"),
                            text_or_none(term_elem, "SIGNER_NAME"),
                            text_or_none(term_elem, "CREDITOR_REQ_END_DATE")
                        ))

                    ex_elem = elem.find("EXCHANGE_DATA")
                    if ex_elem is not None:
                        for ex_ans in ex_elem.findall("EXCHANGE_ANSWER"):
                            batches["exchange"].append((
                                "fsu", record, None,
                                text_or_none(ex_ans, "TAX_PAYER_TYPE"),
                                text_or_none(ex_ans, "START_DATE"),
                                text_or_none(ex_ans, "START_NUM"),
                                text_or_none(ex_ans, "END_DATE"),
                                text_or_none(ex_ans, "END_NUM")
                            ))

                    count += 1
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

        copy_batch(cur, "fsu_subjects", fsu_cols, batches["fsu"])
        copy_batch(cur, "founders", founders_cols, batches["founders"])
        copy_batch(cur, "beneficiaries", beneficiaries_cols, batches["beneficiaries"])
        copy_batch(cur, "signers", signers_cols, batches["signers"])
        copy_batch(cur, "predecessors", predecessors_cols, batches["predecessors"])
        copy_batch(cur, "termination_started", term_cols, batches["term"])
        copy_batch(cur, "exchange_data", exchange_cols, batches["exchange"])
        conn.commit()

        cur.close()
        conn.close()
        print(f"[FSU-Worker] FSU import complete: {count:,} subjects in {time.time()-t0:.1f}s")
        return count
    except Exception as err:
        print(f"Error in FSU processing: {err}\n{traceback.format_exc()}")
        raise RuntimeError(f"FSU Error: {err}")

def run_indexing():
    print("Creating indexes in PostgreSQL...")
    t0 = time.time()
    conn = get_db_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT create_edr_indexes();")
    conn.close()
    print(f"Indexes created successfully in {time.time()-t0:.1f}s!")

def main():
    parser = argparse.ArgumentParser(description="Parallel EDR Dataset PostgreSQL Importer")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of parallel worker processes (default: NUM_WORKERS from .env or CPU count)")
    args = parser.parse_args()

    workers = args.workers
    start_total = time.time()
    print(f"=== Starting EDR Dataset Parallel Import (Workers: {workers}) ===")

    uo_zip = os.path.join(DATA_DIR, "uo.zip")
    fop_zip = os.path.join(DATA_DIR, "fop.zip")
    fsu_zip = os.path.join(DATA_DIR, "fsu.zip")

    for zpath in [uo_zip, fop_zip, fsu_zip]:
        if not os.path.exists(zpath):
            print(f"Error: Missing dataset file {zpath}. Please run download.py first.")
            sys.exit(1)

    # 1. Process FSU (small dataset)
    process_fsu(fsu_zip)

    # 2. Check table populations for UO & FOP
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM uo_subjects")
        uo_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fop_subjects")
        fop_count = cur.fetchone()[0]
    conn.close()

    tasks = []
    
    if uo_count == 0:
        uo_chunks = max(1, workers // 2)
        uo_bounds = find_xml_chunk_boundaries(uo_zip, uo_chunks)
        print(f"Splitting UO into {len(uo_bounds)} parallel worker chunks...")
        for i, (s, e) in enumerate(uo_bounds):
            tasks.append((process_uo_chunk, (uo_zip, s, e, i)))
    else:
        print(f"UO table already populated ({uo_count:,} records). Skipping UO import.")

    if fop_count == 0:
        fop_chunks = max(1, workers - (len(tasks)))
        fop_bounds = find_xml_chunk_boundaries(fop_zip, fop_chunks)
        print(f"Splitting FOP into {len(fop_bounds)} parallel worker chunks...")
        for i, (s, e) in enumerate(fop_bounds):
            tasks.append((process_fop_chunk, (fop_zip, s, e, i)))
    else:
        print(f"FOP table already populated ({fop_count:,} records). Skipping FOP import.")

    if tasks:
        pool_size = min(workers, len(tasks))
        print(f"Launching worker pool with {pool_size} parallel processes...")
        with mp.Pool(processes=pool_size) as pool:
            results = [pool.apply_async(func, (task_args,)) for func, task_args in tasks]
            for res in results:
                res.get()

    run_indexing()

    total_time = time.time() - start_total
    print(f"=== EDR Import Finished Successfully in {total_time/60:.2f} minutes ===")

if __name__ == "__main__":
    main()
