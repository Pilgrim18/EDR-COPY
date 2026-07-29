#!/usr/bin/env python3
"""
import_edr.py - Optimized, stream-based ETL pipeline for Ukrainian EDR dataset into PostgreSQL.
"""
import io
import os
import sys
import time
import zipfile
import multiprocessing as mp
from lxml import etree
import psycopg
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "192.168.0.46")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "edr")
DB_USER = os.getenv("DB_USER", "edr")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DATA_DIR = os.getenv("DATA_DIR", "./data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))

def get_db_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

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

def process_uo(zip_path):
    print(f"[{mp.current_process().name}] Starting UO parsing from {zip_path}...")
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
        with z.open(xml_filename) as f:
            context = etree.iterparse(f, events=('end',), tag='SUBJECT')
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

                # FOUNDERS
                founders_elem = elem.find("FOUNDERS")
                if founders_elem is not None:
                    for f_item in founders_elem.findall("FOUNDER"):
                        if f_item.text and f_item.text.strip():
                            batches["founders"].append(("uo", record, f_item.text.strip()))

                # BENEFICIARIES
                ben_elem = elem.find("BENEFICIARIES")
                if ben_elem is not None:
                    for b_item in ben_elem.findall("BENEFICIARY"):
                        if b_item.text and b_item.text.strip():
                            batches["beneficiaries"].append(("uo", record, b_item.text.strip()))

                # SIGNERS
                sig_elem = elem.find("SIGNERS")
                if sig_elem is not None:
                    for s_item in sig_elem.findall("SIGNER"):
                        if s_item.text and s_item.text.strip():
                            batches["signers"].append(("uo", record, s_item.text.strip()))

                # MEMBERS
                mem_elem = elem.find("MEMBERS")
                if mem_elem is not None:
                    for m_item in mem_elem.findall("MEMBER"):
                        if m_item.text and m_item.text.strip():
                            batches["members"].append((record, m_item.text.strip()))

                # BRANCHES
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
                        # Branch exchange data
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

                # PREDECESSORS
                pred_elem = elem.find("PREDECESSORS")
                if pred_elem is not None:
                    for p_item in pred_elem.findall("PREDECESSOR"):
                        batches["predecessors"].append((
                            "uo", record,
                            text_or_none(p_item, "NAME"),
                            text_or_none(p_item, "CODE")
                        ))

                # ASSIGNEES
                ass_elem = elem.find("ASSIGNEES")
                if ass_elem is not None:
                    for a_item in ass_elem.findall("ASSIGNEE"):
                        batches["assignees"].append((
                            record,
                            text_or_none(a_item, "NAME"),
                            text_or_none(a_item, "CODE")
                        ))

                # TERMINATION_STARTED_INFO
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

                # BANKRUPTCY_READJUSTMENT_INFO
                bank_elem = elem.find("BANKRUPTCY_READJUSTMENT_INFO")
                if bank_elem is not None:
                    batches["bankrupt"].append((
                        record,
                        text_or_none(bank_elem, "OP_DATE"),
                        text_or_none(bank_elem, "REASON"),
                        text_or_none(bank_elem, "SBJ_STATE"),
                        text_or_none(bank_elem, "BANKRUPTCY_READJUSTMENT_HEAD_NAME")
                    ))

                # EXCHANGE_DATA
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

    # Flush remaining
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
    print(f"[{mp.current_process().name}] UO import complete: {count:,} subjects in {time.time()-t0:.1f}s")
    return count

def process_fop(zip_path):
    print(f"[{mp.current_process().name}] Starting FOP parsing from {zip_path}...")
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
        with z.open(xml_filename) as f:
            context = etree.iterparse(f, events=('end',), tag='SUBJECT')
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
    print(f"[{mp.current_process().name}] FOP import complete: {count:,} subjects in {time.time()-t0:.1f}s")
    return count

def process_fsu(zip_path):
    print(f"[{mp.current_process().name}] Starting FSU parsing from {zip_path}...")
    conn = get_db_connection()
    cur = conn.cursor()

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
            context = etree.iterparse(f, events=('end',), tag='SUBJECT')
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
    print(f"[{mp.current_process().name}] FSU import complete: {count:,} subjects in {time.time()-t0:.1f}s")
    return count

def run_indexing():
    print("Creating indexes in PostgreSQL (this may take 1-2 minutes)...")
    t0 = time.time()
    conn = get_db_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT create_edr_indexes();")
    conn.close()
    print(f"Indexes created successfully in {time.time()-t0:.1f}s!")

def main():
    start_total = time.time()
    print("=== Starting EDR Dataset Multiprocessing Import ===")

    uo_zip = os.path.join(DATA_DIR, "uo.zip")
    fop_zip = os.path.join(DATA_DIR, "fop.zip")
    fsu_zip = os.path.join(DATA_DIR, "fsu.zip")

    for zpath in [uo_zip, fop_zip, fsu_zip]:
        if not os.path.exists(zpath):
            print(f"Error: Missing dataset file {zpath}. Please run download.py first.")
            sys.exit(1)

    tasks = [
        ("Process-FSU", process_fsu, fsu_zip),
        ("Process-UO", process_uo, uo_zip),
        ("Process-FOP", process_fop, fop_zip)
    ]

    with mp.Pool(processes=len(tasks)) as pool:
        results = [pool.apply_async(func, (path,)) for name, func, path in tasks]
        for res in results:
            res.get()

    run_indexing()

    total_time = time.time() - start_total
    print(f"=== EDR Import Finished Successfully in {total_time/60:.2f} minutes ===")

if __name__ == "__main__":
    main()
