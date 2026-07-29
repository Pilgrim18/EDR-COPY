-- EDR Database Schema (PostgreSQL)

DROP TABLE IF EXISTS exchange_data CASCADE;
DROP TABLE IF EXISTS bankruptcy_readjustment CASCADE;
DROP TABLE IF EXISTS termination_started CASCADE;
DROP TABLE IF EXISTS assignees CASCADE;
DROP TABLE IF EXISTS predecessors CASCADE;
DROP TABLE IF EXISTS branches CASCADE;
DROP TABLE IF EXISTS members CASCADE;
DROP TABLE IF EXISTS signers CASCADE;
DROP TABLE IF EXISTS beneficiaries CASCADE;
DROP TABLE IF EXISTS founders CASCADE;
DROP TABLE IF EXISTS fsu_subjects CASCADE;
DROP TABLE IF EXISTS fop_subjects CASCADE;
DROP TABLE IF EXISTS uo_subjects CASCADE;

-- 1. Юридичні особи (UO)
CREATE TABLE uo_subjects (
    id                      BIGSERIAL PRIMARY KEY,
    record                  TEXT NOT NULL UNIQUE,
    name                    TEXT,
    short_name              TEXT,
    opf                     TEXT,
    edrpou                  TEXT,
    stan                    TEXT,
    founding_document_num   TEXT,
    executive_power_name    TEXT,
    executive_power_code    TEXT,
    purpose                 TEXT,
    superior_management     TEXT,
    authorized_capital      TEXT,
    statute                 TEXT,
    registration            TEXT,
    managing_paper          TEXT,
    terminated_info         TEXT,
    termination_cancel_info TEXT
);

-- 2. ФОП (FOP)
CREATE TABLE fop_subjects (
    id                      BIGSERIAL PRIMARY KEY,
    record                  TEXT NOT NULL UNIQUE,
    name                    TEXT,
    stan                    TEXT,
    farmer                  TEXT,
    estate_manager          TEXT,
    registration            TEXT,
    terminated_info         TEXT,
    termination_cancel_info TEXT
);

-- 3. Відокремлені підрозділи іноземних ЮО (FSU)
CREATE TABLE fsu_subjects (
    id                      BIGSERIAL PRIMARY KEY,
    record                  TEXT NOT NULL UNIQUE,
    name                    TEXT,
    short_name              TEXT,
    type_subject            TEXT,
    type_branch             TEXT,
    edrpou                  TEXT,
    stan                    TEXT,
    founding_document       TEXT,
    registration            TEXT,
    terminated_info         TEXT,
    termination_cancel_info TEXT
);

-- 4. Засновники (UO / FSU)
CREATE TABLE founders (
    id              BIGSERIAL PRIMARY KEY,
    subject_type    TEXT NOT NULL, -- 'uo' | 'fsu'
    subject_record  TEXT NOT NULL,
    founder_info    TEXT
);

-- 5. Кінцеві бенефіціарні власники (UO / FSU)
CREATE TABLE beneficiaries (
    id              BIGSERIAL PRIMARY KEY,
    subject_type    TEXT NOT NULL, -- 'uo' | 'fsu'
    subject_record  TEXT NOT NULL,
    beneficiary_info TEXT
);

-- 6. Керівники / Підписанти (UO / FSU)
CREATE TABLE signers (
    id              BIGSERIAL PRIMARY KEY,
    subject_type    TEXT NOT NULL, -- 'uo' | 'fsu'
    subject_record  TEXT NOT NULL,
    signer_info     TEXT
);

-- 7. Члени керівних органів (UO)
CREATE TABLE members (
    id              BIGSERIAL PRIMARY KEY,
    subject_record  TEXT NOT NULL,
    member_info     TEXT
);

-- 8. Відокремлені підрозділи ЮО (UO)
CREATE TABLE branches (
    id              BIGSERIAL PRIMARY KEY,
    subject_record  TEXT NOT NULL,
    code            TEXT,
    name            TEXT,
    signer          TEXT,
    create_date     TEXT
);

-- 9. Попередники (UO / FSU)
CREATE TABLE predecessors (
    id              BIGSERIAL PRIMARY KEY,
    subject_type    TEXT NOT NULL, -- 'uo' | 'fsu'
    subject_record  TEXT NOT NULL,
    name            TEXT,
    code            TEXT
);

-- 10. Правонаступники (UO)
CREATE TABLE assignees (
    id              BIGSERIAL PRIMARY KEY,
    subject_record  TEXT NOT NULL,
    name            TEXT,
    code            TEXT
);

-- 11. Перебування у процесі припинення (UO / FSU)
CREATE TABLE termination_started (
    id                      BIGSERIAL PRIMARY KEY,
    subject_type            TEXT NOT NULL, -- 'uo' | 'fsu'
    subject_record          TEXT NOT NULL,
    op_date                 TEXT,
    reason                  TEXT,
    sbj_state               TEXT,
    signer_name             TEXT,
    creditor_req_end_date   TEXT
);

-- 12. Банкрутство / Санація (UO)
CREATE TABLE bankruptcy_readjustment (
    id              BIGSERIAL PRIMARY KEY,
    subject_record  TEXT NOT NULL,
    op_date         TEXT,
    reason          TEXT,
    sbj_state       TEXT,
    head_name       TEXT
);

-- 13. Дані про взаємодію з держорганами (UO / FOP / FSU)
CREATE TABLE exchange_data (
    id              BIGSERIAL PRIMARY KEY,
    subject_type    TEXT NOT NULL, -- 'uo' | 'fop' | 'fsu' | 'branch'
    subject_record  TEXT NOT NULL,
    branch_code     TEXT,
    tax_payer_type  TEXT,
    start_date      TEXT,
    start_num       TEXT,
    end_date        TEXT,
    end_num         TEXT
);

-- Функція для створення індексів після імпорту
CREATE OR REPLACE FUNCTION create_edr_indexes() RETURNS void AS $$
BEGIN
    -- Індекси за ЄДРПОУ
    CREATE INDEX IF NOT EXISTS idx_uo_edrpou ON uo_subjects(edrpou);
    CREATE INDEX IF NOT EXISTS idx_fsu_edrpou ON fsu_subjects(edrpou);
    CREATE INDEX IF NOT EXISTS idx_branches_code ON branches(code);

    -- Індекси за ПІБ / Найменуванням
    CREATE INDEX IF NOT EXISTS idx_fop_name ON fop_subjects(name);
    CREATE INDEX IF NOT EXISTS idx_uo_name ON uo_subjects(name);
    CREATE INDEX IF NOT EXISTS idx_fsu_name ON fsu_subjects(name);

    -- Індекси зв'язків (subject_record)
    CREATE INDEX IF NOT EXISTS idx_founders_record ON founders(subject_record);
    CREATE INDEX IF NOT EXISTS idx_beneficiaries_record ON beneficiaries(subject_record);
    CREATE INDEX IF NOT EXISTS idx_signers_record ON signers(subject_record);
    CREATE INDEX IF NOT EXISTS idx_members_record ON members(subject_record);
    CREATE INDEX IF NOT EXISTS idx_branches_record ON branches(subject_record);
    CREATE INDEX IF NOT EXISTS idx_predecessors_record ON predecessors(subject_record);
    CREATE INDEX IF NOT EXISTS idx_assignees_record ON assignees(subject_record);
    CREATE INDEX IF NOT EXISTS idx_termination_record ON termination_started(subject_record);
    CREATE INDEX IF NOT EXISTS idx_bankruptcy_record ON bankruptcy_readjustment(subject_record);
    CREATE INDEX IF NOT EXISTS idx_exchange_record ON exchange_data(subject_record);

    -- Індекси стану
    CREATE INDEX IF NOT EXISTS idx_uo_stan ON uo_subjects(stan);
    CREATE INDEX IF NOT EXISTS idx_fop_stan ON fop_subjects(stan);
    CREATE INDEX IF NOT EXISTS idx_fsu_stan ON fsu_subjects(stan);
END;
$$ LANGUAGE plpgsql;
