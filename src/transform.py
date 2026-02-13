from src.db import execute_sql_script
import os
import logging

# Import lookup table functions
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scripts.add_lookups import create_lookups
from scripts.ingest_formulas import ingest_formulas
from scripts.ingest_labels import ingest_labels

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get the project root directory (parent of src/)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def main():
    
    logger.info("Loading lookup tables...")
    
    # Load lookup tables BEFORE running SQL transformations
    try:
        create_lookups()
        logger.info("✅ Lookup tables created successfully")
    except Exception as e:
        logger.error(f"Error creating lookup tables: {e}")
        raise
    
    try:
        ingest_formulas()
        logger.info("✅ Payment formulas ingested successfully")
    except Exception as e:
        logger.error(f"Error ingesting formulas: {e}")
        raise
    
    try:
        ingest_labels()
        logger.info("✅ Variable labels ingested successfully")
    except Exception as e:
        logger.error(f"Error ingesting labels: {e}")
        raise
    
    logger.info("Starting Phase 1 SQL transformations.")
    sql_script = """CREATE OR REPLACE FUNCTION sigma_level(p_yield) AS (
            -- This is a standard approximation for the Inverse Normal Distribution
            -- combined with your 1.5 Sigma Shift
            (5.5556 * (1 - POWER(1 - p_yield, 0.1186))) + 1.5
        );

        DROP VIEW IF EXISTS vw_db_schema;
        CREATE VIEW vw_db_schema as 
        SELECT 
            table_schema, 
            table_name, 
            column_name, 
            data_type, 
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, ordinal_position;

        -- Compare row counts for both table sets
        DROP VIEW IF EXISTS vw_source_counts;
        CREATE VIEW vw_source_counts as
        SELECT 
            'beneficiary_summary' AS table_name,
            (SELECT COUNT(*) FROM src_beneficiary_summary) AS src_count,
            (SELECT COUNT(*) FROM new_beneficiary_summary) AS new_count,
            (SELECT COUNT(*) FROM new_beneficiary_summary) - (SELECT COUNT(*) FROM src_beneficiary_summary) AS diff
        UNION ALL
        SELECT 
            'carrier_claims',
            (SELECT COUNT(*) FROM src_carrier_claims),
            (SELECT COUNT(*) FROM new_carrier_claims),
            (SELECT COUNT(*) FROM new_carrier_claims) - (SELECT COUNT(*) FROM src_carrier_claims) AS diff;"""
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Phase 1 SQL transformations completed successfully")
    except Exception as e:
        logger.error(f"Error running Phase 1 SQL transformations: {e}")
        raise

    logger.info("Starting Phase 2 SQL transformations.")
    sql_script = """/*BENEFICIARY DIFFERENCES*/

        CREATE OR REPLACE VIEW vw_beneficiary_errors AS
        SELECT
            s.desynpuf_id as DESYNPUF_ID
            ,case when s.BENE_DEATH_DT = 'nan' then '' else left(s.BENE_DEATH_DT,4) end as BENE_DEATH_YEAR
            ,s.year::text as "YEAR"
            ,'Error: Beneficiary Missing in New File' as FINDING
        FROM
            src_beneficiary_summary s
        LEFT JOIN new_beneficiary_summary n 
        ON
            s.desynpuf_id = n.desynpuf_id
            AND s.year = n.year
        WHERE
            (n.desynpuf_id IS NULL and s.bene_death_dt= 'nan') 
            or (n.desynpuf_id IS NULL and left(s.bene_death_dt,4) <= s."YEAR"::text)
        UNION ALL
        SELECT
            n.desynpuf_id as DESYNPUF_ID
            ,case when n.BENE_DEATH_DT = 'nan' then '' else left(n.BENE_DEATH_DT,4) end as BENE_DEATH_YEAR
            ,n.year::text as "YEAR"
            ,'Error: Beneficiary Present in New File, Not in Original File' as FINDING
        FROM
            new_beneficiary_summary n
        LEFT JOIN src_beneficiary_summary s 
        ON
            n.desynpuf_id = s.desynpuf_id
            AND n.year = s.year
        WHERE
            (s.desynpuf_id IS NULL and n.bene_death_dt= 'nan') 
            or (s.desynpuf_id IS NULL and left(n.bene_death_dt,4) <= n."YEAR"::text);

        CREATE OR REPLACE VIEW vw_beneficiary_attribute_errors AS
        SELECT 
            s.desynpuf_id
            , s.year
            , s.bene_birth_dt as src_BENE_BIRTH_DT
            , n.bene_birth_dt as new_BENE_BIRTH_DT
            , s.bene_sex_ident_cd as src_BENE_SEX_IDENT_CD
            , n.bene_sex_ident_cd as new_BENE_SEX_IDENT_CD
            , s.bene_RACE_CD as src_BENE_RACE_CD
            , n.bene_RACE_CD as new_BENE_RACE_CD
            , s.bene_esrd_ind as src_ERSD_IND
            , n.bene_esrd_ind as new_ERSD_IND
            ,'Error: Beneficiary Attributes Changed'
        FROM
            src_beneficiary_summary s
        JOIN new_beneficiary_summary n 
        ON
            s.desynpuf_id = n.desynpuf_id
            AND s.year = n.year
        WHERE 
            s.bene_birth_dt IS DISTINCT FROM n.bene_birth_dt
            OR s.bene_sex_ident_cd IS DISTINCT FROM n.bene_sex_ident_cd
            OR s.bene_race_cd is distinct from n.bene_race_cd
            OR s.bene_esrd_ind IS DISTINCT FROM n.bene_esrd_ind;

        DROP VIEW IF EXISTS vw_bene_dt_differences;
        CREATE VIEW vw_bene_dt_differences as
        SELECT s.DESYNPUF_ID, s.BENE_BIRTH_DT as src_dob, 
                n.BENE_BIRTH_DT as new_dob,
                s.BENE_DEATH_DT as src_dod,
                n.BENE_DEATH_DT as new_dod       
            FROM src_beneficiary_summary s
            JOIN new_beneficiary_summary n ON s.DESYNPUF_ID = n.DESYNPUF_ID AND s."YEAR" = n."YEAR"
            WHERE s.BENE_BIRTH_DT <> n.BENE_BIRTH_DT
                OR s.BENE_DEATH_DT <> n.BENE_DEATH_DT;

        DROP VIEW IF EXISTS vw_beneficiary_lines_not_identical;
        CREATE VIEW vw_beneficiary_lines_not_identical as
        -- This returns all rows that are NOT identical across all columns
        (SELECT * FROM src_beneficiary_summary EXCEPT SELECT * FROM new_beneficiary_summary)
        UNION ALL
        (SELECT * FROM new_beneficiary_summary EXCEPT SELECT * FROM src_beneficiary_summary);

        DROP TABLE IF EXISTS audit_beneficiary_summary;
        CREATE TABLE audit_beneficiary_summary AS
        WITH KEYS AS (
            SELECT DISTINCT
                DESYNPUF_ID
                , "YEAR"
            FROM
                data_eng.main.src_beneficiary_summary
            UNION 
            SELECT DISTINCT
                DESYNPUF_ID
                , "YEAR"
            FROM 
                data_eng.main.new_beneficiary_summary        
        ),
        src_ as (
            SELECT
                DESYNPUF_ID
                , "YEAR"
                , BENE_BIRTH_DT
                , BENE_DEATH_DT
                , BENE_SEX_IDENT_CD
                , BENE_RACE_CD
                , BENE_ESRD_IND
                , SP_STATE_CODE
                , BENE_COUNTY_CD
                , BENE_HI_CVRAGE_TOT_MONS
                , BENE_SMI_CVRAGE_TOT_MONS
                , BENE_HMO_CVRAGE_TOT_MONS
                , PLAN_CVRG_MOS_NUM
                , SP_ALZHDMTA
                , SP_CHF
                , SP_CHRNKIDN
                , SP_CNCR
                , SP_COPD
                , SP_DEPRESSN
                , SP_DIABETES
                , SP_ISCHMCHT
                , SP_OSTEOPRS
                , SP_RA_OA
                , SP_STRKETIA
                , MEDREIMB_IP
                , BENRES_IP
                , PPPYMT_IP
                , MEDREIMB_OP
                , BENRES_OP
                , PPPYMT_OP
                , MEDREIMB_CAR
                , BENRES_CAR
                , PPPYMT_CAR
            FROM
                data_eng.main.src_beneficiary_summary
        ),
        new_ as (
            SELECT
                DESYNPUF_ID
                , "YEAR"
                , BENE_BIRTH_DT
                , BENE_DEATH_DT
                , BENE_SEX_IDENT_CD
                , BENE_RACE_CD
                , BENE_ESRD_IND
                , SP_STATE_CODE
                , BENE_COUNTY_CD
                , BENE_HI_CVRAGE_TOT_MONS
                , BENE_SMI_CVRAGE_TOT_MONS
                , BENE_HMO_CVRAGE_TOT_MONS
                , PLAN_CVRG_MOS_NUM
                , SP_ALZHDMTA
                , SP_CHF
                , SP_CHRNKIDN
                , SP_CNCR
                , SP_COPD
                , SP_DEPRESSN
                , SP_DIABETES
                , SP_ISCHMCHT
                , SP_OSTEOPRS
                , SP_RA_OA
                , SP_STRKETIA
                , MEDREIMB_IP
                , BENRES_IP
                , PPPYMT_IP
                , MEDREIMB_OP
                , BENRES_OP
                , PPPYMT_OP
                , MEDREIMB_CAR
                , BENRES_CAR
                , PPPYMT_CAR
            FROM
                data_eng.main.new_beneficiary_summary
        )
        SELECT
                k.DESYNPUF_ID
                , k."YEAR"
                -- Demographics & Geography (VARCHAR)
                , CASE WHEN COALESCE(s.BENE_BIRTH_DT, '') <> COALESCE(n.BENE_BIRTH_DT, '') THEN 1 ELSE 0 END AS BENE_BIRTH_DT
                , CASE WHEN COALESCE(s.BENE_DEATH_DT, '') <> COALESCE(n.BENE_DEATH_DT, '') THEN 1 ELSE 0 END AS BENE_DEATH_DT
                , CASE WHEN COALESCE(s.BENE_SEX_IDENT_CD, '') <> COALESCE(n.BENE_SEX_IDENT_CD, '') THEN 1 ELSE 0 END AS BENE_SEX_IDENT_CD
                , CASE WHEN COALESCE(s.BENE_RACE_CD, '') <> COALESCE(n.BENE_RACE_CD, '') THEN 1 ELSE 0 END AS BENE_RACE_CD
                , CASE WHEN COALESCE(s.BENE_ESRD_IND, '') <> COALESCE(n.BENE_ESRD_IND, '') THEN 1 ELSE 0 END AS BENE_ESRD_IND
                , CASE WHEN COALESCE(s.SP_STATE_CODE, '') <> COALESCE(n.SP_STATE_CODE, '') THEN 1 ELSE 0 END AS SP_STATE_CODE
                , CASE WHEN COALESCE(s.BENE_COUNTY_CD, '') <> COALESCE(n.BENE_COUNTY_CD, '') THEN 1 ELSE 0 END AS BENE_COUNTY_CD
                
                -- Coverage Months (INTEGER)
                , CASE WHEN COALESCE(s.BENE_HI_CVRAGE_TOT_MONS, 0) <> COALESCE(n.BENE_HI_CVRAGE_TOT_MONS, 0) THEN 1 ELSE 0 END AS BENE_HI_CVRAGE_TOT_MONS
                , CASE WHEN COALESCE(s.BENE_SMI_CVRAGE_TOT_MONS, 0) <> COALESCE(n.BENE_SMI_CVRAGE_TOT_MONS, 0) THEN 1 ELSE 0 END AS BENE_SMI_CVRAGE_TOT_MONS
                , CASE WHEN COALESCE(s.BENE_HMO_CVRAGE_TOT_MONS, 0) <> COALESCE(n.BENE_HMO_CVRAGE_TOT_MONS, 0) THEN 1 ELSE 0 END AS BENE_HMO_CVRAGE_TOT_MONS
                , CASE WHEN COALESCE(s.PLAN_CVRG_MOS_NUM, 0) <> COALESCE(n.PLAN_CVRG_MOS_NUM, 0) THEN 1 ELSE 0 END AS PLAN_CVRG_MOS_NUM
                
                -- Chronic Conditions (VARCHAR Flags)
                , CASE WHEN COALESCE(s.SP_ALZHDMTA, '') <> COALESCE(n.SP_ALZHDMTA, '') THEN 1 ELSE 0 END AS SP_ALZHDMTA
                , CASE WHEN COALESCE(s.SP_CHF, '') <> COALESCE(n.SP_CHF, '') THEN 1 ELSE 0 END AS SP_CHF
                , CASE WHEN COALESCE(s.SP_CHRNKIDN, '') <> COALESCE(n.SP_CHRNKIDN, '') THEN 1 ELSE 0 END AS SP_CHRNKIDN
                , CASE WHEN COALESCE(s.SP_CNCR, '') <> COALESCE(n.SP_CNCR, '') THEN 1 ELSE 0 END AS SP_CNCR
                , CASE WHEN COALESCE(s.SP_COPD, '') <> COALESCE(n.SP_COPD, '') THEN 1 ELSE 0 END AS SP_COPD
                , CASE WHEN COALESCE(s.SP_DEPRESSN, '') <> COALESCE(n.SP_DEPRESSN, '') THEN 1 ELSE 0 END AS SP_DEPRESSN
                , CASE WHEN COALESCE(s.SP_DIABETES, '') <> COALESCE(n.SP_DIABETES, '') THEN 1 ELSE 0 END AS SP_DIABETES
                , CASE WHEN COALESCE(s.SP_ISCHMCHT, '') <> COALESCE(n.SP_ISCHMCHT, '') THEN 1 ELSE 0 END AS SP_ISCHMCHT
                , CASE WHEN COALESCE(s.SP_OSTEOPRS, '') <> COALESCE(n.SP_OSTEOPRS, '') THEN 1 ELSE 0 END AS SP_OSTEOPRS
                , CASE WHEN COALESCE(s.SP_RA_OA, '') <> COALESCE(n.SP_RA_OA, '') THEN 1 ELSE 0 END AS SP_RA_OA
                , CASE WHEN COALESCE(s.SP_STRKETIA, '') <> COALESCE(n.SP_STRKETIA, '') THEN 1 ELSE 0 END AS SP_STRKETIA
                
                -- Financials (FLOAT/DOUBLE)
                , CASE WHEN COALESCE(s.MEDREIMB_IP, 0) <> COALESCE(n.MEDREIMB_IP, 0) THEN 1 ELSE 0 END AS MEDREIMB_IP
                , CASE WHEN COALESCE(s.BENRES_IP, 0) <> COALESCE(n.BENRES_IP, 0) THEN 1 ELSE 0 END AS BENRES_IP
                , CASE WHEN COALESCE(s.PPPYMT_IP, 0) <> COALESCE(n.PPPYMT_IP, 0) THEN 1 ELSE 0 END AS PPPYMT_IP
                , CASE WHEN COALESCE(s.MEDREIMB_OP, 0) <> COALESCE(n.MEDREIMB_OP, 0) THEN 1 ELSE 0 END AS MEDREIMB_OP
                , CASE WHEN COALESCE(s.BENRES_OP, 0) <> COALESCE(n.BENRES_OP, 0) THEN 1 ELSE 0 END AS BENRES_OP
                , CASE WHEN COALESCE(s.PPPYMT_OP, 0) <> COALESCE(n.PPPYMT_OP, 0) THEN 1 ELSE 0 END AS PPPYMT_OP
                , CASE WHEN COALESCE(s.MEDREIMB_CAR, 0) <> COALESCE(n.MEDREIMB_CAR, 0) THEN 1 ELSE 0 END AS MEDREIMB_CAR
                , CASE WHEN COALESCE(s.BENRES_CAR, 0) <> COALESCE(n.BENRES_CAR, 0) THEN 1 ELSE 0 END AS BENRES_CAR
                , CASE WHEN COALESCE(s.PPPYMT_CAR, 0) <> COALESCE(n.PPPYMT_CAR, 0) THEN 1 ELSE 0 END AS PPPYMT_CAR
            FROM
                keys k 
        LEFT JOIN src_ s ON k.DESYNPUF_ID = s.DESYNPUF_ID AND k."YEAR" = s."YEAR"
        LEFT JOIN new_ n ON k.DESYNPUF_ID = n.DESYNPUF_ID AND k."YEAR" = n."YEAR";
        -- CREATE INDEX for performance
        CREATE INDEX idx_audit_beneficiary_desynpuf_year ON audit_beneficiary_summary(DESYNPUF_ID, "YEAR");
        ANALYZE audit_beneficiary_summary;

        DROP TABLE IF EXISTS audit_beneficiary_financials;
        CREATE TABLE audit_beneficiary_financials AS
        WITH KEYS AS (
            SELECT DISTINCT
                DESYNPUF_ID
                , "YEAR"
                , BENE_BIRTH_DT
                , BENE_DEATH_DT
                , BENE_SEX_IDENT_CD
                , BENE_RACE_CD
            FROM
                data_eng.main.src_beneficiary_summary
            UNION 
            SELECT DISTINCT
                DESYNPUF_ID
                , "YEAR"
                , BENE_BIRTH_DT
                , BENE_DEATH_DT
                , BENE_SEX_IDENT_CD
                , BENE_RACE_CD
            FROM
                data_eng.main.new_beneficiary_summary
        ),
        src_ AS (
            SELECT
                DESYNPUF_ID
                , "YEAR"
                , BENE_BIRTH_DT
                , BENE_DEATH_DT
                , BENE_SEX_IDENT_CD
                , BENE_RACE_CD
                , CASE WHEN isnan(MEDREIMB_IP)  THEN 0 ELSE coalesce(MEDREIMB_IP,  0) END AS MEDREIMB_IP
                , CASE WHEN isnan(BENRES_IP)    THEN 0 ELSE coalesce(BENRES_IP,    0) END AS BENRES_IP
                , CASE WHEN isnan(PPPYMT_IP)    THEN 0 ELSE coalesce(PPPYMT_IP,    0) END AS PPPYMT_IP
                , CASE WHEN isnan(MEDREIMB_OP)  THEN 0 ELSE coalesce(MEDREIMB_OP,  0) END AS MEDREIMB_OP
                , CASE WHEN isnan(BENRES_OP)    THEN 0 ELSE coalesce(BENRES_OP,    0) END AS BENRES_OP
                , CASE WHEN isnan(PPPYMT_OP)    THEN 0 ELSE coalesce(PPPYMT_OP,    0) END AS PPPYMT_OP
                , CASE WHEN isnan(MEDREIMB_CAR) THEN 0 ELSE coalesce(MEDREIMB_CAR, 0) END AS MEDREIMB_CAR
                , CASE WHEN isnan(BENRES_CAR)   THEN 0 ELSE coalesce(BENRES_CAR,   0) END AS BENRES_CAR
                , CASE WHEN isnan(PPPYMT_CAR)   THEN 0 ELSE coalesce(PPPYMT_CAR,   0) END AS PPPYMT_CAR
            FROM data_eng.main.src_beneficiary_summary
        ),
        new_ AS (
            SELECT
                DESYNPUF_ID
                , "YEAR"
                , BENE_BIRTH_DT
                , BENE_DEATH_DT
                , BENE_SEX_IDENT_CD
                , BENE_RACE_CD
                , CASE WHEN isnan(MEDREIMB_IP)  THEN 0 ELSE coalesce(MEDREIMB_IP,  0) END AS MEDREIMB_IP
                , CASE WHEN isnan(BENRES_IP)    THEN 0 ELSE coalesce(BENRES_IP,    0) END AS BENRES_IP
                , CASE WHEN isnan(PPPYMT_IP)    THEN 0 ELSE coalesce(PPPYMT_IP,    0) END AS PPPYMT_IP
                , CASE WHEN isnan(MEDREIMB_OP)  THEN 0 ELSE coalesce(MEDREIMB_OP,  0) END AS MEDREIMB_OP
                , CASE WHEN isnan(BENRES_OP)    THEN 0 ELSE coalesce(BENRES_OP,    0) END AS BENRES_OP
                , CASE WHEN isnan(PPPYMT_OP)    THEN 0 ELSE coalesce(PPPYMT_OP,    0) END AS PPPYMT_OP
                , CASE WHEN isnan(MEDREIMB_CAR) THEN 0 ELSE coalesce(MEDREIMB_CAR, 0) END AS MEDREIMB_CAR
                , CASE WHEN isnan(BENRES_CAR)   THEN 0 ELSE coalesce(BENRES_CAR,   0) END AS BENRES_CAR
                , CASE WHEN isnan(PPPYMT_CAR)   THEN 0 ELSE coalesce(PPPYMT_CAR,   0) END AS PPPYMT_CAR
            FROM data_eng.main.new_beneficiary_summary
        )
        SELECT
                k.DESYNPUF_ID
                , k."YEAR"
                , k.BENE_BIRTH_DT
                , k.BENE_DEATH_DT
                , k.BENE_SEX_IDENT_CD
                , k.BENE_RACE_CD
                , s.MEDREIMB_IP as src_MEDREIMB_IP
                , n.MEDREIMB_IP as new_MEDREIMB_IP
                , n.MEDREIMB_IP - s.MEDREIMB_IP as delta_MEDREIMB_IP
                , abs(n.MEDREIMB_IP - s.MEDREIMB_IP) as abs_delta_MEDREIMB_IP
                , s.BENRES_IP as src_BENRES_IP
                , n.BENRES_IP as new_BENRES_IP
                , n.BENRES_IP - s.BENRES_IP as delta_BENRES_IP
                , abs(n.BENRES_IP - s.BENRES_IP) as abs_delta_BENRES_IP
                , s.PPPYMT_IP as src_PPPYMT_IP
                , n.PPPYMT_IP as new_PPPYMT_IP
                , n.PPPYMT_IP - s.PPPYMT_IP as delta_PPPYMT_IP
                , abs(n.PPPYMT_IP - s.PPPYMT_IP) as abs_delta_PPPYMT_IP
                , s.MEDREIMB_OP as src_MEDREIMB_OP
                , n.MEDREIMB_OP as new_MEDREIMB_OP
                , n.MEDREIMB_OP - s.MEDREIMB_OP as delta_MEDREIMB_OP
                , abs(n.MEDREIMB_OP - s.MEDREIMB_OP) as abs_delta_MEDREIMB_OP
                , s.BENRES_OP as src_BENRES_OP
                , n.BENRES_OP as new_BENRES_OP
                , n.BENRES_OP - s.BENRES_OP as delta_BENRES_OP
                , abs(n.BENRES_OP - s.BENRES_OP) as abs_delta_BENRES_OP
                , s.PPPYMT_OP as src_PPPYMT_OP
                , n.PPPYMT_OP as new_PPPYMT_OP
                , n.PPPYMT_OP - s.PPPYMT_OP as delta_PPPYMT_OP
                , abs(n.PPPYMT_OP - s.PPPYMT_OP) as abs_delta_PPPYMT_OP
                , s.MEDREIMB_CAR as src_MEDREIMB_CAR
                , n.MEDREIMB_CAR as new_MEDREIMB_CAR
                , n.MEDREIMB_CAR - s.MEDREIMB_CAR as delta_MEDREIMB_CAR
                , abs(n.MEDREIMB_CAR - s.MEDREIMB_CAR) as abs_delta_MEDREIMB_CAR
                , s.BENRES_CAR as src_BENRES_CAR
                , n.BENRES_CAR as new_BENRES_CAR
                , n.BENRES_CAR - s.BENRES_CAR as delta_BENRES_CAR
                , abs(n.BENRES_CAR - s.BENRES_CAR) as abs_delta_BENRES_CAR
                , s.PPPYMT_CAR as src_PPPYMT_CAR
                , n.PPPYMT_CAR as new_PPPYMT_CAR
                , n.PPPYMT_CAR - s.PPPYMT_CAR as delta_PPPYMT_CAR
                , abs(n.PPPYMT_CAR - s.PPPYMT_CAR) as abs_delta_PPPYMT_CAR
            FROM
                keys k 
        LEFT JOIN src_ s ON k.DESYNPUF_ID = s.DESYNPUF_ID 
                AND k."YEAR" = s."YEAR" 
                AND k.BENE_BIRTH_DT = s.BENE_BIRTH_DT 
                AND k.BENE_DEATH_DT = s.BENE_DEATH_DT
                AND k.BENE_SEX_IDENT_CD = s.BENE_SEX_IDENT_CD
                AND k.BENE_RACE_CD = s.BENE_RACE_CD
        LEFT JOIN new_ n ON k.DESYNPUF_ID = n.DESYNPUF_ID 
                AND k."YEAR" = n."YEAR" 
                AND k.BENE_BIRTH_DT = n.BENE_BIRTH_DT 
                AND k.BENE_DEATH_DT = n.BENE_DEATH_DT
                AND k.BENE_SEX_IDENT_CD = n.BENE_SEX_IDENT_CD
                AND k.BENE_RACE_CD = n.BENE_RACE_CD;
        CREATE INDEX idx_audit_beneficiary_financials_desynpuf_year ON audit_beneficiary_financials(DESYNPUF_ID, "YEAR");
        CREATE INDEX idx_audit_beneficiary_financials_desynpuf_year_bene_dts ON audit_beneficiary_financials(DESYNPUF_ID, "YEAR", BENE_BIRTH_DT, BENE_DEATH_DT);
        ANALYZE audit_beneficiary_financials;

        CREATE OR REPLACE VIEW vw_financial_differences_bene AS
        WITH agg AS (
            SELECT
                DESYNPUF_ID
                , "YEAR"
                , sum(abs_delta_MEDREIMB_IP)  as abs_delta_MEDREIMB_IP
                , sum(abs_delta_BENRES_IP)  as abs_delta_BENRES_IP
                , sum(abs_delta_PPPYMT_IP)  as abs_delta_PPPYMT_IP
                , sum(abs_delta_MEDREIMB_OP)  as abs_delta_MEDREIMB_OP
                , sum(abs_delta_BENRES_OP)  as abs_delta_BENRES_OP
                , sum(abs_delta_PPPYMT_OP)  as abs_delta_PPPYMT_OP
                , sum(abs_delta_MEDREIMB_CAR)  as abs_delta_MEDREIMB_CAR
                , sum(abs_delta_BENRES_CAR)  as abs_delta_BENRES_CAR
                , sum(abs_delta_PPPYMT_CAR)  as abs_delta_PPPYMT_CAR
            FROM data_eng.main.audit_beneficiary_financials
            GROUP BY 
                DESYNPUF_ID
                , "YEAR"
        )
        SELECT *
        FROM agg
        WHERE list_sum(list_value(
                abs_delta_MEDREIMB_IP,
                abs_delta_BENRES_IP,
                abs_delta_PPPYMT_IP,
                abs_delta_MEDREIMB_OP,
                abs_delta_BENRES_OP,
                abs_delta_PPPYMT_OP,
                abs_delta_MEDREIMB_CAR,
                abs_delta_BENRES_CAR,
                abs_delta_PPPYMT_CAR
        )) > 0;

        CREATE OR REPLACE TABLE audit_beneficiary_financial_fields AS
        WITH flat AS (
            SELECT
                [
                    struct_pack(metric_name := 'abs_delta_MEDREIMB_IP',  metric_value := abs_delta_MEDREIMB_IP),
                    struct_pack(metric_name := 'abs_delta_BENRES_IP',  metric_value := abs_delta_BENRES_IP),
                    struct_pack(metric_name := 'abs_delta_PPPYMT_IP',  metric_value := abs_delta_PPPYMT_IP),
                    struct_pack(metric_name := 'abs_delta_MEDREIMB_OP',  metric_value := abs_delta_MEDREIMB_OP),
                    struct_pack(metric_name := 'abs_delta_BENRES_OP',  metric_value := abs_delta_BENRES_OP),
                    struct_pack(metric_name := 'abs_delta_PPPYMT_OP',  metric_value := abs_delta_PPPYMT_OP),
                    struct_pack(metric_name := 'abs_delta_MEDREIMB_CAR',  metric_value := abs_delta_MEDREIMB_CAR),
                    struct_pack(metric_name := 'abs_delta_BENRES_CAR',  metric_value := abs_delta_BENRES_CAR),
                    struct_pack(metric_name := 'abs_delta_PPPYMT_CAR',  metric_value := abs_delta_PPPYMT_CAR)
                ] AS metrics
            FROM data_eng.main.vw_financial_differences_bene
        )
        SELECT
            unnest.metric_name,
            round(sum(unnest.metric_value),2) AS total_abs_delta
        FROM flat
        CROSS JOIN UNNEST(metrics)
        GROUP BY unnest.metric_name
        ORDER BY total_abs_delta DESC;
        ANALYZE audit_beneficiary_financial_fields;"""
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Phase 2 SQL transformations completed successfully")
    except Exception as e:
        logger.error(f"Error running Phase 2 SQL transformations: {e}")
        raise

    logger.info("Starting Phase 3 SQL transformations.")
    sql_script = """
        DROP TABLE IF EXISTS audit_carrier_claims;
        CREATE TABLE audit_carrier_claims AS
        WITH KEYS AS (
            SELECT DISTINCT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
            FROM
                data_eng.main.src_carrier_claims
            UNION 
            SELECT DISTINCT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
            FROM
                data_eng.main.new_carrier_claims
        ),
        src_ as (
            SELECT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
                , ICD9_DGNS_CD_1
                , ICD9_DGNS_CD_2
                , ICD9_DGNS_CD_3
                , ICD9_DGNS_CD_4
                , ICD9_DGNS_CD_5
                , ICD9_DGNS_CD_6
                , ICD9_DGNS_CD_7
                , ICD9_DGNS_CD_8
                , PRF_PHYSN_NPI_1
                , PRF_PHYSN_NPI_2
                , PRF_PHYSN_NPI_3
                , PRF_PHYSN_NPI_4
                , PRF_PHYSN_NPI_5
                , PRF_PHYSN_NPI_6
                , PRF_PHYSN_NPI_7
                , PRF_PHYSN_NPI_8
                , PRF_PHYSN_NPI_9
                , PRF_PHYSN_NPI_10
                , PRF_PHYSN_NPI_11
                , PRF_PHYSN_NPI_12
                , PRF_PHYSN_NPI_13
                , TAX_NUM_1
                , TAX_NUM_2
                , TAX_NUM_3
                , TAX_NUM_4
                , TAX_NUM_5
                , TAX_NUM_6
                , TAX_NUM_7
                , TAX_NUM_8
                , TAX_NUM_9
                , TAX_NUM_10
                , TAX_NUM_11
                , TAX_NUM_12
                , TAX_NUM_13
                , HCPCS_CD_1
                , HCPCS_CD_2
                , HCPCS_CD_3
                , HCPCS_CD_4
                , HCPCS_CD_5
                , HCPCS_CD_6
                , HCPCS_CD_7
                , HCPCS_CD_8
                , HCPCS_CD_9
                , HCPCS_CD_10
                , HCPCS_CD_11
                , HCPCS_CD_12
                , HCPCS_CD_13
                , LINE_NCH_PMT_AMT_1
                , LINE_NCH_PMT_AMT_2
                , LINE_NCH_PMT_AMT_3
                , LINE_NCH_PMT_AMT_4
                , LINE_NCH_PMT_AMT_5
                , LINE_NCH_PMT_AMT_6
                , LINE_NCH_PMT_AMT_7
                , LINE_NCH_PMT_AMT_8
                , LINE_NCH_PMT_AMT_9
                , LINE_NCH_PMT_AMT_10
                , LINE_NCH_PMT_AMT_11
                , LINE_NCH_PMT_AMT_12
                , LINE_NCH_PMT_AMT_13
                , LINE_BENE_PTB_DDCTBL_AMT_1
                , LINE_BENE_PTB_DDCTBL_AMT_2
                , LINE_BENE_PTB_DDCTBL_AMT_3
                , LINE_BENE_PTB_DDCTBL_AMT_4
                , LINE_BENE_PTB_DDCTBL_AMT_5
                , LINE_BENE_PTB_DDCTBL_AMT_6
                , LINE_BENE_PTB_DDCTBL_AMT_7
                , LINE_BENE_PTB_DDCTBL_AMT_8
                , LINE_BENE_PTB_DDCTBL_AMT_9
                , LINE_BENE_PTB_DDCTBL_AMT_10
                , LINE_BENE_PTB_DDCTBL_AMT_11
                , LINE_BENE_PTB_DDCTBL_AMT_12
                , LINE_BENE_PTB_DDCTBL_AMT_13
                , LINE_BENE_PRMRY_PYR_PD_AMT_1
                , LINE_BENE_PRMRY_PYR_PD_AMT_2
                , LINE_BENE_PRMRY_PYR_PD_AMT_3
                , LINE_BENE_PRMRY_PYR_PD_AMT_4
                , LINE_BENE_PRMRY_PYR_PD_AMT_5
                , LINE_BENE_PRMRY_PYR_PD_AMT_6
                , LINE_BENE_PRMRY_PYR_PD_AMT_7
                , LINE_BENE_PRMRY_PYR_PD_AMT_8
                , LINE_BENE_PRMRY_PYR_PD_AMT_9
                , LINE_BENE_PRMRY_PYR_PD_AMT_10
                , LINE_BENE_PRMRY_PYR_PD_AMT_11
                , LINE_BENE_PRMRY_PYR_PD_AMT_12
                , LINE_BENE_PRMRY_PYR_PD_AMT_13
                , LINE_COINSRNC_AMT_1
                , LINE_COINSRNC_AMT_2
                , LINE_COINSRNC_AMT_3
                , LINE_COINSRNC_AMT_4
                , LINE_COINSRNC_AMT_5
                , LINE_COINSRNC_AMT_6
                , LINE_COINSRNC_AMT_7
                , LINE_COINSRNC_AMT_8
                , LINE_COINSRNC_AMT_9
                , LINE_COINSRNC_AMT_10
                , LINE_COINSRNC_AMT_11
                , LINE_COINSRNC_AMT_12
                , LINE_COINSRNC_AMT_13
                , LINE_ALOWD_CHRG_AMT_1
                , LINE_ALOWD_CHRG_AMT_2
                , LINE_ALOWD_CHRG_AMT_3
                , LINE_ALOWD_CHRG_AMT_4
                , LINE_ALOWD_CHRG_AMT_5
                , LINE_ALOWD_CHRG_AMT_6
                , LINE_ALOWD_CHRG_AMT_7
                , LINE_ALOWD_CHRG_AMT_8
                , LINE_ALOWD_CHRG_AMT_9
                , LINE_ALOWD_CHRG_AMT_10
                , LINE_ALOWD_CHRG_AMT_11
                , LINE_ALOWD_CHRG_AMT_12
                , LINE_ALOWD_CHRG_AMT_13
                , LINE_PRCSG_IND_CD_1
                , LINE_PRCSG_IND_CD_2
                , LINE_PRCSG_IND_CD_3
                , LINE_PRCSG_IND_CD_4
                , LINE_PRCSG_IND_CD_5
                , LINE_PRCSG_IND_CD_6
                , LINE_PRCSG_IND_CD_7
                , LINE_PRCSG_IND_CD_8
                , LINE_PRCSG_IND_CD_9
                , LINE_PRCSG_IND_CD_10
                , LINE_PRCSG_IND_CD_11
                , LINE_PRCSG_IND_CD_12
                , LINE_PRCSG_IND_CD_13
                , LINE_ICD9_DGNS_CD_1
                , LINE_ICD9_DGNS_CD_2
                , LINE_ICD9_DGNS_CD_3
                , LINE_ICD9_DGNS_CD_4
                , LINE_ICD9_DGNS_CD_5
                , LINE_ICD9_DGNS_CD_6
                , LINE_ICD9_DGNS_CD_7
                , LINE_ICD9_DGNS_CD_8
                , LINE_ICD9_DGNS_CD_9
                , LINE_ICD9_DGNS_CD_10
                , LINE_ICD9_DGNS_CD_11
                , LINE_ICD9_DGNS_CD_12
                , LINE_ICD9_DGNS_CD_13
            FROM
                data_eng.main.src_carrier_claims
        ),
        new_ as (
            SELECT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
                , ICD9_DGNS_CD_1
                , ICD9_DGNS_CD_2
                , ICD9_DGNS_CD_3
                , ICD9_DGNS_CD_4
                , ICD9_DGNS_CD_5
                , ICD9_DGNS_CD_6
                , ICD9_DGNS_CD_7
                , ICD9_DGNS_CD_8
                , PRF_PHYSN_NPI_1
                , PRF_PHYSN_NPI_2
                , PRF_PHYSN_NPI_3
                , PRF_PHYSN_NPI_4
                , PRF_PHYSN_NPI_5
                , PRF_PHYSN_NPI_6
                , PRF_PHYSN_NPI_7
                , PRF_PHYSN_NPI_8
                , PRF_PHYSN_NPI_9
                , PRF_PHYSN_NPI_10
                , PRF_PHYSN_NPI_11
                , PRF_PHYSN_NPI_12
                , PRF_PHYSN_NPI_13
                , TAX_NUM_1
                , TAX_NUM_2
                , TAX_NUM_3
                , TAX_NUM_4
                , TAX_NUM_5
                , TAX_NUM_6
                , TAX_NUM_7
                , TAX_NUM_8
                , TAX_NUM_9
                , TAX_NUM_10
                , TAX_NUM_11
                , TAX_NUM_12
                , TAX_NUM_13
                , HCPCS_CD_1
                , HCPCS_CD_2
                , HCPCS_CD_3
                , HCPCS_CD_4
                , HCPCS_CD_5
                , HCPCS_CD_6
                , HCPCS_CD_7
                , HCPCS_CD_8
                , HCPCS_CD_9
                , HCPCS_CD_10
                , HCPCS_CD_11
                , HCPCS_CD_12
                , HCPCS_CD_13
                , LINE_NCH_PMT_AMT_1
                , LINE_NCH_PMT_AMT_2
                , LINE_NCH_PMT_AMT_3
                , LINE_NCH_PMT_AMT_4
                , LINE_NCH_PMT_AMT_5
                , LINE_NCH_PMT_AMT_6
                , LINE_NCH_PMT_AMT_7
                , LINE_NCH_PMT_AMT_8
                , LINE_NCH_PMT_AMT_9
                , LINE_NCH_PMT_AMT_10
                , LINE_NCH_PMT_AMT_11
                , LINE_NCH_PMT_AMT_12
                , LINE_NCH_PMT_AMT_13
                , LINE_BENE_PTB_DDCTBL_AMT_1
                , LINE_BENE_PTB_DDCTBL_AMT_2
                , LINE_BENE_PTB_DDCTBL_AMT_3
                , LINE_BENE_PTB_DDCTBL_AMT_4
                , LINE_BENE_PTB_DDCTBL_AMT_5
                , LINE_BENE_PTB_DDCTBL_AMT_6
                , LINE_BENE_PTB_DDCTBL_AMT_7
                , LINE_BENE_PTB_DDCTBL_AMT_8
                , LINE_BENE_PTB_DDCTBL_AMT_9
                , LINE_BENE_PTB_DDCTBL_AMT_10
                , LINE_BENE_PTB_DDCTBL_AMT_11
                , LINE_BENE_PTB_DDCTBL_AMT_12
                , LINE_BENE_PTB_DDCTBL_AMT_13
                , LINE_BENE_PRMRY_PYR_PD_AMT_1
                , LINE_BENE_PRMRY_PYR_PD_AMT_2
                , LINE_BENE_PRMRY_PYR_PD_AMT_3
                , LINE_BENE_PRMRY_PYR_PD_AMT_4
                , LINE_BENE_PRMRY_PYR_PD_AMT_5
                , LINE_BENE_PRMRY_PYR_PD_AMT_6
                , LINE_BENE_PRMRY_PYR_PD_AMT_7
                , LINE_BENE_PRMRY_PYR_PD_AMT_8
                , LINE_BENE_PRMRY_PYR_PD_AMT_9
                , LINE_BENE_PRMRY_PYR_PD_AMT_10
                , LINE_BENE_PRMRY_PYR_PD_AMT_11
                , LINE_BENE_PRMRY_PYR_PD_AMT_12
                , LINE_BENE_PRMRY_PYR_PD_AMT_13
                , LINE_COINSRNC_AMT_1
                , LINE_COINSRNC_AMT_2
                , LINE_COINSRNC_AMT_3
                , LINE_COINSRNC_AMT_4
                , LINE_COINSRNC_AMT_5
                , LINE_COINSRNC_AMT_6
                , LINE_COINSRNC_AMT_7
                , LINE_COINSRNC_AMT_8
                , LINE_COINSRNC_AMT_9
                , LINE_COINSRNC_AMT_10
                , LINE_COINSRNC_AMT_11
                , LINE_COINSRNC_AMT_12
                , LINE_COINSRNC_AMT_13
                , LINE_ALOWD_CHRG_AMT_1
                , LINE_ALOWD_CHRG_AMT_2
                , LINE_ALOWD_CHRG_AMT_3
                , LINE_ALOWD_CHRG_AMT_4
                , LINE_ALOWD_CHRG_AMT_5
                , LINE_ALOWD_CHRG_AMT_6
                , LINE_ALOWD_CHRG_AMT_7
                , LINE_ALOWD_CHRG_AMT_8
                , LINE_ALOWD_CHRG_AMT_9
                , LINE_ALOWD_CHRG_AMT_10
                , LINE_ALOWD_CHRG_AMT_11
                , LINE_ALOWD_CHRG_AMT_12
                , LINE_ALOWD_CHRG_AMT_13
                , LINE_PRCSG_IND_CD_1
                , LINE_PRCSG_IND_CD_2
                , LINE_PRCSG_IND_CD_3
                , LINE_PRCSG_IND_CD_4
                , LINE_PRCSG_IND_CD_5
                , LINE_PRCSG_IND_CD_6
                , LINE_PRCSG_IND_CD_7
                , LINE_PRCSG_IND_CD_8
                , LINE_PRCSG_IND_CD_9
                , LINE_PRCSG_IND_CD_10
                , LINE_PRCSG_IND_CD_11
                , LINE_PRCSG_IND_CD_12
                , LINE_PRCSG_IND_CD_13
                , LINE_ICD9_DGNS_CD_1
                , LINE_ICD9_DGNS_CD_2
                , LINE_ICD9_DGNS_CD_3
                , LINE_ICD9_DGNS_CD_4
                , LINE_ICD9_DGNS_CD_5
                , LINE_ICD9_DGNS_CD_6
                , LINE_ICD9_DGNS_CD_7
                , LINE_ICD9_DGNS_CD_8
                , LINE_ICD9_DGNS_CD_9
                , LINE_ICD9_DGNS_CD_10
                , LINE_ICD9_DGNS_CD_11
                , LINE_ICD9_DGNS_CD_12
                , LINE_ICD9_DGNS_CD_13
            FROM
                data_eng.main.new_carrier_claims
        )
        SELECT
                k.DESYNPUF_ID
                , k.CLM_ID
                , k.CLM_FROM_DT
                , k.CLM_THRU_DT
                , CASE WHEN COALESCE(s.CLM_FROM_DT, '') <> COALESCE(n.CLM_FROM_DT, '') THEN 1 ELSE 0 END AS CLM_FROM_DT
                , CASE WHEN COALESCE(s.CLM_THRU_DT, '') <> COALESCE(n.CLM_THRU_DT, '') THEN 1 ELSE 0 END AS CLM_THRU_DT
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_1, '') <> COALESCE(n.ICD9_DGNS_CD_1, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_1
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_2, '') <> COALESCE(n.ICD9_DGNS_CD_2, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_2
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_3, '') <> COALESCE(n.ICD9_DGNS_CD_3, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_3
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_4, '') <> COALESCE(n.ICD9_DGNS_CD_4, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_4
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_5, '') <> COALESCE(n.ICD9_DGNS_CD_5, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_5
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_6, '') <> COALESCE(n.ICD9_DGNS_CD_6, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_6
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_7, '') <> COALESCE(n.ICD9_DGNS_CD_7, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_7
                , CASE WHEN COALESCE(s.ICD9_DGNS_CD_8, '') <> COALESCE(n.ICD9_DGNS_CD_8, '') THEN 1 ELSE 0 END AS ICD9_DGNS_CD_8
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_1, '') <> COALESCE(n.PRF_PHYSN_NPI_1, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_1
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_2, '') <> COALESCE(n.PRF_PHYSN_NPI_2, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_2
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_3, '') <> COALESCE(n.PRF_PHYSN_NPI_3, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_3
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_4, '') <> COALESCE(n.PRF_PHYSN_NPI_4, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_4
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_5, '') <> COALESCE(n.PRF_PHYSN_NPI_5, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_5
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_6, '') <> COALESCE(n.PRF_PHYSN_NPI_6, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_6
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_7, '') <> COALESCE(n.PRF_PHYSN_NPI_7, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_7
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_8, '') <> COALESCE(n.PRF_PHYSN_NPI_8, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_8
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_9, '') <> COALESCE(n.PRF_PHYSN_NPI_9, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_9
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_10, '') <> COALESCE(n.PRF_PHYSN_NPI_10, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_10
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_11, '') <> COALESCE(n.PRF_PHYSN_NPI_11, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_11
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_12, '') <> COALESCE(n.PRF_PHYSN_NPI_12, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_12
                , CASE WHEN COALESCE(s.PRF_PHYSN_NPI_13, '') <> COALESCE(n.PRF_PHYSN_NPI_13, '') THEN 1 ELSE 0 END AS PRF_PHYSN_NPI_13
                , CASE WHEN COALESCE(s.TAX_NUM_1, '') <> COALESCE(n.TAX_NUM_1, '') THEN 1 ELSE 0 END AS TAX_NUM_1
                , CASE WHEN COALESCE(s.TAX_NUM_2, '') <> COALESCE(n.TAX_NUM_2, '') THEN 1 ELSE 0 END AS TAX_NUM_2
                , CASE WHEN COALESCE(s.TAX_NUM_3, '') <> COALESCE(n.TAX_NUM_3, '') THEN 1 ELSE 0 END AS TAX_NUM_3
                , CASE WHEN COALESCE(s.TAX_NUM_4, '') <> COALESCE(n.TAX_NUM_4, '') THEN 1 ELSE 0 END AS TAX_NUM_4
                , CASE WHEN COALESCE(s.TAX_NUM_5, '') <> COALESCE(n.TAX_NUM_5, '') THEN 1 ELSE 0 END AS TAX_NUM_5
                , CASE WHEN COALESCE(s.TAX_NUM_6, '') <> COALESCE(n.TAX_NUM_6, '') THEN 1 ELSE 0 END AS TAX_NUM_6
                , CASE WHEN COALESCE(s.TAX_NUM_7, '') <> COALESCE(n.TAX_NUM_7, '') THEN 1 ELSE 0 END AS TAX_NUM_7
                , CASE WHEN COALESCE(s.TAX_NUM_8, '') <> COALESCE(n.TAX_NUM_8, '') THEN 1 ELSE 0 END AS TAX_NUM_8
                , CASE WHEN COALESCE(s.TAX_NUM_9, '') <> COALESCE(n.TAX_NUM_9, '') THEN 1 ELSE 0 END AS TAX_NUM_9
                , CASE WHEN COALESCE(s.TAX_NUM_10, '') <> COALESCE(n.TAX_NUM_10, '') THEN 1 ELSE 0 END AS TAX_NUM_10
                , CASE WHEN COALESCE(s.TAX_NUM_11, '') <> COALESCE(n.TAX_NUM_11, '') THEN 1 ELSE 0 END AS TAX_NUM_11
                , CASE WHEN COALESCE(s.TAX_NUM_12, '') <> COALESCE(n.TAX_NUM_12, '') THEN 1 ELSE 0 END AS TAX_NUM_12
                , CASE WHEN COALESCE(s.TAX_NUM_13, '') <> COALESCE(n.TAX_NUM_13, '') THEN 1 ELSE 0 END AS TAX_NUM_13
                , CASE WHEN COALESCE(s.HCPCS_CD_1, '') <> COALESCE(n.HCPCS_CD_1, '') THEN 1 ELSE 0 END AS HCPCS_CD_1
                , CASE WHEN COALESCE(s.HCPCS_CD_2, '') <> COALESCE(n.HCPCS_CD_2, '') THEN 1 ELSE 0 END AS HCPCS_CD_2
                , CASE WHEN COALESCE(s.HCPCS_CD_3, '') <> COALESCE(n.HCPCS_CD_3, '') THEN 1 ELSE 0 END AS HCPCS_CD_3
                , CASE WHEN COALESCE(s.HCPCS_CD_4, '') <> COALESCE(n.HCPCS_CD_4, '') THEN 1 ELSE 0 END AS HCPCS_CD_4
                , CASE WHEN COALESCE(s.HCPCS_CD_5, '') <> COALESCE(n.HCPCS_CD_5, '') THEN 1 ELSE 0 END AS HCPCS_CD_5
                , CASE WHEN COALESCE(s.HCPCS_CD_6, '') <> COALESCE(n.HCPCS_CD_6, '') THEN 1 ELSE 0 END AS HCPCS_CD_6
                , CASE WHEN COALESCE(s.HCPCS_CD_7, '') <> COALESCE(n.HCPCS_CD_7, '') THEN 1 ELSE 0 END AS HCPCS_CD_7
                , CASE WHEN COALESCE(s.HCPCS_CD_8, '') <> COALESCE(n.HCPCS_CD_8, '') THEN 1 ELSE 0 END AS HCPCS_CD_8
                , CASE WHEN COALESCE(s.HCPCS_CD_9, '') <> COALESCE(n.HCPCS_CD_9, '') THEN 1 ELSE 0 END AS HCPCS_CD_9
                , CASE WHEN COALESCE(s.HCPCS_CD_10, '') <> COALESCE(n.HCPCS_CD_10, '') THEN 1 ELSE 0 END AS HCPCS_CD_10
                , CASE WHEN COALESCE(s.HCPCS_CD_11, '') <> COALESCE(n.HCPCS_CD_11, '') THEN 1 ELSE 0 END AS HCPCS_CD_11
                , CASE WHEN COALESCE(s.HCPCS_CD_12, '') <> COALESCE(n.HCPCS_CD_12, '') THEN 1 ELSE 0 END AS HCPCS_CD_12
                , CASE WHEN COALESCE(s.HCPCS_CD_13, '') <> COALESCE(n.HCPCS_CD_13, '') THEN 1 ELSE 0 END AS HCPCS_CD_13
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_1, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_1, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_1
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_2, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_2, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_2
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_3, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_3, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_3
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_4, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_4, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_4
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_5, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_5, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_5
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_6, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_6, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_6
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_7, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_7, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_7
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_8, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_8, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_8
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_9, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_9, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_9
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_10, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_10, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_10
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_11, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_11, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_11
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_12, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_12, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_12
                , CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_13, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_13, 0) THEN 1 ELSE 0 END AS LINE_NCH_PMT_AMT_13
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_1, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_1, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_1
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_2, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_2, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_2
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_3, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_3, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_3
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_4, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_4, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_4
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_5, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_5, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_5
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_6, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_6, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_6
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_7, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_7, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_7
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_8, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_8, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_8
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_9, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_9, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_9
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_10, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_10, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_10
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_11, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_11, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_11
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_12, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_12, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_12
                , CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_13, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_13, 0) THEN 1 ELSE 0 END AS LINE_BENE_PTB_DDCTBL_AMT_13
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_1, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_1, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_1
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_2, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_2, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_2
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_3, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_3, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_3
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_4, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_4, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_4
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_5, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_5, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_5
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_6, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_6, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_6
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_7, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_7, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_7
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_8, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_8, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_8
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_9, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_9, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_9
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_10, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_10, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_10
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_11, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_11, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_11
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_12, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_12, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_12
                , CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_13, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_13, 0) THEN 1 ELSE 0 END AS LINE_BENE_PRMRY_PYR_PD_AMT_13
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_1, 0) <> COALESCE(n.LINE_COINSRNC_AMT_1, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_1
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_2, 0) <> COALESCE(n.LINE_COINSRNC_AMT_2, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_2
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_3, 0) <> COALESCE(n.LINE_COINSRNC_AMT_3, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_3
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_4, 0) <> COALESCE(n.LINE_COINSRNC_AMT_4, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_4
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_5, 0) <> COALESCE(n.LINE_COINSRNC_AMT_5, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_5
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_6, 0) <> COALESCE(n.LINE_COINSRNC_AMT_6, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_6
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_7, 0) <> COALESCE(n.LINE_COINSRNC_AMT_7, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_7
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_8, 0) <> COALESCE(n.LINE_COINSRNC_AMT_8, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_8
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_9, 0) <> COALESCE(n.LINE_COINSRNC_AMT_9, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_9
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_10, 0) <> COALESCE(n.LINE_COINSRNC_AMT_10, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_10
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_11, 0) <> COALESCE(n.LINE_COINSRNC_AMT_11, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_11
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_12, 0) <> COALESCE(n.LINE_COINSRNC_AMT_12, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_12
                , CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_13, 0) <> COALESCE(n.LINE_COINSRNC_AMT_13, 0) THEN 1 ELSE 0 END AS LINE_COINSRNC_AMT_13
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_1, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_1, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_1
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_2, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_2, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_2
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_3, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_3, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_3
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_4, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_4, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_4
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_5, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_5, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_5
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_6, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_6, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_6
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_7, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_7, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_7
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_8, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_8, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_8
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_9, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_9, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_9
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_10, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_10, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_10
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_11, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_11, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_11
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_12, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_12, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_12
                , CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_13, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_13, 0) THEN 1 ELSE 0 END AS LINE_ALOWD_CHRG_AMT_13
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_1, '') <> COALESCE(n.LINE_PRCSG_IND_CD_1, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_1
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_2, '') <> COALESCE(n.LINE_PRCSG_IND_CD_2, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_2
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_3, '') <> COALESCE(n.LINE_PRCSG_IND_CD_3, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_3
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_4, '') <> COALESCE(n.LINE_PRCSG_IND_CD_4, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_4
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_5, '') <> COALESCE(n.LINE_PRCSG_IND_CD_5, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_5
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_6, '') <> COALESCE(n.LINE_PRCSG_IND_CD_6, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_6
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_7, '') <> COALESCE(n.LINE_PRCSG_IND_CD_7, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_7
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_8, '') <> COALESCE(n.LINE_PRCSG_IND_CD_8, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_8
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_9, '') <> COALESCE(n.LINE_PRCSG_IND_CD_9, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_9
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_10, '') <> COALESCE(n.LINE_PRCSG_IND_CD_10, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_10
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_11, '') <> COALESCE(n.LINE_PRCSG_IND_CD_11, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_11
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_12, '') <> COALESCE(n.LINE_PRCSG_IND_CD_12, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_12
                , CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_13, '') <> COALESCE(n.LINE_PRCSG_IND_CD_13, '') THEN 1 ELSE 0 END AS LINE_PRCSG_IND_CD_13
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_1, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_1, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_1
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_2, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_2, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_2
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_3, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_3, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_3
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_4, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_4, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_4
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_5, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_5, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_5
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_6, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_6, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_6
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_7, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_7, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_7
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_8, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_8, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_8
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_9, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_9, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_9
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_10, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_10, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_10
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_11, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_11, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_11
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_12, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_12, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_12
                , CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_13, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_13, '') THEN 1 ELSE 0 END AS LINE_ICD9_DGNS_CD_13
            FROM
                keys k 
        LEFT JOIN src_ s ON k.DESYNPUF_ID = s.DESYNPUF_ID AND k.CLM_ID = s.CLM_ID AND k.CLM_FROM_DT = s.CLM_FROM_DT AND k.CLM_THRU_DT = s.CLM_THRU_DT
        LEFT JOIN new_ n ON k.DESYNPUF_ID = n.DESYNPUF_ID AND k.CLM_ID = n.CLM_ID AND k.CLM_FROM_DT = n.CLM_FROM_DT AND k.CLM_THRU_DT = n.CLM_THRU_DT;"""
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Phase 3 SQL transformations completed successfully")
    except Exception as e:
        logger.error(f"Error running Phase 3 SQL transformations: {e}")
        logger.info("Starting Phase 3a SQL transformations.")
        sql_script = """/*CLAIM DIFFERENCES*/

            DROP VIEW IF EXISTS vw_claim_mismatches;
            CREATE VIEW vw_claim_mismatches AS
            SELECT
                s.clm_id AS CLM_ID
                ,'Error: Claim Missing in New File' as FINDING
            FROM
                src_carrier_claims s
            LEFT JOIN new_carrier_claims n ON
                s.clm_id = n.clm_id
            WHERE
                n.clm_id IS NULL
            UNION 
            SELECT
                n.clm_id AS CLM_ID
                ,'Error: Claim Present in New File, Not in Original File' as FINDING
            FROM
                new_carrier_claims n
            LEFT JOIN src_carrier_claims s ON
                n.clm_id = s.clm_id
            WHERE
                s.clm_id IS NULL;

            -- This query calculates the "True" Payment for Line 1 using the business rules
            -- and compares it to the Source to see if the migration actually failed.
            DROP VIEW IF EXISTS vw_claim_line_nch_pmt_amt_1_differences;
            CREATE OR REPLACE VIEW vw_claim_line_nch_pmt_amt_1_differences AS
            WITH combined_claims AS (
                SELECT 
                    COALESCE(s.CLM_ID, n.CLM_ID) AS CLM_ID,
                    COALESCE(s.DESYNPUF_ID, n.DESYNPUF_ID) AS BENE_ID,
                    s.LINE_NCH_PMT_AMT_1 AS src_pmt,
                    n.LINE_NCH_PMT_AMT_1 AS new_pmt,
                    n.LINE_PRCSG_IND_CD_1 AS processing_ind,
                    n.LINE_ALOWD_CHRG_AMT_1 AS allowed_amt,
                    COALESCE(s.CLM_FROM_DT, n.CLM_FROM_DT) AS CLM_FROM_DT,
                    CASE 
                        WHEN s.CLM_ID IS NOT NULL AND n.CLM_ID IS NOT NULL THEN 'Matched'
                        WHEN s.CLM_ID IS NOT NULL AND n.CLM_ID IS NULL THEN 'Src Only'
                        WHEN s.CLM_ID IS NULL AND n.CLM_ID IS NOT NULL THEN 'New Only'
                    END AS record_status
                FROM src_carrier_claims s
                FULL OUTER JOIN new_carrier_claims n ON s.CLM_ID = n.CLM_ID
            )
            SELECT 
                c.CLM_ID,
                c.record_status,
                -- Translate Code to Name: Priority sb, then nb, then fallback to 'Unknown'
                COALESCE(ls_sb.name, ls_nb.name, 'Unknown/Orphan') AS STATE_NAME,
                c.CLM_FROM_DT,
                COALESCE(c.src_pmt, 0) AS src_pmt,
                COALESCE(c.new_pmt, 0) AS new_pmt,
                c.processing_ind,
                c.allowed_amt,
                CASE 
                    WHEN c.processing_ind = 'A' THEN 'Valid'
                    WHEN c.processing_ind IN ('R', 'S') AND c.allowed_amt > 0 THEN 'Valid'
                    ELSE 'Denied/Invalid'
                END AS business_rule_status,
                (COALESCE(c.src_pmt, 0) - COALESCE(c.new_pmt, 0)) AS variance
            FROM combined_claims c
            LEFT JOIN src_beneficiary_summary sb ON c.BENE_ID = sb.DESYNPUF_ID
            LEFT JOIN new_beneficiary_summary nb ON c.BENE_ID = nb.DESYNPUF_ID
            -- Joins to your Lookup Table
            LEFT JOIN lookup_state ls_sb ON sb.SP_STATE_CODE = ls_sb.code
            LEFT JOIN lookup_state ls_nb ON nb.SP_STATE_CODE = ls_nb.code
            WHERE business_rule_status = 'Valid' 
            AND COALESCE(c.src_pmt, 0) <> COALESCE(c.new_pmt, 0);

            CREATE OR REPLACE TABLE carrier_claims_orphans as 
            WITH KEYS AS (
                SELECT DISTINCT
                    DESYNPUF_ID
                    ,CLM_ID
                    ,CLM_FROM_DT
                    ,CLM_THRU_DT
                FROM 
                    data_eng.main.src_carrier_claims
            )
            SELECT 
                n.* 
            FROM 
                data_eng.main.new_carrier_claims n
            FULL OUTER JOIN 
                KEYS k 
            ON
                n.DESYNPUF_ID = k.DESYNPUF_ID
                AND n.CLM_ID = k.CLM_ID
                AND n.CLM_FROM_DT = k.CLM_FROM_DT
                AND n.CLM_THRU_DT = k.CLM_THRU_DT
            WHERE 
                k.DESYNPUF_ID IS NULL
                OR k.CLM_ID IS NULL
                OR k.CLM_FROM_DT IS NULL
                OR k.CLM_THRU_DT IS NULL;
            ANALYZE carrier_claims_orphans;"""

        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3a SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3a SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3b SQL transformations.")
        sql_script = """
            DROP TABLE IF EXISTS audit_carrier_keys;
            CREATE TABLE audit_carrier_keys AS
            WITH KEYS AS (
                SELECT DISTINCT
                    DESYNPUF_ID,
                    CLM_ID,
                    CLM_FROM_DT,
                    CLM_THRU_DT
                FROM data_eng.main.src_carrier_claims
                UNION
                SELECT DISTINCT
                    DESYNPUF_ID,
                    CLM_ID,
                    CLM_FROM_DT,
                    CLM_THRU_DT
                FROM data_eng.main.new_carrier_claims
            )
            SELECT * FROM KEYS;
            CREATE INDEX idx_audit_carrier_keys
                ON audit_carrier_keys (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT);
            ANALYZE audit_carrier_keys;

            DROP TABLE IF EXISTS audit_carrier_src;
            CREATE TABLE audit_carrier_src AS
            SELECT
                DESYNPUF_ID,
                CLM_ID,
                CLM_FROM_DT,
                CLM_THRU_DT,
                ICD9_DGNS_CD_1,
                ICD9_DGNS_CD_2,
                ICD9_DGNS_CD_3,
                ICD9_DGNS_CD_4,
                ICD9_DGNS_CD_5,
                ICD9_DGNS_CD_6,
                ICD9_DGNS_CD_7,
                ICD9_DGNS_CD_8,
                PRF_PHYSN_NPI_1,
                PRF_PHYSN_NPI_2,
                PRF_PHYSN_NPI_3,
                PRF_PHYSN_NPI_4,
                PRF_PHYSN_NPI_5,
                PRF_PHYSN_NPI_6,
                PRF_PHYSN_NPI_7,
                PRF_PHYSN_NPI_8,
                PRF_PHYSN_NPI_9,
                PRF_PHYSN_NPI_10,
                PRF_PHYSN_NPI_11,
                PRF_PHYSN_NPI_12,
                PRF_PHYSN_NPI_13,
                TAX_NUM_1,
                TAX_NUM_2,
                TAX_NUM_3,
                TAX_NUM_4,
                TAX_NUM_5,
                TAX_NUM_6,
                TAX_NUM_7,
                TAX_NUM_8,
                TAX_NUM_9,
                TAX_NUM_10,
                TAX_NUM_11,
                TAX_NUM_12,
                TAX_NUM_13,
                HCPCS_CD_1,
                HCPCS_CD_2,
                HCPCS_CD_3,
                HCPCS_CD_4,
                HCPCS_CD_5,
                HCPCS_CD_6,
                HCPCS_CD_7,
                HCPCS_CD_8,
                HCPCS_CD_9,
                HCPCS_CD_10,
                HCPCS_CD_11,
                HCPCS_CD_12,
                HCPCS_CD_13,
                LINE_NCH_PMT_AMT_1,
                LINE_NCH_PMT_AMT_2,
                LINE_NCH_PMT_AMT_3,
                LINE_NCH_PMT_AMT_4,
                LINE_NCH_PMT_AMT_5,
                LINE_NCH_PMT_AMT_6,
                LINE_NCH_PMT_AMT_7,
                LINE_NCH_PMT_AMT_8,
                LINE_NCH_PMT_AMT_9,
                LINE_NCH_PMT_AMT_10,
                LINE_NCH_PMT_AMT_11,
                LINE_NCH_PMT_AMT_12,
                LINE_NCH_PMT_AMT_13,
                LINE_BENE_PTB_DDCTBL_AMT_1,
                LINE_BENE_PTB_DDCTBL_AMT_2,
                LINE_BENE_PTB_DDCTBL_AMT_3,
                LINE_BENE_PTB_DDCTBL_AMT_4,
                LINE_BENE_PTB_DDCTBL_AMT_5,
                LINE_BENE_PTB_DDCTBL_AMT_6,
                LINE_BENE_PTB_DDCTBL_AMT_7,
                LINE_BENE_PTB_DDCTBL_AMT_8,
                LINE_BENE_PTB_DDCTBL_AMT_9,
                LINE_BENE_PTB_DDCTBL_AMT_10,
                LINE_BENE_PTB_DDCTBL_AMT_11,
                LINE_BENE_PTB_DDCTBL_AMT_12,
                LINE_BENE_PTB_DDCTBL_AMT_13,
                LINE_BENE_PRMRY_PYR_PD_AMT_1,
                LINE_BENE_PRMRY_PYR_PD_AMT_2,
                LINE_BENE_PRMRY_PYR_PD_AMT_3,
                LINE_BENE_PRMRY_PYR_PD_AMT_4,
                LINE_BENE_PRMRY_PYR_PD_AMT_5,
                LINE_BENE_PRMRY_PYR_PD_AMT_6,
                LINE_BENE_PRMRY_PYR_PD_AMT_7,
                LINE_BENE_PRMRY_PYR_PD_AMT_8,
                LINE_BENE_PRMRY_PYR_PD_AMT_9,
                LINE_BENE_PRMRY_PYR_PD_AMT_10,
                LINE_BENE_PRMRY_PYR_PD_AMT_11,
                LINE_BENE_PRMRY_PYR_PD_AMT_12,
                LINE_BENE_PRMRY_PYR_PD_AMT_13,
                LINE_COINSRNC_AMT_1,
                LINE_COINSRNC_AMT_2,
                LINE_COINSRNC_AMT_3,
                LINE_COINSRNC_AMT_4,
                LINE_COINSRNC_AMT_5,
                LINE_COINSRNC_AMT_6,
                LINE_COINSRNC_AMT_7,
                LINE_COINSRNC_AMT_8,
                LINE_COINSRNC_AMT_9,
                LINE_COINSRNC_AMT_10,
                LINE_COINSRNC_AMT_11,
                LINE_COINSRNC_AMT_12,
                LINE_COINSRNC_AMT_13,
                LINE_ALOWD_CHRG_AMT_1,
                LINE_ALOWD_CHRG_AMT_2,
                LINE_ALOWD_CHRG_AMT_3,
                LINE_ALOWD_CHRG_AMT_4,
                LINE_ALOWD_CHRG_AMT_5,
                LINE_ALOWD_CHRG_AMT_6,
                LINE_ALOWD_CHRG_AMT_7,
                LINE_ALOWD_CHRG_AMT_8,
                LINE_ALOWD_CHRG_AMT_9,
                LINE_ALOWD_CHRG_AMT_10,
                LINE_ALOWD_CHRG_AMT_11,
                LINE_ALOWD_CHRG_AMT_12,
                LINE_ALOWD_CHRG_AMT_13,
                LINE_PRCSG_IND_CD_1,
                LINE_PRCSG_IND_CD_2,
                LINE_PRCSG_IND_CD_3,
                LINE_PRCSG_IND_CD_4,
                LINE_PRCSG_IND_CD_5,
                LINE_PRCSG_IND_CD_6,
                LINE_PRCSG_IND_CD_7,
                LINE_PRCSG_IND_CD_8,
                LINE_PRCSG_IND_CD_9,
                LINE_PRCSG_IND_CD_10,
                LINE_PRCSG_IND_CD_11,
                LINE_PRCSG_IND_CD_12,
                LINE_PRCSG_IND_CD_13,
                LINE_ICD9_DGNS_CD_1,
                LINE_ICD9_DGNS_CD_2,
                LINE_ICD9_DGNS_CD_3,
                LINE_ICD9_DGNS_CD_4,
                LINE_ICD9_DGNS_CD_5,
                LINE_ICD9_DGNS_CD_6,
                LINE_ICD9_DGNS_CD_7,
                LINE_ICD9_DGNS_CD_8,
                LINE_ICD9_DGNS_CD_9,
                LINE_ICD9_DGNS_CD_10,
                LINE_ICD9_DGNS_CD_11,
                LINE_ICD9_DGNS_CD_12,
                LINE_ICD9_DGNS_CD_13
            FROM data_eng.main.src_carrier_claims;
            CREATE INDEX idx_audit_carrier_src_keys
                ON audit_carrier_src (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT);
            ANALYZE audit_carrier_src;

            DROP TABLE IF EXISTS audit_carrier_new;
            CREATE TABLE audit_carrier_new AS
            SELECT
                DESYNPUF_ID,
                CLM_ID,
                CLM_FROM_DT,
                CLM_THRU_DT,
                -- same column list as audit_carrier_src
                ICD9_DGNS_CD_1,
                ICD9_DGNS_CD_2,
                ICD9_DGNS_CD_3,
                ICD9_DGNS_CD_4,
                ICD9_DGNS_CD_5,
                ICD9_DGNS_CD_6,
                ICD9_DGNS_CD_7,
                ICD9_DGNS_CD_8,
                PRF_PHYSN_NPI_1,
                PRF_PHYSN_NPI_2,
                PRF_PHYSN_NPI_3,
                PRF_PHYSN_NPI_4,
                PRF_PHYSN_NPI_5,
                PRF_PHYSN_NPI_6,
                PRF_PHYSN_NPI_7,
                PRF_PHYSN_NPI_8,
                PRF_PHYSN_NPI_9,
                PRF_PHYSN_NPI_10,
                PRF_PHYSN_NPI_11,
                PRF_PHYSN_NPI_12,
                PRF_PHYSN_NPI_13,
                TAX_NUM_1,
                TAX_NUM_2,
                TAX_NUM_3,
                TAX_NUM_4,
                TAX_NUM_5,
                TAX_NUM_6,
                TAX_NUM_7,
                TAX_NUM_8,
                TAX_NUM_9,
                TAX_NUM_10,
                TAX_NUM_11,
                TAX_NUM_12,
                TAX_NUM_13,
                HCPCS_CD_1,
                HCPCS_CD_2,
                HCPCS_CD_3,
                HCPCS_CD_4,
                HCPCS_CD_5,
                HCPCS_CD_6,
                HCPCS_CD_7,
                HCPCS_CD_8,
                HCPCS_CD_9,
                HCPCS_CD_10,
                HCPCS_CD_11,
                HCPCS_CD_12,
                HCPCS_CD_13,
                LINE_NCH_PMT_AMT_1,
                LINE_NCH_PMT_AMT_2,
                LINE_NCH_PMT_AMT_3,
                LINE_NCH_PMT_AMT_4,
                LINE_NCH_PMT_AMT_5,
                LINE_NCH_PMT_AMT_6,
                LINE_NCH_PMT_AMT_7,
                LINE_NCH_PMT_AMT_8,
                LINE_NCH_PMT_AMT_9,
                LINE_NCH_PMT_AMT_10,
                LINE_NCH_PMT_AMT_11,
                LINE_NCH_PMT_AMT_12,
                LINE_NCH_PMT_AMT_13,
                LINE_BENE_PTB_DDCTBL_AMT_1,
                LINE_BENE_PTB_DDCTBL_AMT_2,
                LINE_BENE_PTB_DDCTBL_AMT_3,
                LINE_BENE_PTB_DDCTBL_AMT_4,
                LINE_BENE_PTB_DDCTBL_AMT_5,
                LINE_BENE_PTB_DDCTBL_AMT_6,
                LINE_BENE_PTB_DDCTBL_AMT_7,
                LINE_BENE_PTB_DDCTBL_AMT_8,
                LINE_BENE_PTB_DDCTBL_AMT_9,
                LINE_BENE_PTB_DDCTBL_AMT_10,
                LINE_BENE_PTB_DDCTBL_AMT_11,
                LINE_BENE_PTB_DDCTBL_AMT_12,
                LINE_BENE_PTB_DDCTBL_AMT_13,
                LINE_BENE_PRMRY_PYR_PD_AMT_1,
                LINE_BENE_PRMRY_PYR_PD_AMT_2,
                LINE_BENE_PRMRY_PYR_PD_AMT_3,
                LINE_BENE_PRMRY_PYR_PD_AMT_4,
                LINE_BENE_PRMRY_PYR_PD_AMT_5,
                LINE_BENE_PRMRY_PYR_PD_AMT_6,
                LINE_BENE_PRMRY_PYR_PD_AMT_7,
                LINE_BENE_PRMRY_PYR_PD_AMT_8,
                LINE_BENE_PRMRY_PYR_PD_AMT_9,
                LINE_BENE_PRMRY_PYR_PD_AMT_10,
                LINE_BENE_PRMRY_PYR_PD_AMT_11,
                LINE_BENE_PRMRY_PYR_PD_AMT_12,
                LINE_BENE_PRMRY_PYR_PD_AMT_13,
                LINE_COINSRNC_AMT_1,
                LINE_COINSRNC_AMT_2,
                LINE_COINSRNC_AMT_3,
                LINE_COINSRNC_AMT_4,
                LINE_COINSRNC_AMT_5,
                LINE_COINSRNC_AMT_6,
                LINE_COINSRNC_AMT_7,
                LINE_COINSRNC_AMT_8,
                LINE_COINSRNC_AMT_9,
                LINE_COINSRNC_AMT_10,
                LINE_COINSRNC_AMT_11,
                LINE_COINSRNC_AMT_12,
                LINE_COINSRNC_AMT_13,
                LINE_ALOWD_CHRG_AMT_1,
                LINE_ALOWD_CHRG_AMT_2,
                LINE_ALOWD_CHRG_AMT_3,
                LINE_ALOWD_CHRG_AMT_4,
                LINE_ALOWD_CHRG_AMT_5,
                LINE_ALOWD_CHRG_AMT_6,
                LINE_ALOWD_CHRG_AMT_7,
                LINE_ALOWD_CHRG_AMT_8,
                LINE_ALOWD_CHRG_AMT_9,
                LINE_ALOWD_CHRG_AMT_10,
                LINE_ALOWD_CHRG_AMT_11,
                LINE_ALOWD_CHRG_AMT_12,
                LINE_ALOWD_CHRG_AMT_13,
                LINE_PRCSG_IND_CD_1,
                LINE_PRCSG_IND_CD_2,
                LINE_PRCSG_IND_CD_3,
                LINE_PRCSG_IND_CD_4,
                LINE_PRCSG_IND_CD_5,
                LINE_PRCSG_IND_CD_6,
                LINE_PRCSG_IND_CD_7,
                LINE_PRCSG_IND_CD_8,
                LINE_PRCSG_IND_CD_9,
                LINE_PRCSG_IND_CD_10,
                LINE_PRCSG_IND_CD_11,
                LINE_PRCSG_IND_CD_12,
                LINE_PRCSG_IND_CD_13,
                LINE_ICD9_DGNS_CD_1,
                LINE_ICD9_DGNS_CD_2,
                LINE_ICD9_DGNS_CD_3,
                LINE_ICD9_DGNS_CD_4,
                LINE_ICD9_DGNS_CD_5,
                LINE_ICD9_DGNS_CD_6,
                LINE_ICD9_DGNS_CD_7,
                LINE_ICD9_DGNS_CD_8,
                LINE_ICD9_DGNS_CD_9,
                LINE_ICD9_DGNS_CD_10,
                LINE_ICD9_DGNS_CD_11,
                LINE_ICD9_DGNS_CD_12,
                LINE_ICD9_DGNS_CD_13
            FROM data_eng.main.new_carrier_claims;
            CREATE INDEX idx_audit_carrier_new_keys
                ON audit_carrier_new (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT);
            ANALYZE audit_carrier_new;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3b SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3b SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c1 SQL transformations.")
        sql_script = """
            DROP TABLE IF EXISTS audit_carrier_claims;
            CREATE TABLE audit_carrier_claims AS
            SELECT
                DESYNPUF_ID,
                CLM_ID,
                CLM_FROM_DT,
                CLM_THRU_DT
            FROM audit_carrier_keys;
            """
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c1 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c1 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c2 SQL transformations.")
        sql_script = """
            -- Add columns
            ALTER TABLE audit_carrier_claims ADD COLUMN CLM_FROM_DT_DIFF INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN CLM_THRU_DT_DIFF INTEGER;

            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_1 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_2 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_3 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_4 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_5 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_6 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_7 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN ICD9_DGNS_CD_8 INTEGER;

            -- Populate
            UPDATE audit_carrier_claims t
            SET
                CLM_FROM_DT_DIFF = CASE WHEN COALESCE(s.CLM_FROM_DT, '') <> COALESCE(n.CLM_FROM_DT, '') THEN 1 ELSE 0 END,
                CLM_THRU_DT_DIFF = CASE WHEN COALESCE(s.CLM_THRU_DT, '') <> COALESCE(n.CLM_THRU_DT, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_1   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_1, '') <> COALESCE(n.ICD9_DGNS_CD_1, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_2   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_2, '') <> COALESCE(n.ICD9_DGNS_CD_2, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_3   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_3, '') <> COALESCE(n.ICD9_DGNS_CD_3, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_4   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_4, '') <> COALESCE(n.ICD9_DGNS_CD_4, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_5   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_5, '') <> COALESCE(n.ICD9_DGNS_CD_5, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_6   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_6, '') <> COALESCE(n.ICD9_DGNS_CD_6, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_7   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_7, '') <> COALESCE(n.ICD9_DGNS_CD_7, '') THEN 1 ELSE 0 END,
                ICD9_DGNS_CD_8   = CASE WHEN COALESCE(s.ICD9_DGNS_CD_8, '') <> COALESCE(n.ICD9_DGNS_CD_8, '') THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;
            """
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c2 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c2 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c3 SQL transformations.")
        sql_script = """
            -- Add columns
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN PRF_PHYSN_NPI_13 INTEGER;

            -- Populate
            UPDATE audit_carrier_claims t
            SET
                PRF_PHYSN_NPI_1  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_1,  '') <> COALESCE(n.PRF_PHYSN_NPI_1,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_2  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_2,  '') <> COALESCE(n.PRF_PHYSN_NPI_2,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_3  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_3,  '') <> COALESCE(n.PRF_PHYSN_NPI_3,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_4  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_4,  '') <> COALESCE(n.PRF_PHYSN_NPI_4,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_5  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_5,  '') <> COALESCE(n.PRF_PHYSN_NPI_5,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_6  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_6,  '') <> COALESCE(n.PRF_PHYSN_NPI_6,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_7  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_7,  '') <> COALESCE(n.PRF_PHYSN_NPI_7,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_8  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_8,  '') <> COALESCE(n.PRF_PHYSN_NPI_8,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_9  = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_9,  '') <> COALESCE(n.PRF_PHYSN_NPI_9,  '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_10 = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_10, '') <> COALESCE(n.PRF_PHYSN_NPI_10, '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_11 = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_11, '') <> COALESCE(n.PRF_PHYSN_NPI_11, '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_12 = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_12, '') <> COALESCE(n.PRF_PHYSN_NPI_12, '') THEN 1 ELSE 0 END,
                PRF_PHYSN_NPI_13 = CASE WHEN COALESCE(s.PRF_PHYSN_NPI_13, '') <> COALESCE(n.PRF_PHYSN_NPI_13, '') THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;
            """
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c3 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c3 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c4 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN TAX_NUM_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                TAX_NUM_1  = CASE WHEN COALESCE(s.TAX_NUM_1,  '') <> COALESCE(n.TAX_NUM_1,  '') THEN 1 ELSE 0 END,
                TAX_NUM_2  = CASE WHEN COALESCE(s.TAX_NUM_2,  '') <> COALESCE(n.TAX_NUM_2,  '') THEN 1 ELSE 0 END,
                TAX_NUM_3  = CASE WHEN COALESCE(s.TAX_NUM_3,  '') <> COALESCE(n.TAX_NUM_3,  '') THEN 1 ELSE 0 END,
                TAX_NUM_4  = CASE WHEN COALESCE(s.TAX_NUM_4,  '') <> COALESCE(n.TAX_NUM_4,  '') THEN 1 ELSE 0 END,
                TAX_NUM_5  = CASE WHEN COALESCE(s.TAX_NUM_5,  '') <> COALESCE(n.TAX_NUM_5,  '') THEN 1 ELSE 0 END,
                TAX_NUM_6  = CASE WHEN COALESCE(s.TAX_NUM_6,  '') <> COALESCE(n.TAX_NUM_6,  '') THEN 1 ELSE 0 END,
                TAX_NUM_7  = CASE WHEN COALESCE(s.TAX_NUM_7,  '') <> COALESCE(n.TAX_NUM_7,  '') THEN 1 ELSE 0 END,
                TAX_NUM_8  = CASE WHEN COALESCE(s.TAX_NUM_8,  '') <> COALESCE(n.TAX_NUM_8,  '') THEN 1 ELSE 0 END,
                TAX_NUM_9  = CASE WHEN COALESCE(s.TAX_NUM_9,  '') <> COALESCE(n.TAX_NUM_9,  '') THEN 1 ELSE 0 END,
                TAX_NUM_10 = CASE WHEN COALESCE(s.TAX_NUM_10, '') <> COALESCE(n.TAX_NUM_10, '') THEN 1 ELSE 0 END,
                TAX_NUM_11 = CASE WHEN COALESCE(s.TAX_NUM_11, '') <> COALESCE(n.TAX_NUM_11, '') THEN 1 ELSE 0 END,
                TAX_NUM_12 = CASE WHEN COALESCE(s.TAX_NUM_12, '') <> COALESCE(n.TAX_NUM_12, '') THEN 1 ELSE 0 END,
                TAX_NUM_13 = CASE WHEN COALESCE(s.TAX_NUM_13, '') <> COALESCE(n.TAX_NUM_13, '') THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;
            """
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c4 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c4 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c5 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN HCPCS_CD_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                HCPCS_CD_1  = CASE WHEN COALESCE(s.HCPCS_CD_1,  '') <> COALESCE(n.HCPCS_CD_1,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_2  = CASE WHEN COALESCE(s.HCPCS_CD_2,  '') <> COALESCE(n.HCPCS_CD_2,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_3  = CASE WHEN COALESCE(s.HCPCS_CD_3,  '') <> COALESCE(n.HCPCS_CD_3,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_4  = CASE WHEN COALESCE(s.HCPCS_CD_4,  '') <> COALESCE(n.HCPCS_CD_4,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_5  = CASE WHEN COALESCE(s.HCPCS_CD_5,  '') <> COALESCE(n.HCPCS_CD_5,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_6  = CASE WHEN COALESCE(s.HCPCS_CD_6,  '') <> COALESCE(n.HCPCS_CD_6,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_7  = CASE WHEN COALESCE(s.HCPCS_CD_7,  '') <> COALESCE(n.HCPCS_CD_7,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_8  = CASE WHEN COALESCE(s.HCPCS_CD_8,  '') <> COALESCE(n.HCPCS_CD_8,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_9  = CASE WHEN COALESCE(s.HCPCS_CD_9,  '') <> COALESCE(n.HCPCS_CD_9,  '') THEN 1 ELSE 0 END,
                HCPCS_CD_10 = CASE WHEN COALESCE(s.HCPCS_CD_10, '') <> COALESCE(n.HCPCS_CD_10, '') THEN 1 ELSE 0 END,
                HCPCS_CD_11 = CASE WHEN COALESCE(s.HCPCS_CD_11, '') <> COALESCE(n.HCPCS_CD_11, '') THEN 1 ELSE 0 END,
                HCPCS_CD_12 = CASE WHEN COALESCE(s.HCPCS_CD_12, '') <> COALESCE(n.HCPCS_CD_12, '') THEN 1 ELSE 0 END,
                HCPCS_CD_13 = CASE WHEN COALESCE(s.HCPCS_CD_13, '') <> COALESCE(n.HCPCS_CD_13, '') THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c5 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c5 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c6 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_NCH_PMT_AMT_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                LINE_NCH_PMT_AMT_1  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_1,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_1,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_2  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_2,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_2,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_3  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_3,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_3,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_4  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_4,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_4,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_5  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_5,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_5,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_6  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_6,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_6,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_7  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_7,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_7,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_8  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_8,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_8,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_9  = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_9,  0) <> COALESCE(n.LINE_NCH_PMT_AMT_9,  0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_10 = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_10, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_10, 0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_11 = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_11, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_11, 0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_12 = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_12, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_12, 0) THEN 1 ELSE 0 END,
                LINE_NCH_PMT_AMT_13 = CASE WHEN COALESCE(s.LINE_NCH_PMT_AMT_13, 0) <> COALESCE(n.LINE_NCH_PMT_AMT_13, 0) THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c6 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c6 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c7 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PTB_DDCTBL_AMT_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                LINE_BENE_PTB_DDCTBL_AMT_1  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_1,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_1,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_2  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_2,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_2,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_3  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_3,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_3,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_4  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_4,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_4,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_5  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_5,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_5,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_6  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_6,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_6,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_7  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_7,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_7,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_8  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_8,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_8,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_9  = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_9,  0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_9,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_10 = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_10, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_10, 0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_11 = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_11, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_11, 0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_12 = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_12, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_12, 0) THEN 1 ELSE 0 END,
                LINE_BENE_PTB_DDCTBL_AMT_13 = CASE WHEN COALESCE(s.LINE_BENE_PTB_DDCTBL_AMT_13, 0) <> COALESCE(n.LINE_BENE_PTB_DDCTBL_AMT_13, 0) THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c7 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c7 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c8 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_BENE_PRMRY_PYR_PD_AMT_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                LINE_BENE_PRMRY_PYR_PD_AMT_1  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_1,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_1,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_2  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_2,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_2,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_3  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_3,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_3,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_4  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_4,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_4,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_5  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_5,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_5,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_6  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_6,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_6,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_7  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_7,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_7,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_8  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_8,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_8,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_9  = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_9,  0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_9,  0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_10 = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_10, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_10, 0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_11 = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_11, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_11, 0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_12 = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_12, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_12, 0) THEN 1 ELSE 0 END,
                LINE_BENE_PRMRY_PYR_PD_AMT_13 = CASE WHEN COALESCE(s.LINE_BENE_PRMRY_PYR_PD_AMT_13, 0) <> COALESCE(n.LINE_BENE_PRMRY_PYR_PD_AMT_13, 0) THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c8 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c8 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c9 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_COINSRNC_AMT_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                LINE_COINSRNC_AMT_1  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_1,  0) <> COALESCE(n.LINE_COINSRNC_AMT_1,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_2  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_2,  0) <> COALESCE(n.LINE_COINSRNC_AMT_2,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_3  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_3,  0) <> COALESCE(n.LINE_COINSRNC_AMT_3,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_4  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_4,  0) <> COALESCE(n.LINE_COINSRNC_AMT_4,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_5  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_5,  0) <> COALESCE(n.LINE_COINSRNC_AMT_5,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_6  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_6,  0) <> COALESCE(n.LINE_COINSRNC_AMT_6,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_7  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_7,  0) <> COALESCE(n.LINE_COINSRNC_AMT_7,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_8  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_8,  0) <> COALESCE(n.LINE_COINSRNC_AMT_8,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_9  = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_9,  0) <> COALESCE(n.LINE_COINSRNC_AMT_9,  0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_10 = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_10, 0) <> COALESCE(n.LINE_COINSRNC_AMT_10, 0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_11 = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_11, 0) <> COALESCE(n.LINE_COINSRNC_AMT_11, 0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_12 = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_12, 0) <> COALESCE(n.LINE_COINSRNC_AMT_12, 0) THEN 1 ELSE 0 END,
                LINE_COINSRNC_AMT_13 = CASE WHEN COALESCE(s.LINE_COINSRNC_AMT_13, 0) <> COALESCE(n.LINE_COINSRNC_AMT_13, 0) THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c9 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c9 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c10 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ALOWD_CHRG_AMT_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                LINE_ALOWD_CHRG_AMT_1  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_1,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_1,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_2  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_2,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_2,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_3  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_3,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_3,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_4  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_4,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_4,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_5  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_5,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_5,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_6  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_6,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_6,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_7  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_7,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_7,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_8  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_8,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_8,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_9  = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_9,  0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_9,  0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_10 = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_10, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_10, 0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_11 = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_11, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_11, 0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_12 = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_12, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_12, 0) THEN 1 ELSE 0 END,
                LINE_ALOWD_CHRG_AMT_13 = CASE WHEN COALESCE(s.LINE_ALOWD_CHRG_AMT_13, 0) <> COALESCE(n.LINE_ALOWD_CHRG_AMT_13, 0) THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c10 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c10 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c11 SQL transformations.")
        sql_script = """
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_1  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_2  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_3  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_4  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_5  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_6  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_7  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_8  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_9  INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_10 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_11 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_12 INTEGER;
            ALTER TABLE audit_carrier_claims ADD COLUMN LINE_PRCSG_IND_CD_13 INTEGER;

            UPDATE audit_carrier_claims t
            SET
                LINE_PRCSG_IND_CD_1  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_1,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_1,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_2  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_2,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_2,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_3  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_3,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_3,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_4  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_4,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_4,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_5  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_5,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_5,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_6  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_6,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_6,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_7  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_7,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_7,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_8  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_8,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_8,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_9  = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_9,  '') <> COALESCE(n.LINE_PRCSG_IND_CD_9,  '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_10 = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_10, '') <> COALESCE(n.LINE_PRCSG_IND_CD_10, '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_11 = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_11, '') <> COALESCE(n.LINE_PRCSG_IND_CD_11, '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_12 = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_12, '') <> COALESCE(n.LINE_PRCSG_IND_CD_12, '') THEN 1 ELSE 0 END,
                LINE_PRCSG_IND_CD_13 = CASE WHEN COALESCE(s.LINE_PRCSG_IND_CD_13, '') <> COALESCE(n.LINE_PRCSG_IND_CD_13, '') THEN 1 ELSE 0 END
            FROM audit_carrier_src s
            LEFT JOIN audit_carrier_new n
            ON s.DESYNPUF_ID = n.DESYNPUF_ID
            AND s.CLM_ID      = n.CLM_ID
            AND s.CLM_FROM_DT = n.CLM_FROM_DT
            AND s.CLM_THRU_DT = n.CLM_THRU_DT
            WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
            AND t.CLM_ID      = s.CLM_ID
            AND t.CLM_FROM_DT = s.CLM_FROM_DT
            AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c11 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c11 SQL transformations: {e}")
            raise

        logger.info("Starting Phase 3c12 SQL transformations.")
        sql_script = """
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_1  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_2  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_3  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_4  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_5  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_6  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_7  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_8  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_9  INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_10 INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_11 INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_12 INTEGER;
        ALTER TABLE audit_carrier_claims ADD COLUMN LINE_ICD9_DGNS_CD_13 INTEGER;

        UPDATE audit_carrier_claims t
        SET
            LINE_ICD9_DGNS_CD_1  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_1,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_1,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_2  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_2,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_2,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_3  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_3,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_3,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_4  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_4,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_4,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_5  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_5,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_5,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_6  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_6,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_6,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_7  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_7,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_7,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_8  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_8,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_8,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_9  = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_9,  '') <> COALESCE(n.LINE_ICD9_DGNS_CD_9,  '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_10 = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_10, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_10, '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_11 = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_11, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_11, '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_12 = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_12, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_12, '') THEN 1 ELSE 0 END,
            LINE_ICD9_DGNS_CD_13 = CASE WHEN COALESCE(s.LINE_ICD9_DGNS_CD_13, '') <> COALESCE(n.LINE_ICD9_DGNS_CD_13, '') THEN 1 ELSE 0 END
        FROM audit_carrier_src s
        LEFT JOIN audit_carrier_new n
        ON s.DESYNPUF_ID = n.DESYNPUF_ID
        AND s.CLM_ID      = n.CLM_ID
        AND s.CLM_FROM_DT = n.CLM_FROM_DT
        AND s.CLM_THRU_DT = n.CLM_THRU_DT
        WHERE t.DESYNPUF_ID = s.DESYNPUF_ID
        AND t.CLM_ID      = s.CLM_ID
        AND t.CLM_FROM_DT = s.CLM_FROM_DT
        AND t.CLM_THRU_DT = s.CLM_THRU_DT;"""
        try:
            execute_sql_script(sql_script)
            logger.info("✅ Phase 3c12 SQL transformations completed successfully")
        except Exception as e:
            logger.error(f"Error running Phase 3c12 SQL transformations: {e}")
            raise

    logger.info("Starting Phase 3 Index/Analyze.")
    sql_script = """
        CREATE INDEX idx_audit_carrier_claims_keys 
            ON audit_carrier_claims (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT);
        ANALYZE audit_carrier_claims;
        """
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Phase 3 Index/Analyzes completed successfully")
    except Exception as e:
        logger.error(f"Error running Phase 3 Index/Analyze: {e}")
        raise

    logger.info("Starting Phase 4 SQL transformations.")
    sql_script = """
        DROP TABLE IF EXISTS audit_carrier_financials;
        CREATE TABLE audit_carrier_financials AS
        WITH KEYS AS (
            SELECT DISTINCT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
            FROM
                data_eng.main.src_carrier_claims
            UNION 
            SELECT DISTINCT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
            FROM
                data_eng.main.new_carrier_claims
        ),
        src_ AS (
            SELECT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
                -- Financial Fields
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_1)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_1,  0) END AS LINE_NCH_PMT_AMT_1
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_2)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_2,  0) END AS LINE_NCH_PMT_AMT_2
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_3)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_3,  0) END AS LINE_NCH_PMT_AMT_3
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_4)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_4,  0) END AS LINE_NCH_PMT_AMT_4
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_5)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_5,  0) END AS LINE_NCH_PMT_AMT_5
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_6)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_6,  0) END AS LINE_NCH_PMT_AMT_6
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_7)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_7,  0) END AS LINE_NCH_PMT_AMT_7
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_8)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_8,  0) END AS LINE_NCH_PMT_AMT_8
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_9)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_9,  0) END AS LINE_NCH_PMT_AMT_9
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_10) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_10, 0) END AS LINE_NCH_PMT_AMT_10
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_11) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_11, 0) END AS LINE_NCH_PMT_AMT_11
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_12) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_12, 0) END AS LINE_NCH_PMT_AMT_12
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_13) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_13, 0) END AS LINE_NCH_PMT_AMT_13

                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_1)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_1,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_1
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_2)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_2,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_2
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_3)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_3,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_3
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_4)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_4,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_4
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_5)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_5,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_5
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_6)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_6,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_6
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_7)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_7,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_7
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_8)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_8,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_8
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_9)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_9,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_9
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_10) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_10, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_10
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_11) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_11, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_11
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_12) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_12, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_12
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_13) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_13, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_13

                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_1)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_1,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_1
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_2)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_2,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_2
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_3)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_3,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_3
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_4)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_4,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_4
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_5)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_5,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_5
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_6)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_6,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_6
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_7)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_7,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_7
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_8)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_8,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_8
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_9)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_9,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_9
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_10) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_10, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_10
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_11) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_11, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_11
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_12) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_12, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_12
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_13) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_13, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_13

                , CASE WHEN isnan(LINE_COINSRNC_AMT_1)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_1,  0) END AS LINE_COINSRNC_AMT_1
                , CASE WHEN isnan(LINE_COINSRNC_AMT_2)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_2,  0) END AS LINE_COINSRNC_AMT_2
                , CASE WHEN isnan(LINE_COINSRNC_AMT_3)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_3,  0) END AS LINE_COINSRNC_AMT_3
                , CASE WHEN isnan(LINE_COINSRNC_AMT_4)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_4,  0) END AS LINE_COINSRNC_AMT_4
                , CASE WHEN isnan(LINE_COINSRNC_AMT_5)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_5,  0) END AS LINE_COINSRNC_AMT_5
                , CASE WHEN isnan(LINE_COINSRNC_AMT_6)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_6,  0) END AS LINE_COINSRNC_AMT_6
                , CASE WHEN isnan(LINE_COINSRNC_AMT_7)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_7,  0) END AS LINE_COINSRNC_AMT_7
                , CASE WHEN isnan(LINE_COINSRNC_AMT_8)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_8,  0) END AS LINE_COINSRNC_AMT_8
                , CASE WHEN isnan(LINE_COINSRNC_AMT_9)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_9,  0) END AS LINE_COINSRNC_AMT_9
                , CASE WHEN isnan(LINE_COINSRNC_AMT_10) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_10, 0) END AS LINE_COINSRNC_AMT_10
                , CASE WHEN isnan(LINE_COINSRNC_AMT_11) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_11, 0) END AS LINE_COINSRNC_AMT_11
                , CASE WHEN isnan(LINE_COINSRNC_AMT_12) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_12, 0) END AS LINE_COINSRNC_AMT_12
                , CASE WHEN isnan(LINE_COINSRNC_AMT_13) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_13, 0) END AS LINE_COINSRNC_AMT_13

                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_1)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_1,  0) END AS LINE_ALOWD_CHRG_AMT_1
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_2)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_2,  0) END AS LINE_ALOWD_CHRG_AMT_2
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_3)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_3,  0) END AS LINE_ALOWD_CHRG_AMT_3
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_4)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_4,  0) END AS LINE_ALOWD_CHRG_AMT_4
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_5)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_5,  0) END AS LINE_ALOWD_CHRG_AMT_5
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_6)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_6,  0) END AS LINE_ALOWD_CHRG_AMT_6
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_7)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_7,  0) END AS LINE_ALOWD_CHRG_AMT_7
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_8)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_8,  0) END AS LINE_ALOWD_CHRG_AMT_8
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_9)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_9,  0) END AS LINE_ALOWD_CHRG_AMT_9
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_10) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_10, 0) END AS LINE_ALOWD_CHRG_AMT_10
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_11) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_11, 0) END AS LINE_ALOWD_CHRG_AMT_11
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_12) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_12, 0) END AS LINE_ALOWD_CHRG_AMT_12
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_13) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_13, 0) END AS LINE_ALOWD_CHRG_AMT_13
            FROM data_eng.main.src_carrier_claims
        ),
        new_ AS (
            SELECT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
                -- Financial Fields
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_1)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_1,  0) END AS LINE_NCH_PMT_AMT_1
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_2)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_2,  0) END AS LINE_NCH_PMT_AMT_2
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_3)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_3,  0) END AS LINE_NCH_PMT_AMT_3
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_4)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_4,  0) END AS LINE_NCH_PMT_AMT_4
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_5)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_5,  0) END AS LINE_NCH_PMT_AMT_5
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_6)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_6,  0) END AS LINE_NCH_PMT_AMT_6
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_7)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_7,  0) END AS LINE_NCH_PMT_AMT_7
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_8)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_8,  0) END AS LINE_NCH_PMT_AMT_8
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_9)  THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_9,  0) END AS LINE_NCH_PMT_AMT_9
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_10) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_10, 0) END AS LINE_NCH_PMT_AMT_10
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_11) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_11, 0) END AS LINE_NCH_PMT_AMT_11
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_12) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_12, 0) END AS LINE_NCH_PMT_AMT_12
                , CASE WHEN isnan(LINE_NCH_PMT_AMT_13) THEN 0 ELSE coalesce(LINE_NCH_PMT_AMT_13, 0) END AS LINE_NCH_PMT_AMT_13

                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_1)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_1,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_1
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_2)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_2,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_2
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_3)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_3,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_3
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_4)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_4,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_4
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_5)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_5,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_5
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_6)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_6,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_6
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_7)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_7,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_7
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_8)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_8,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_8
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_9)  THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_9,  0) END AS LINE_BENE_PTB_DDCTBL_AMT_9
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_10) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_10, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_10
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_11) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_11, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_11
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_12) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_12, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_12
                , CASE WHEN isnan(LINE_BENE_PTB_DDCTBL_AMT_13) THEN 0 ELSE coalesce(LINE_BENE_PTB_DDCTBL_AMT_13, 0) END AS LINE_BENE_PTB_DDCTBL_AMT_13

                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_1)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_1,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_1
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_2)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_2,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_2
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_3)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_3,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_3
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_4)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_4,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_4
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_5)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_5,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_5
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_6)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_6,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_6
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_7)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_7,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_7
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_8)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_8,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_8
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_9)  THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_9,  0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_9
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_10) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_10, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_10
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_11) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_11, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_11
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_12) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_12, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_12
                , CASE WHEN isnan(LINE_BENE_PRMRY_PYR_PD_AMT_13) THEN 0 ELSE coalesce(LINE_BENE_PRMRY_PYR_PD_AMT_13, 0) END AS LINE_BENE_PRMRY_PYR_PD_AMT_13

                , CASE WHEN isnan(LINE_COINSRNC_AMT_1)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_1,  0) END AS LINE_COINSRNC_AMT_1
                , CASE WHEN isnan(LINE_COINSRNC_AMT_2)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_2,  0) END AS LINE_COINSRNC_AMT_2
                , CASE WHEN isnan(LINE_COINSRNC_AMT_3)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_3,  0) END AS LINE_COINSRNC_AMT_3
                , CASE WHEN isnan(LINE_COINSRNC_AMT_4)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_4,  0) END AS LINE_COINSRNC_AMT_4
                , CASE WHEN isnan(LINE_COINSRNC_AMT_5)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_5,  0) END AS LINE_COINSRNC_AMT_5
                , CASE WHEN isnan(LINE_COINSRNC_AMT_6)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_6,  0) END AS LINE_COINSRNC_AMT_6
                , CASE WHEN isnan(LINE_COINSRNC_AMT_7)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_7,  0) END AS LINE_COINSRNC_AMT_7
                , CASE WHEN isnan(LINE_COINSRNC_AMT_8)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_8,  0) END AS LINE_COINSRNC_AMT_8
                , CASE WHEN isnan(LINE_COINSRNC_AMT_9)  THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_9,  0) END AS LINE_COINSRNC_AMT_9
                , CASE WHEN isnan(LINE_COINSRNC_AMT_10) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_10, 0) END AS LINE_COINSRNC_AMT_10
                , CASE WHEN isnan(LINE_COINSRNC_AMT_11) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_11, 0) END AS LINE_COINSRNC_AMT_11
                , CASE WHEN isnan(LINE_COINSRNC_AMT_12) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_12, 0) END AS LINE_COINSRNC_AMT_12
                , CASE WHEN isnan(LINE_COINSRNC_AMT_13) THEN 0 ELSE coalesce(LINE_COINSRNC_AMT_13, 0) END AS LINE_COINSRNC_AMT_13

                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_1)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_1,  0) END AS LINE_ALOWD_CHRG_AMT_1
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_2)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_2,  0) END AS LINE_ALOWD_CHRG_AMT_2
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_3)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_3,  0) END AS LINE_ALOWD_CHRG_AMT_3
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_4)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_4,  0) END AS LINE_ALOWD_CHRG_AMT_4
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_5)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_5,  0) END AS LINE_ALOWD_CHRG_AMT_5
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_6)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_6,  0) END AS LINE_ALOWD_CHRG_AMT_6
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_7)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_7,  0) END AS LINE_ALOWD_CHRG_AMT_7
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_8)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_8,  0) END AS LINE_ALOWD_CHRG_AMT_8
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_9)  THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_9,  0) END AS LINE_ALOWD_CHRG_AMT_9
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_10) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_10, 0) END AS LINE_ALOWD_CHRG_AMT_10
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_11) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_11, 0) END AS LINE_ALOWD_CHRG_AMT_11
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_12) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_12, 0) END AS LINE_ALOWD_CHRG_AMT_12
                , CASE WHEN isnan(LINE_ALOWD_CHRG_AMT_13) THEN 0 ELSE coalesce(LINE_ALOWD_CHRG_AMT_13, 0) END AS LINE_ALOWD_CHRG_AMT_13
            FROM data_eng.main.new_carrier_claims
        )
        SELECT
                k.DESYNPUF_ID
                , k.CLM_ID
                , k.CLM_FROM_DT
                , k.CLM_THRU_DT
                -- LINE_NCH_PMT_AMT (Medicare Reimbursement)
                , s.LINE_NCH_PMT_AMT_1 as src_LINE_NCH_PMT_AMT_1
                , n.LINE_NCH_PMT_AMT_1 as new_LINE_NCH_PMT_AMT_1
                , n.LINE_NCH_PMT_AMT_1 - s.LINE_NCH_PMT_AMT_1 as delta_LINE_NCH_PMT_AMT_1
                , abs(n.LINE_NCH_PMT_AMT_1 - s.LINE_NCH_PMT_AMT_1) as abs_delta_LINE_NCH_PMT_AMT_1
                , s.LINE_NCH_PMT_AMT_2 as src_LINE_NCH_PMT_AMT_2
                , n.LINE_NCH_PMT_AMT_2 as new_LINE_NCH_PMT_AMT_2
                , n.LINE_NCH_PMT_AMT_2 - s.LINE_NCH_PMT_AMT_2 as delta_LINE_NCH_PMT_AMT_2
                , abs(n.LINE_NCH_PMT_AMT_2 - s.LINE_NCH_PMT_AMT_2) as abs_delta_LINE_NCH_PMT_AMT_2
                , s.LINE_NCH_PMT_AMT_3 as src_LINE_NCH_PMT_AMT_3
                , n.LINE_NCH_PMT_AMT_3 as new_LINE_NCH_PMT_AMT_3
                , n.LINE_NCH_PMT_AMT_3 - s.LINE_NCH_PMT_AMT_3 as delta_LINE_NCH_PMT_AMT_3
                , abs(n.LINE_NCH_PMT_AMT_3 - s.LINE_NCH_PMT_AMT_3) as abs_delta_LINE_NCH_PMT_AMT_3
                , s.LINE_NCH_PMT_AMT_4 as src_LINE_NCH_PMT_AMT_4
                , n.LINE_NCH_PMT_AMT_4 as new_LINE_NCH_PMT_AMT_4
                , n.LINE_NCH_PMT_AMT_4 - s.LINE_NCH_PMT_AMT_4 as delta_LINE_NCH_PMT_AMT_4
                , abs(n.LINE_NCH_PMT_AMT_4 - s.LINE_NCH_PMT_AMT_4) as abs_delta_LINE_NCH_PMT_AMT_4
                , s.LINE_NCH_PMT_AMT_5 as src_LINE_NCH_PMT_AMT_5
                , n.LINE_NCH_PMT_AMT_5 as new_LINE_NCH_PMT_AMT_5
                , n.LINE_NCH_PMT_AMT_5 - s.LINE_NCH_PMT_AMT_5 as delta_LINE_NCH_PMT_AMT_5
                , abs(n.LINE_NCH_PMT_AMT_5 - s.LINE_NCH_PMT_AMT_5) as abs_delta_LINE_NCH_PMT_AMT_5
                , s.LINE_NCH_PMT_AMT_6 as src_LINE_NCH_PMT_AMT_6
                , n.LINE_NCH_PMT_AMT_6 as new_LINE_NCH_PMT_AMT_6
                , n.LINE_NCH_PMT_AMT_6 - s.LINE_NCH_PMT_AMT_6 as delta_LINE_NCH_PMT_AMT_6
                , abs(n.LINE_NCH_PMT_AMT_6 - s.LINE_NCH_PMT_AMT_6) as abs_delta_LINE_NCH_PMT_AMT_6
                , s.LINE_NCH_PMT_AMT_7 as src_LINE_NCH_PMT_AMT_7
                , n.LINE_NCH_PMT_AMT_7 as new_LINE_NCH_PMT_AMT_7
                , n.LINE_NCH_PMT_AMT_7 - s.LINE_NCH_PMT_AMT_7 as delta_LINE_NCH_PMT_AMT_7
                , abs(n.LINE_NCH_PMT_AMT_7 - s.LINE_NCH_PMT_AMT_7) as abs_delta_LINE_NCH_PMT_AMT_7
                , s.LINE_NCH_PMT_AMT_8 as src_LINE_NCH_PMT_AMT_8
                , n.LINE_NCH_PMT_AMT_8 as new_LINE_NCH_PMT_AMT_8
                , n.LINE_NCH_PMT_AMT_8 - s.LINE_NCH_PMT_AMT_8 as delta_LINE_NCH_PMT_AMT_8
                , abs(n.LINE_NCH_PMT_AMT_8 - s.LINE_NCH_PMT_AMT_8) as abs_delta_LINE_NCH_PMT_AMT_8
                , s.LINE_NCH_PMT_AMT_9 as src_LINE_NCH_PMT_AMT_9
                , n.LINE_NCH_PMT_AMT_9 as new_LINE_NCH_PMT_AMT_9
                , n.LINE_NCH_PMT_AMT_9 - s.LINE_NCH_PMT_AMT_9 as delta_LINE_NCH_PMT_AMT_9
                , abs(n.LINE_NCH_PMT_AMT_9 - s.LINE_NCH_PMT_AMT_9) as abs_delta_LINE_NCH_PMT_AMT_9
                , s.LINE_NCH_PMT_AMT_10 as src_LINE_NCH_PMT_AMT_10
                , n.LINE_NCH_PMT_AMT_10 as new_LINE_NCH_PMT_AMT_10
                , n.LINE_NCH_PMT_AMT_10 - s.LINE_NCH_PMT_AMT_10 as delta_LINE_NCH_PMT_AMT_10
                , abs(n.LINE_NCH_PMT_AMT_10 - s.LINE_NCH_PMT_AMT_10) as abs_delta_LINE_NCH_PMT_AMT_10
                , s.LINE_NCH_PMT_AMT_11 as src_LINE_NCH_PMT_AMT_11
                , n.LINE_NCH_PMT_AMT_11 as new_LINE_NCH_PMT_AMT_11
                , n.LINE_NCH_PMT_AMT_11 - s.LINE_NCH_PMT_AMT_11 as delta_LINE_NCH_PMT_AMT_11
                , abs(n.LINE_NCH_PMT_AMT_11 - s.LINE_NCH_PMT_AMT_11) as abs_delta_LINE_NCH_PMT_AMT_11
                , s.LINE_NCH_PMT_AMT_12 as src_LINE_NCH_PMT_AMT_12
                , n.LINE_NCH_PMT_AMT_12 as new_LINE_NCH_PMT_AMT_12
                , n.LINE_NCH_PMT_AMT_12 - s.LINE_NCH_PMT_AMT_12 as delta_LINE_NCH_PMT_AMT_12
                , abs(n.LINE_NCH_PMT_AMT_12 - s.LINE_NCH_PMT_AMT_12) as abs_delta_LINE_NCH_PMT_AMT_12
                , s.LINE_NCH_PMT_AMT_13 as src_LINE_NCH_PMT_AMT_13
                , n.LINE_NCH_PMT_AMT_13 as new_LINE_NCH_PMT_AMT_13
                , n.LINE_NCH_PMT_AMT_13 - s.LINE_NCH_PMT_AMT_13 as delta_LINE_NCH_PMT_AMT_13
                , abs(n.LINE_NCH_PMT_AMT_13 - s.LINE_NCH_PMT_AMT_13) as abs_delta_LINE_NCH_PMT_AMT_13
                -- LINE_BENE_PTB_DDCTBL_AMT (Beneficiary Responsibility)
                , s.LINE_BENE_PTB_DDCTBL_AMT_1 as src_LINE_BENE_PTB_DDCTBL_AMT_1
                , n.LINE_BENE_PTB_DDCTBL_AMT_1 as new_LINE_BENE_PTB_DDCTBL_AMT_1
                , n.LINE_BENE_PTB_DDCTBL_AMT_1 - s.LINE_BENE_PTB_DDCTBL_AMT_1 as delta_LINE_BENE_PTB_DDCTBL_AMT_1
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_1 - s.LINE_BENE_PTB_DDCTBL_AMT_1) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_1
                , s.LINE_BENE_PTB_DDCTBL_AMT_2 as src_LINE_BENE_PTB_DDCTBL_AMT_2
                , n.LINE_BENE_PTB_DDCTBL_AMT_2 as new_LINE_BENE_PTB_DDCTBL_AMT_2
                , n.LINE_BENE_PTB_DDCTBL_AMT_2 - s.LINE_BENE_PTB_DDCTBL_AMT_2 as delta_LINE_BENE_PTB_DDCTBL_AMT_2
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_2 - s.LINE_BENE_PTB_DDCTBL_AMT_2) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_2
                , s.LINE_BENE_PTB_DDCTBL_AMT_3 as src_LINE_BENE_PTB_DDCTBL_AMT_3
                , n.LINE_BENE_PTB_DDCTBL_AMT_3 as new_LINE_BENE_PTB_DDCTBL_AMT_3
                , n.LINE_BENE_PTB_DDCTBL_AMT_3 - s.LINE_BENE_PTB_DDCTBL_AMT_3 as delta_LINE_BENE_PTB_DDCTBL_AMT_3
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_3 - s.LINE_BENE_PTB_DDCTBL_AMT_3) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_3
                , s.LINE_BENE_PTB_DDCTBL_AMT_4 as src_LINE_BENE_PTB_DDCTBL_AMT_4
                , n.LINE_BENE_PTB_DDCTBL_AMT_4 as new_LINE_BENE_PTB_DDCTBL_AMT_4
                , n.LINE_BENE_PTB_DDCTBL_AMT_4 - s.LINE_BENE_PTB_DDCTBL_AMT_4 as delta_LINE_BENE_PTB_DDCTBL_AMT_4
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_4 - s.LINE_BENE_PTB_DDCTBL_AMT_4) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_4
                , s.LINE_BENE_PTB_DDCTBL_AMT_5 as src_LINE_BENE_PTB_DDCTBL_AMT_5
                , n.LINE_BENE_PTB_DDCTBL_AMT_5 as new_LINE_BENE_PTB_DDCTBL_AMT_5
                , n.LINE_BENE_PTB_DDCTBL_AMT_5 - s.LINE_BENE_PTB_DDCTBL_AMT_5 as delta_LINE_BENE_PTB_DDCTBL_AMT_5
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_5 - s.LINE_BENE_PTB_DDCTBL_AMT_5) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_5
                , s.LINE_BENE_PTB_DDCTBL_AMT_6 as src_LINE_BENE_PTB_DDCTBL_AMT_6
                , n.LINE_BENE_PTB_DDCTBL_AMT_6 as new_LINE_BENE_PTB_DDCTBL_AMT_6
                , n.LINE_BENE_PTB_DDCTBL_AMT_6 - s.LINE_BENE_PTB_DDCTBL_AMT_6 as delta_LINE_BENE_PTB_DDCTBL_AMT_6
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_6 - s.LINE_BENE_PTB_DDCTBL_AMT_6) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_6
                , s.LINE_BENE_PTB_DDCTBL_AMT_7 as src_LINE_BENE_PTB_DDCTBL_AMT_7
                , n.LINE_BENE_PTB_DDCTBL_AMT_7 as new_LINE_BENE_PTB_DDCTBL_AMT_7
                , n.LINE_BENE_PTB_DDCTBL_AMT_7 - s.LINE_BENE_PTB_DDCTBL_AMT_7 as delta_LINE_BENE_PTB_DDCTBL_AMT_7
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_7 - s.LINE_BENE_PTB_DDCTBL_AMT_7) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_7
                , s.LINE_BENE_PTB_DDCTBL_AMT_8 as src_LINE_BENE_PTB_DDCTBL_AMT_8
                , n.LINE_BENE_PTB_DDCTBL_AMT_8 as new_LINE_BENE_PTB_DDCTBL_AMT_8
                , n.LINE_BENE_PTB_DDCTBL_AMT_8 - s.LINE_BENE_PTB_DDCTBL_AMT_8 as delta_LINE_BENE_PTB_DDCTBL_AMT_8
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_8 - s.LINE_BENE_PTB_DDCTBL_AMT_8) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_8
                , s.LINE_BENE_PTB_DDCTBL_AMT_9 as src_LINE_BENE_PTB_DDCTBL_AMT_9
                , n.LINE_BENE_PTB_DDCTBL_AMT_9 as new_LINE_BENE_PTB_DDCTBL_AMT_9
                , n.LINE_BENE_PTB_DDCTBL_AMT_9 - s.LINE_BENE_PTB_DDCTBL_AMT_9 as delta_LINE_BENE_PTB_DDCTBL_AMT_9
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_9 - s.LINE_BENE_PTB_DDCTBL_AMT_9) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_9
                , s.LINE_BENE_PTB_DDCTBL_AMT_10 as src_LINE_BENE_PTB_DDCTBL_AMT_10
                , n.LINE_BENE_PTB_DDCTBL_AMT_10 as new_LINE_BENE_PTB_DDCTBL_AMT_10
                , n.LINE_BENE_PTB_DDCTBL_AMT_10 - s.LINE_BENE_PTB_DDCTBL_AMT_10 as delta_LINE_BENE_PTB_DDCTBL_AMT_10
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_10 - s.LINE_BENE_PTB_DDCTBL_AMT_10) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_10
                , s.LINE_BENE_PTB_DDCTBL_AMT_11 as src_LINE_BENE_PTB_DDCTBL_AMT_11
                , n.LINE_BENE_PTB_DDCTBL_AMT_11 as new_LINE_BENE_PTB_DDCTBL_AMT_11
                , n.LINE_BENE_PTB_DDCTBL_AMT_11 - s.LINE_BENE_PTB_DDCTBL_AMT_11 as delta_LINE_BENE_PTB_DDCTBL_AMT_11
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_11 - s.LINE_BENE_PTB_DDCTBL_AMT_11) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_11
                , s.LINE_BENE_PTB_DDCTBL_AMT_12 as src_LINE_BENE_PTB_DDCTBL_AMT_12
                , n.LINE_BENE_PTB_DDCTBL_AMT_12 as new_LINE_BENE_PTB_DDCTBL_AMT_12
                , n.LINE_BENE_PTB_DDCTBL_AMT_12 - s.LINE_BENE_PTB_DDCTBL_AMT_12 as delta_LINE_BENE_PTB_DDCTBL_AMT_12
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_12 - s.LINE_BENE_PTB_DDCTBL_AMT_12) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_12
                , s.LINE_BENE_PTB_DDCTBL_AMT_13 as src_LINE_BENE_PTB_DDCTBL_AMT_13
                , n.LINE_BENE_PTB_DDCTBL_AMT_13 as new_LINE_BENE_PTB_DDCTBL_AMT_13
                , n.LINE_BENE_PTB_DDCTBL_AMT_13 - s.LINE_BENE_PTB_DDCTBL_AMT_13 as delta_LINE_BENE_PTB_DDCTBL_AMT_13
                , abs(n.LINE_BENE_PTB_DDCTBL_AMT_13 - s.LINE_BENE_PTB_DDCTBL_AMT_13) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_13
                -- LINE_BENE_PRMRY_PYR_PD_AMT (Primary Payer Reimbursement)
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_1 as src_LINE_BENE_PRMRY_PYR_PD_AMT_1
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_1 as new_LINE_BENE_PRMRY_PYR_PD_AMT_1
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_1 - s.LINE_BENE_PRMRY_PYR_PD_AMT_1 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_1
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_1 - s.LINE_BENE_PRMRY_PYR_PD_AMT_1) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_1
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_2 as src_LINE_BENE_PRMRY_PYR_PD_AMT_2
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_2 as new_LINE_BENE_PRMRY_PYR_PD_AMT_2
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_2 - s.LINE_BENE_PRMRY_PYR_PD_AMT_2 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_2
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_2 - s.LINE_BENE_PRMRY_PYR_PD_AMT_2) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_2
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_3 as src_LINE_BENE_PRMRY_PYR_PD_AMT_3
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_3 as new_LINE_BENE_PRMRY_PYR_PD_AMT_3
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_3 - s.LINE_BENE_PRMRY_PYR_PD_AMT_3 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_3
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_3 - s.LINE_BENE_PRMRY_PYR_PD_AMT_3) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_3
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_4 as src_LINE_BENE_PRMRY_PYR_PD_AMT_4
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_4 as new_LINE_BENE_PRMRY_PYR_PD_AMT_4
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_4 - s.LINE_BENE_PRMRY_PYR_PD_AMT_4 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_4
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_4 - s.LINE_BENE_PRMRY_PYR_PD_AMT_4) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_4
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_5 as src_LINE_BENE_PRMRY_PYR_PD_AMT_5
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_5 as new_LINE_BENE_PRMRY_PYR_PD_AMT_5
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_5 - s.LINE_BENE_PRMRY_PYR_PD_AMT_5 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_5
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_5 - s.LINE_BENE_PRMRY_PYR_PD_AMT_5) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_5
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_6 as src_LINE_BENE_PRMRY_PYR_PD_AMT_6
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_6 as new_LINE_BENE_PRMRY_PYR_PD_AMT_6
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_6 - s.LINE_BENE_PRMRY_PYR_PD_AMT_6 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_6
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_6 - s.LINE_BENE_PRMRY_PYR_PD_AMT_6) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_6
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_7 as src_LINE_BENE_PRMRY_PYR_PD_AMT_7
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_7 as new_LINE_BENE_PRMRY_PYR_PD_AMT_7
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_7 - s.LINE_BENE_PRMRY_PYR_PD_AMT_7 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_7
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_7 - s.LINE_BENE_PRMRY_PYR_PD_AMT_7) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_7
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_8 as src_LINE_BENE_PRMRY_PYR_PD_AMT_8
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_8 as new_LINE_BENE_PRMRY_PYR_PD_AMT_8
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_8 - s.LINE_BENE_PRMRY_PYR_PD_AMT_8 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_8
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_8 - s.LINE_BENE_PRMRY_PYR_PD_AMT_8) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_8
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_9 as src_LINE_BENE_PRMRY_PYR_PD_AMT_9
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_9 as new_LINE_BENE_PRMRY_PYR_PD_AMT_9
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_9 - s.LINE_BENE_PRMRY_PYR_PD_AMT_9 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_9
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_9 - s.LINE_BENE_PRMRY_PYR_PD_AMT_9) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_9
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_10 as src_LINE_BENE_PRMRY_PYR_PD_AMT_10
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_10 as new_LINE_BENE_PRMRY_PYR_PD_AMT_10
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_10 - s.LINE_BENE_PRMRY_PYR_PD_AMT_10 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_10
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_10 - s.LINE_BENE_PRMRY_PYR_PD_AMT_10) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_10
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_11 as src_LINE_BENE_PRMRY_PYR_PD_AMT_11
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_11 as new_LINE_BENE_PRMRY_PYR_PD_AMT_11
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_11 - s.LINE_BENE_PRMRY_PYR_PD_AMT_11 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_11
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_11 - s.LINE_BENE_PRMRY_PYR_PD_AMT_11) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_11
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_12 as src_LINE_BENE_PRMRY_PYR_PD_AMT_12
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_12 as new_LINE_BENE_PRMRY_PYR_PD_AMT_12
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_12 - s.LINE_BENE_PRMRY_PYR_PD_AMT_12 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_12
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_12 - s.LINE_BENE_PRMRY_PYR_PD_AMT_12) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_12
                , s.LINE_BENE_PRMRY_PYR_PD_AMT_13 as src_LINE_BENE_PRMRY_PYR_PD_AMT_13
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_13 as new_LINE_BENE_PRMRY_PYR_PD_AMT_13
                , n.LINE_BENE_PRMRY_PYR_PD_AMT_13 - s.LINE_BENE_PRMRY_PYR_PD_AMT_13 as delta_LINE_BENE_PRMRY_PYR_PD_AMT_13
                , abs(n.LINE_BENE_PRMRY_PYR_PD_AMT_13 - s.LINE_BENE_PRMRY_PYR_PD_AMT_13) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_13
                -- LINE_COINSRNC_AMT (Beneficiary Responsibility)
                , s.LINE_COINSRNC_AMT_1 as src_LINE_COINSRNC_AMT_1
                , n.LINE_COINSRNC_AMT_1 as new_LINE_COINSRNC_AMT_1
                , n.LINE_COINSRNC_AMT_1 - s.LINE_COINSRNC_AMT_1 as delta_LINE_COINSRNC_AMT_1
                , abs(n.LINE_COINSRNC_AMT_1 - s.LINE_COINSRNC_AMT_1) as abs_delta_LINE_COINSRNC_AMT_1
                , s.LINE_COINSRNC_AMT_2 as src_LINE_COINSRNC_AMT_2
                , n.LINE_COINSRNC_AMT_2 as new_LINE_COINSRNC_AMT_2
                , n.LINE_COINSRNC_AMT_2 - s.LINE_COINSRNC_AMT_2 as delta_LINE_COINSRNC_AMT_2
                , abs(n.LINE_COINSRNC_AMT_2 - s.LINE_COINSRNC_AMT_2) as abs_delta_LINE_COINSRNC_AMT_2
                , s.LINE_COINSRNC_AMT_3 as src_LINE_COINSRNC_AMT_3
                , n.LINE_COINSRNC_AMT_3 as new_LINE_COINSRNC_AMT_3
                , n.LINE_COINSRNC_AMT_3 - s.LINE_COINSRNC_AMT_3 as delta_LINE_COINSRNC_AMT_3
                , abs(n.LINE_COINSRNC_AMT_3 - s.LINE_COINSRNC_AMT_3) as abs_delta_LINE_COINSRNC_AMT_3
                , s.LINE_COINSRNC_AMT_4 as src_LINE_COINSRNC_AMT_4
                , n.LINE_COINSRNC_AMT_4 as new_LINE_COINSRNC_AMT_4
                , n.LINE_COINSRNC_AMT_4 - s.LINE_COINSRNC_AMT_4 as delta_LINE_COINSRNC_AMT_4
                , abs(n.LINE_COINSRNC_AMT_4 - s.LINE_COINSRNC_AMT_4) as abs_delta_LINE_COINSRNC_AMT_4
                , s.LINE_COINSRNC_AMT_5 as src_LINE_COINSRNC_AMT_5
                , n.LINE_COINSRNC_AMT_5 as new_LINE_COINSRNC_AMT_5
                , n.LINE_COINSRNC_AMT_5 - s.LINE_COINSRNC_AMT_5 as delta_LINE_COINSRNC_AMT_5
                , abs(n.LINE_COINSRNC_AMT_5 - s.LINE_COINSRNC_AMT_5) as abs_delta_LINE_COINSRNC_AMT_5
                , s.LINE_COINSRNC_AMT_6 as src_LINE_COINSRNC_AMT_6
                , n.LINE_COINSRNC_AMT_6 as new_LINE_COINSRNC_AMT_6
                , n.LINE_COINSRNC_AMT_6 - s.LINE_COINSRNC_AMT_6 as delta_LINE_COINSRNC_AMT_6
                , abs(n.LINE_COINSRNC_AMT_6 - s.LINE_COINSRNC_AMT_6) as abs_delta_LINE_COINSRNC_AMT_6
                , s.LINE_COINSRNC_AMT_7 as src_LINE_COINSRNC_AMT_7
                , n.LINE_COINSRNC_AMT_7 as new_LINE_COINSRNC_AMT_7
                , n.LINE_COINSRNC_AMT_7 - s.LINE_COINSRNC_AMT_7 as delta_LINE_COINSRNC_AMT_7
                , abs(n.LINE_COINSRNC_AMT_7 - s.LINE_COINSRNC_AMT_7) as abs_delta_LINE_COINSRNC_AMT_7
                , s.LINE_COINSRNC_AMT_8 as src_LINE_COINSRNC_AMT_8
                , n.LINE_COINSRNC_AMT_8 as new_LINE_COINSRNC_AMT_8
                , n.LINE_COINSRNC_AMT_8 - s.LINE_COINSRNC_AMT_8 as delta_LINE_COINSRNC_AMT_8
                , abs(n.LINE_COINSRNC_AMT_8 - s.LINE_COINSRNC_AMT_8) as abs_delta_LINE_COINSRNC_AMT_8
                , s.LINE_COINSRNC_AMT_9 as src_LINE_COINSRNC_AMT_9
                , n.LINE_COINSRNC_AMT_9 as new_LINE_COINSRNC_AMT_9
                , n.LINE_COINSRNC_AMT_9 - s.LINE_COINSRNC_AMT_9 as delta_LINE_COINSRNC_AMT_9
                , abs(n.LINE_COINSRNC_AMT_9 - s.LINE_COINSRNC_AMT_9) as abs_delta_LINE_COINSRNC_AMT_9
                , s.LINE_COINSRNC_AMT_10 as src_LINE_COINSRNC_AMT_10
                , n.LINE_COINSRNC_AMT_10 as new_LINE_COINSRNC_AMT_10
                , n.LINE_COINSRNC_AMT_10 - s.LINE_COINSRNC_AMT_10 as delta_LINE_COINSRNC_AMT_10
                , abs(n.LINE_COINSRNC_AMT_10 - s.LINE_COINSRNC_AMT_10) as abs_delta_LINE_COINSRNC_AMT_10
                , s.LINE_COINSRNC_AMT_11 as src_LINE_COINSRNC_AMT_11
                , n.LINE_COINSRNC_AMT_11 as new_LINE_COINSRNC_AMT_11
                , n.LINE_COINSRNC_AMT_11 - s.LINE_COINSRNC_AMT_11 as delta_LINE_COINSRNC_AMT_11
                , abs(n.LINE_COINSRNC_AMT_11 - s.LINE_COINSRNC_AMT_11) as abs_delta_LINE_COINSRNC_AMT_11
                , s.LINE_COINSRNC_AMT_12 as src_LINE_COINSRNC_AMT_12
                , n.LINE_COINSRNC_AMT_12 as new_LINE_COINSRNC_AMT_12
                , n.LINE_COINSRNC_AMT_12 - s.LINE_COINSRNC_AMT_12 as delta_LINE_COINSRNC_AMT_12
                , abs(n.LINE_COINSRNC_AMT_12 - s.LINE_COINSRNC_AMT_12) as abs_delta_LINE_COINSRNC_AMT_12
                , s.LINE_COINSRNC_AMT_13 as src_LINE_COINSRNC_AMT_13
                , n.LINE_COINSRNC_AMT_13 as new_LINE_COINSRNC_AMT_13
                , n.LINE_COINSRNC_AMT_13 - s.LINE_COINSRNC_AMT_13 as delta_LINE_COINSRNC_AMT_13
                , abs(n.LINE_COINSRNC_AMT_13 - s.LINE_COINSRNC_AMT_13) as abs_delta_LINE_COINSRNC_AMT_13
                -- LINE_ALOWD_CHRG_AMT (Filter Baseline)
                , s.LINE_ALOWD_CHRG_AMT_1 as src_LINE_ALOWD_CHRG_AMT_1
                , n.LINE_ALOWD_CHRG_AMT_1 as new_LINE_ALOWD_CHRG_AMT_1
                , n.LINE_ALOWD_CHRG_AMT_1 - s.LINE_ALOWD_CHRG_AMT_1 as delta_LINE_ALOWD_CHRG_AMT_1
                , abs(n.LINE_ALOWD_CHRG_AMT_1 - s.LINE_ALOWD_CHRG_AMT_1) as abs_delta_LINE_ALOWD_CHRG_AMT_1
                , s.LINE_ALOWD_CHRG_AMT_2 as src_LINE_ALOWD_CHRG_AMT_2
                , n.LINE_ALOWD_CHRG_AMT_2 as new_LINE_ALOWD_CHRG_AMT_2
                , n.LINE_ALOWD_CHRG_AMT_2 - s.LINE_ALOWD_CHRG_AMT_2 as delta_LINE_ALOWD_CHRG_AMT_2
                , abs(n.LINE_ALOWD_CHRG_AMT_2 - s.LINE_ALOWD_CHRG_AMT_2) as abs_delta_LINE_ALOWD_CHRG_AMT_2
                , s.LINE_ALOWD_CHRG_AMT_3 as src_LINE_ALOWD_CHRG_AMT_3
                , n.LINE_ALOWD_CHRG_AMT_3 as new_LINE_ALOWD_CHRG_AMT_3
                , n.LINE_ALOWD_CHRG_AMT_3 - s.LINE_ALOWD_CHRG_AMT_3 as delta_LINE_ALOWD_CHRG_AMT_3
                , abs(n.LINE_ALOWD_CHRG_AMT_3 - s.LINE_ALOWD_CHRG_AMT_3) as abs_delta_LINE_ALOWD_CHRG_AMT_3
                , s.LINE_ALOWD_CHRG_AMT_4 as src_LINE_ALOWD_CHRG_AMT_4
                , n.LINE_ALOWD_CHRG_AMT_4 as new_LINE_ALOWD_CHRG_AMT_4
                , n.LINE_ALOWD_CHRG_AMT_4 - s.LINE_ALOWD_CHRG_AMT_4 as delta_LINE_ALOWD_CHRG_AMT_4
                , abs(n.LINE_ALOWD_CHRG_AMT_4 - s.LINE_ALOWD_CHRG_AMT_4) as abs_delta_LINE_ALOWD_CHRG_AMT_4
                , s.LINE_ALOWD_CHRG_AMT_5 as src_LINE_ALOWD_CHRG_AMT_5
                , n.LINE_ALOWD_CHRG_AMT_5 as new_LINE_ALOWD_CHRG_AMT_5
                , n.LINE_ALOWD_CHRG_AMT_5 - s.LINE_ALOWD_CHRG_AMT_5 as delta_LINE_ALOWD_CHRG_AMT_5
                , abs(n.LINE_ALOWD_CHRG_AMT_5 - s.LINE_ALOWD_CHRG_AMT_5) as abs_delta_LINE_ALOWD_CHRG_AMT_5
                , s.LINE_ALOWD_CHRG_AMT_6 as src_LINE_ALOWD_CHRG_AMT_6
                , n.LINE_ALOWD_CHRG_AMT_6 as new_LINE_ALOWD_CHRG_AMT_6
                , n.LINE_ALOWD_CHRG_AMT_6 - s.LINE_ALOWD_CHRG_AMT_6 as delta_LINE_ALOWD_CHRG_AMT_6
                , abs(n.LINE_ALOWD_CHRG_AMT_6 - s.LINE_ALOWD_CHRG_AMT_6) as abs_delta_LINE_ALOWD_CHRG_AMT_6
                , s.LINE_ALOWD_CHRG_AMT_7 as src_LINE_ALOWD_CHRG_AMT_7
                , n.LINE_ALOWD_CHRG_AMT_7 as new_LINE_ALOWD_CHRG_AMT_7
                , n.LINE_ALOWD_CHRG_AMT_7 - s.LINE_ALOWD_CHRG_AMT_7 as delta_LINE_ALOWD_CHRG_AMT_7
                , abs(n.LINE_ALOWD_CHRG_AMT_7 - s.LINE_ALOWD_CHRG_AMT_7) as abs_delta_LINE_ALOWD_CHRG_AMT_7
                , s.LINE_ALOWD_CHRG_AMT_8 as src_LINE_ALOWD_CHRG_AMT_8
                , n.LINE_ALOWD_CHRG_AMT_8 as new_LINE_ALOWD_CHRG_AMT_8
                , n.LINE_ALOWD_CHRG_AMT_8 - s.LINE_ALOWD_CHRG_AMT_8 as delta_LINE_ALOWD_CHRG_AMT_8
                , abs(n.LINE_ALOWD_CHRG_AMT_8 - s.LINE_ALOWD_CHRG_AMT_8) as abs_delta_LINE_ALOWD_CHRG_AMT_8
                , s.LINE_ALOWD_CHRG_AMT_9 as src_LINE_ALOWD_CHRG_AMT_9
                , n.LINE_ALOWD_CHRG_AMT_9 as new_LINE_ALOWD_CHRG_AMT_9
                , n.LINE_ALOWD_CHRG_AMT_9 - s.LINE_ALOWD_CHRG_AMT_9 as delta_LINE_ALOWD_CHRG_AMT_9
                , abs(n.LINE_ALOWD_CHRG_AMT_9 - s.LINE_ALOWD_CHRG_AMT_9) as abs_delta_LINE_ALOWD_CHRG_AMT_9
                , s.LINE_ALOWD_CHRG_AMT_10 as src_LINE_ALOWD_CHRG_AMT_10
                , n.LINE_ALOWD_CHRG_AMT_10 as new_LINE_ALOWD_CHRG_AMT_10
                , n.LINE_ALOWD_CHRG_AMT_10 - s.LINE_ALOWD_CHRG_AMT_10 as delta_LINE_ALOWD_CHRG_AMT_10
                , abs(n.LINE_ALOWD_CHRG_AMT_10 - s.LINE_ALOWD_CHRG_AMT_10) as abs_delta_LINE_ALOWD_CHRG_AMT_10
                , s.LINE_ALOWD_CHRG_AMT_11 as src_LINE_ALOWD_CHRG_AMT_11
                , n.LINE_ALOWD_CHRG_AMT_11 as new_LINE_ALOWD_CHRG_AMT_11
                , n.LINE_ALOWD_CHRG_AMT_11 - s.LINE_ALOWD_CHRG_AMT_11 as delta_LINE_ALOWD_CHRG_AMT_11
                , abs(n.LINE_ALOWD_CHRG_AMT_11 - s.LINE_ALOWD_CHRG_AMT_11) as abs_delta_LINE_ALOWD_CHRG_AMT_11
                , s.LINE_ALOWD_CHRG_AMT_12 as src_LINE_ALOWD_CHRG_AMT_12
                , n.LINE_ALOWD_CHRG_AMT_12 as new_LINE_ALOWD_CHRG_AMT_12
                , n.LINE_ALOWD_CHRG_AMT_12 - s.LINE_ALOWD_CHRG_AMT_12 as delta_LINE_ALOWD_CHRG_AMT_12
                , abs(n.LINE_ALOWD_CHRG_AMT_12 - s.LINE_ALOWD_CHRG_AMT_12) as abs_delta_LINE_ALOWD_CHRG_AMT_12
                , s.LINE_ALOWD_CHRG_AMT_13 as src_LINE_ALOWD_CHRG_AMT_13
                , n.LINE_ALOWD_CHRG_AMT_13 as new_LINE_ALOWD_CHRG_AMT_13
                , n.LINE_ALOWD_CHRG_AMT_13 - s.LINE_ALOWD_CHRG_AMT_13 as delta_LINE_ALOWD_CHRG_AMT_13
                , abs(n.LINE_ALOWD_CHRG_AMT_13 - s.LINE_ALOWD_CHRG_AMT_13) as abs_delta_LINE_ALOWD_CHRG_AMT_13
            FROM
                keys k 
        LEFT JOIN src_ s ON k.DESYNPUF_ID = s.DESYNPUF_ID AND k.CLM_ID = s.CLM_ID AND k.CLM_FROM_DT = s.CLM_FROM_DT AND k.CLM_THRU_DT = s.CLM_THRU_DT
        LEFT JOIN new_ n ON k.DESYNPUF_ID = n.DESYNPUF_ID AND k.CLM_ID = n.CLM_ID AND k.CLM_FROM_DT = n.CLM_FROM_DT AND k.CLM_THRU_DT = n.CLM_THRU_DT;
        CREATE INDEX idx_audit_carrier_financials_keys ON audit_carrier_financials(DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT);
        ANALYZE audit_carrier_financials;

        CREATE OR REPLACE VIEW vw_financial_differences_claim AS
        WITH agg AS (
            SELECT
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
                , sum(abs_delta_LINE_NCH_PMT_AMT_1)  as abs_delta_LINE_NCH_PMT_AMT_1
                , sum(abs_delta_LINE_NCH_PMT_AMT_2)  as abs_delta_LINE_NCH_PMT_AMT_2
                , sum(abs_delta_LINE_NCH_PMT_AMT_3)  as abs_delta_LINE_NCH_PMT_AMT_3
                , sum(abs_delta_LINE_NCH_PMT_AMT_4)  as abs_delta_LINE_NCH_PMT_AMT_4
                , sum(abs_delta_LINE_NCH_PMT_AMT_5)  as abs_delta_LINE_NCH_PMT_AMT_5
                , sum(abs_delta_LINE_NCH_PMT_AMT_6)  as abs_delta_LINE_NCH_PMT_AMT_6
                , sum(abs_delta_LINE_NCH_PMT_AMT_7)  as abs_delta_LINE_NCH_PMT_AMT_7
                , sum(abs_delta_LINE_NCH_PMT_AMT_8)  as abs_delta_LINE_NCH_PMT_AMT_8
                , sum(abs_delta_LINE_NCH_PMT_AMT_9)  as abs_delta_LINE_NCH_PMT_AMT_9
                , sum(abs_delta_LINE_NCH_PMT_AMT_10) as abs_delta_LINE_NCH_PMT_AMT_10
                , sum(abs_delta_LINE_NCH_PMT_AMT_11) as abs_delta_LINE_NCH_PMT_AMT_11
                , sum(abs_delta_LINE_NCH_PMT_AMT_12) as abs_delta_LINE_NCH_PMT_AMT_12
                , sum(abs_delta_LINE_NCH_PMT_AMT_13) as abs_delta_LINE_NCH_PMT_AMT_13
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_1)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_1
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_2)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_2
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_3)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_3
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_4)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_4
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_5)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_5
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_6)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_6
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_7)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_7
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_8)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_8
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_9)  as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_9
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_10) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_10
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_11) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_11
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_12) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_12
                , sum(abs_delta_LINE_BENE_PTB_DDCTBL_AMT_13) as abs_delta_LINE_BENE_PTB_DDCTBL_AMT_13
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_1)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_1
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_2)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_2
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_3)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_3
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_4)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_4
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_5)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_5
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_6)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_6
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_7)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_7
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_8)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_8
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_9)  as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_9
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_10) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_10
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_11) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_11
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_12) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_12
                , sum(abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_13) as abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_13
                , sum(abs_delta_LINE_COINSRNC_AMT_1)  as abs_delta_LINE_COINSRNC_AMT_1
                , sum(abs_delta_LINE_COINSRNC_AMT_2)  as abs_delta_LINE_COINSRNC_AMT_2
                , sum(abs_delta_LINE_COINSRNC_AMT_3)  as abs_delta_LINE_COINSRNC_AMT_3
                , sum(abs_delta_LINE_COINSRNC_AMT_4)  as abs_delta_LINE_COINSRNC_AMT_4
                , sum(abs_delta_LINE_COINSRNC_AMT_5)  as abs_delta_LINE_COINSRNC_AMT_5
                , sum(abs_delta_LINE_COINSRNC_AMT_6)  as abs_delta_LINE_COINSRNC_AMT_6
                , sum(abs_delta_LINE_COINSRNC_AMT_7)  as abs_delta_LINE_COINSRNC_AMT_7
                , sum(abs_delta_LINE_COINSRNC_AMT_8)  as abs_delta_LINE_COINSRNC_AMT_8
                , sum(abs_delta_LINE_COINSRNC_AMT_9)  as abs_delta_LINE_COINSRNC_AMT_9
                , sum(abs_delta_LINE_COINSRNC_AMT_10) as abs_delta_LINE_COINSRNC_AMT_10
                , sum(abs_delta_LINE_COINSRNC_AMT_11) as abs_delta_LINE_COINSRNC_AMT_11
                , sum(abs_delta_LINE_COINSRNC_AMT_12) as abs_delta_LINE_COINSRNC_AMT_12
                , sum(abs_delta_LINE_COINSRNC_AMT_13) as abs_delta_LINE_COINSRNC_AMT_13
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_1)  as abs_delta_LINE_ALOWD_CHRG_AMT_1
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_2)  as abs_delta_LINE_ALOWD_CHRG_AMT_2
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_3)  as abs_delta_LINE_ALOWD_CHRG_AMT_3
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_4)  as abs_delta_LINE_ALOWD_CHRG_AMT_4
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_5)  as abs_delta_LINE_ALOWD_CHRG_AMT_5
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_6)  as abs_delta_LINE_ALOWD_CHRG_AMT_6
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_7)  as abs_delta_LINE_ALOWD_CHRG_AMT_7
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_8)  as abs_delta_LINE_ALOWD_CHRG_AMT_8
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_9)  as abs_delta_LINE_ALOWD_CHRG_AMT_9
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_10) as abs_delta_LINE_ALOWD_CHRG_AMT_10
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_11) as abs_delta_LINE_ALOWD_CHRG_AMT_11
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_12) as abs_delta_LINE_ALOWD_CHRG_AMT_12
                , sum(abs_delta_LINE_ALOWD_CHRG_AMT_13) as abs_delta_LINE_ALOWD_CHRG_AMT_13
            FROM data_eng.main.audit_carrier_financials
            GROUP BY 
                DESYNPUF_ID
                , CLM_ID
                , CLM_FROM_DT
                , CLM_THRU_DT
        )
        SELECT *
        FROM agg
        WHERE list_sum(list_value(
                abs_delta_LINE_NCH_PMT_AMT_1,
                abs_delta_LINE_NCH_PMT_AMT_2,
                abs_delta_LINE_NCH_PMT_AMT_3,
                abs_delta_LINE_NCH_PMT_AMT_4,
                abs_delta_LINE_NCH_PMT_AMT_5,
                abs_delta_LINE_NCH_PMT_AMT_6,
                abs_delta_LINE_NCH_PMT_AMT_7,
                abs_delta_LINE_NCH_PMT_AMT_8,
                abs_delta_LINE_NCH_PMT_AMT_9,
                abs_delta_LINE_NCH_PMT_AMT_10,
                abs_delta_LINE_NCH_PMT_AMT_11,
                abs_delta_LINE_NCH_PMT_AMT_12,
                abs_delta_LINE_NCH_PMT_AMT_13,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_1,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_2,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_3,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_4,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_5,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_6,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_7,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_8,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_9,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_10,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_11,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_12,
                abs_delta_LINE_BENE_PTB_DDCTBL_AMT_13,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_1,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_2,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_3,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_4,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_5,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_6,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_7,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_8,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_9,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_10,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_11,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_12,
                abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_13,
                abs_delta_LINE_COINSRNC_AMT_1,
                abs_delta_LINE_COINSRNC_AMT_2,
                abs_delta_LINE_COINSRNC_AMT_3,
                abs_delta_LINE_COINSRNC_AMT_4,
                abs_delta_LINE_COINSRNC_AMT_5,
                abs_delta_LINE_COINSRNC_AMT_6,
                abs_delta_LINE_COINSRNC_AMT_7,
                abs_delta_LINE_COINSRNC_AMT_8,
                abs_delta_LINE_COINSRNC_AMT_9,
                abs_delta_LINE_COINSRNC_AMT_10,
                abs_delta_LINE_COINSRNC_AMT_11,
                abs_delta_LINE_COINSRNC_AMT_12,
                abs_delta_LINE_COINSRNC_AMT_13,
                abs_delta_LINE_ALOWD_CHRG_AMT_1,
                abs_delta_LINE_ALOWD_CHRG_AMT_2,
                abs_delta_LINE_ALOWD_CHRG_AMT_3,
                abs_delta_LINE_ALOWD_CHRG_AMT_4,
                abs_delta_LINE_ALOWD_CHRG_AMT_5,
                abs_delta_LINE_ALOWD_CHRG_AMT_6,
                abs_delta_LINE_ALOWD_CHRG_AMT_7,
                abs_delta_LINE_ALOWD_CHRG_AMT_8,
                abs_delta_LINE_ALOWD_CHRG_AMT_9,
                abs_delta_LINE_ALOWD_CHRG_AMT_10,
                abs_delta_LINE_ALOWD_CHRG_AMT_11,
                abs_delta_LINE_ALOWD_CHRG_AMT_12,
                abs_delta_LINE_ALOWD_CHRG_AMT_13
        )) > 0;

        CREATE OR REPLACE TABLE audit_claim_financial_fields AS
        WITH flat AS (
            SELECT
                [
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_1',  metric_value := abs_delta_LINE_NCH_PMT_AMT_1),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_2',  metric_value := abs_delta_LINE_NCH_PMT_AMT_2),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_3',  metric_value := abs_delta_LINE_NCH_PMT_AMT_3),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_4',  metric_value := abs_delta_LINE_NCH_PMT_AMT_4),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_5',  metric_value := abs_delta_LINE_NCH_PMT_AMT_5),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_6',  metric_value := abs_delta_LINE_NCH_PMT_AMT_6),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_7',  metric_value := abs_delta_LINE_NCH_PMT_AMT_7),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_8',  metric_value := abs_delta_LINE_NCH_PMT_AMT_8),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_9',  metric_value := abs_delta_LINE_NCH_PMT_AMT_9),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_10', metric_value := abs_delta_LINE_NCH_PMT_AMT_10),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_11', metric_value := abs_delta_LINE_NCH_PMT_AMT_11),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_12', metric_value := abs_delta_LINE_NCH_PMT_AMT_12),
                    struct_pack(metric_name := 'abs_delta_LINE_NCH_PMT_AMT_13', metric_value := abs_delta_LINE_NCH_PMT_AMT_13),

                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_1',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_1),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_2',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_2),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_3',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_3),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_4',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_4),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_5',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_5),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_6',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_6),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_7',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_7),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_8',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_8),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_9',  metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_9),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_10', metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_10),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_11', metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_11),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_12', metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_12),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PTB_DDCTBL_AMT_13', metric_value := abs_delta_LINE_BENE_PTB_DDCTBL_AMT_13),

                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_1',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_1),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_2',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_2),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_3',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_3),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_4',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_4),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_5',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_5),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_6',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_6),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_7',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_7),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_8',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_8),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_9',  metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_9),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_10', metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_10),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_11', metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_11),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_12', metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_12),
                    struct_pack(metric_name := 'abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_13', metric_value := abs_delta_LINE_BENE_PRMRY_PYR_PD_AMT_13),

                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_1',  metric_value := abs_delta_LINE_COINSRNC_AMT_1),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_2',  metric_value := abs_delta_LINE_COINSRNC_AMT_2),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_3',  metric_value := abs_delta_LINE_COINSRNC_AMT_3),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_4',  metric_value := abs_delta_LINE_COINSRNC_AMT_4),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_5',  metric_value := abs_delta_LINE_COINSRNC_AMT_5),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_6',  metric_value := abs_delta_LINE_COINSRNC_AMT_6),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_7',  metric_value := abs_delta_LINE_COINSRNC_AMT_7),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_8',  metric_value := abs_delta_LINE_COINSRNC_AMT_8),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_9',  metric_value := abs_delta_LINE_COINSRNC_AMT_9),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_10', metric_value := abs_delta_LINE_COINSRNC_AMT_10),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_11', metric_value := abs_delta_LINE_COINSRNC_AMT_11),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_12', metric_value := abs_delta_LINE_COINSRNC_AMT_12),
                    struct_pack(metric_name := 'abs_delta_LINE_COINSRNC_AMT_13', metric_value := abs_delta_LINE_COINSRNC_AMT_13),

                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_1',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_1),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_2',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_2),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_3',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_3),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_4',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_4),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_5',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_5),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_6',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_6),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_7',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_7),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_8',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_8),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_9',  metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_9),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_10', metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_10),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_11', metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_11),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_12', metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_12),
                    struct_pack(metric_name := 'abs_delta_LINE_ALOWD_CHRG_AMT_13', metric_value := abs_delta_LINE_ALOWD_CHRG_AMT_13)
                ] AS metrics
            FROM data_eng.main.vw_financial_differences_claim
        )
        SELECT
            unnest.metric_name,
            round(sum(unnest.metric_value),2) AS total_abs_delta
        FROM flat
        CROSS JOIN UNNEST(metrics)
        GROUP BY unnest.metric_name
        ORDER BY total_abs_delta DESC;"""
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Phase 4 SQL transformations completed successfully")
    except Exception as e:
        logger.error(f"Error running Phase 4 SQL transformations: {e}")
        raise
    
    logger.info("Starting Phase 5 SQL transformations.")
    sql_script = """/*SIX SIGMA ANALYSIS*/

        DROP VIEW IF EXISTS vw_sigma_analysis;
        CREATE VIEW vw_sigma_analysis AS
        WITH carrier_claim_dpmo AS (
            SELECT
                COUNT(*) AS TOTAL_UNITS,
                SUM(
                    ICD9_DGNS_CD_1 + ICD9_DGNS_CD_2 + 
                    ICD9_DGNS_CD_3 + ICD9_DGNS_CD_4 + ICD9_DGNS_CD_5 + ICD9_DGNS_CD_6 + 
                    ICD9_DGNS_CD_7 + ICD9_DGNS_CD_8 + PRF_PHYSN_NPI_1 + PRF_PHYSN_NPI_2 + 
                    PRF_PHYSN_NPI_3 + PRF_PHYSN_NPI_4 + PRF_PHYSN_NPI_5 + PRF_PHYSN_NPI_6 + 
                    PRF_PHYSN_NPI_7 + PRF_PHYSN_NPI_8 + PRF_PHYSN_NPI_9 + PRF_PHYSN_NPI_10 + 
                    PRF_PHYSN_NPI_11 + PRF_PHYSN_NPI_12 + PRF_PHYSN_NPI_13 + TAX_NUM_1 + 
                    TAX_NUM_2 + TAX_NUM_3 + TAX_NUM_4 + TAX_NUM_5 + TAX_NUM_6 + TAX_NUM_7 + 
                    TAX_NUM_8 + TAX_NUM_9 + TAX_NUM_10 + TAX_NUM_11 + TAX_NUM_12 + TAX_NUM_13 + 
                    HCPCS_CD_1 + HCPCS_CD_2 + HCPCS_CD_3 + HCPCS_CD_4 + HCPCS_CD_5 + 
                    HCPCS_CD_6 + HCPCS_CD_7 + HCPCS_CD_8 + HCPCS_CD_9 + HCPCS_CD_10 + 
                    HCPCS_CD_11 + HCPCS_CD_12 + HCPCS_CD_13 + LINE_NCH_PMT_AMT_1 + 
                    LINE_NCH_PMT_AMT_2 + LINE_NCH_PMT_AMT_3 + LINE_NCH_PMT_AMT_4 + 
                    LINE_NCH_PMT_AMT_5 + LINE_NCH_PMT_AMT_6 + LINE_NCH_PMT_AMT_7 + 
                    LINE_NCH_PMT_AMT_8 + LINE_NCH_PMT_AMT_9 + LINE_NCH_PMT_AMT_10 + 
                    LINE_NCH_PMT_AMT_11 + LINE_NCH_PMT_AMT_12 + LINE_NCH_PMT_AMT_13 + 
                    LINE_BENE_PTB_DDCTBL_AMT_1 + LINE_BENE_PTB_DDCTBL_AMT_2 + 
                    LINE_BENE_PTB_DDCTBL_AMT_3 + LINE_BENE_PTB_DDCTBL_AMT_4 + 
                    LINE_BENE_PTB_DDCTBL_AMT_5 + LINE_BENE_PTB_DDCTBL_AMT_6 + 
                    LINE_BENE_PTB_DDCTBL_AMT_7 + LINE_BENE_PTB_DDCTBL_AMT_8 + 
                    LINE_BENE_PTB_DDCTBL_AMT_9 + LINE_BENE_PTB_DDCTBL_AMT_10 + 
                    LINE_BENE_PTB_DDCTBL_AMT_11 + LINE_BENE_PTB_DDCTBL_AMT_12 + 
                    LINE_BENE_PTB_DDCTBL_AMT_13 + LINE_BENE_PRMRY_PYR_PD_AMT_1 + 
                    LINE_BENE_PRMRY_PYR_PD_AMT_2 + LINE_BENE_PRMRY_PYR_PD_AMT_3 + 
                    LINE_BENE_PRMRY_PYR_PD_AMT_4 + LINE_BENE_PRMRY_PYR_PD_AMT_5 + 
                    LINE_BENE_PRMRY_PYR_PD_AMT_6 + LINE_BENE_PRMRY_PYR_PD_AMT_7 + 
                    LINE_BENE_PRMRY_PYR_PD_AMT_8 + LINE_BENE_PRMRY_PYR_PD_AMT_9 + 
                    LINE_BENE_PRMRY_PYR_PD_AMT_10 + LINE_BENE_PRMRY_PYR_PD_AMT_11 + 
                    LINE_BENE_PRMRY_PYR_PD_AMT_12 + LINE_BENE_PRMRY_PYR_PD_AMT_13 + 
                    LINE_COINSRNC_AMT_1 + LINE_COINSRNC_AMT_2 + LINE_COINSRNC_AMT_3 + 
                    LINE_COINSRNC_AMT_4 + LINE_COINSRNC_AMT_5 + LINE_COINSRNC_AMT_6 + 
                    LINE_COINSRNC_AMT_7 + LINE_COINSRNC_AMT_8 + LINE_COINSRNC_AMT_9 + 
                    LINE_COINSRNC_AMT_10 + LINE_COINSRNC_AMT_11 + LINE_COINSRNC_AMT_12 + 
                    LINE_COINSRNC_AMT_13 + LINE_ALOWD_CHRG_AMT_1 + LINE_ALOWD_CHRG_AMT_2 + 
                    LINE_ALOWD_CHRG_AMT_3 + LINE_ALOWD_CHRG_AMT_4 + LINE_ALOWD_CHRG_AMT_5 + 
                    LINE_ALOWD_CHRG_AMT_6 + LINE_ALOWD_CHRG_AMT_7 + LINE_ALOWD_CHRG_AMT_8 + 
                    LINE_ALOWD_CHRG_AMT_9 + LINE_ALOWD_CHRG_AMT_10 + LINE_ALOWD_CHRG_AMT_11 + 
                    LINE_ALOWD_CHRG_AMT_12 + LINE_ALOWD_CHRG_AMT_13 + LINE_PRCSG_IND_CD_1 + 
                    LINE_PRCSG_IND_CD_2 + LINE_PRCSG_IND_CD_3 + LINE_PRCSG_IND_CD_4 + 
                    LINE_PRCSG_IND_CD_5 + LINE_PRCSG_IND_CD_6 + LINE_PRCSG_IND_CD_7 + 
                    LINE_PRCSG_IND_CD_8 + LINE_PRCSG_IND_CD_9 + LINE_PRCSG_IND_CD_10 + 
                    LINE_PRCSG_IND_CD_11 + LINE_PRCSG_IND_CD_12 + LINE_PRCSG_IND_CD_13 + 
                    LINE_ICD9_DGNS_CD_1 + LINE_ICD9_DGNS_CD_2 + LINE_ICD9_DGNS_CD_3 + 
                    LINE_ICD9_DGNS_CD_4 + LINE_ICD9_DGNS_CD_5 + LINE_ICD9_DGNS_CD_6 + 
                    LINE_ICD9_DGNS_CD_7 + LINE_ICD9_DGNS_CD_8 + LINE_ICD9_DGNS_CD_9 + 
                    LINE_ICD9_DGNS_CD_10 + LINE_ICD9_DGNS_CD_11 + LINE_ICD9_DGNS_CD_12 + 
                    LINE_ICD9_DGNS_CD_13
                ) AS TOTAL_DEFECTS,
                (COUNT(*) * 102) AS TOTAL_OPPORTUNITIES
            FROM audit_carrier_claims
        ),
        bene_summary_dpmo AS (
            SELECT
                COUNT(*) AS TOTAL_UNITS,
                SUM(
                    BENE_BIRTH_DT + BENE_DEATH_DT + BENE_SEX_IDENT_CD + BENE_RACE_CD + 
                    BENE_ESRD_IND + SP_STATE_CODE + BENE_COUNTY_CD + 
                    BENE_HI_CVRAGE_TOT_MONS + BENE_SMI_CVRAGE_TOT_MONS + 
                    BENE_HMO_CVRAGE_TOT_MONS + PLAN_CVRG_MOS_NUM + 
                    SP_ALZHDMTA + SP_CHF + SP_CHRNKIDN + SP_CNCR + SP_COPD + 
                    SP_DEPRESSN + SP_DIABETES + SP_ISCHMCHT + SP_OSTEOPRS + 
                    SP_RA_OA + SP_STRKETIA + MEDREIMB_IP + BENRES_IP + 
                    PPPYMT_IP + MEDREIMB_OP + BENRES_OP + PPPYMT_OP + 
                    MEDREIMB_CAR + BENRES_CAR + PPPYMT_CAR
                ) AS TOTAL_DEFECTS,
                (COUNT(*) * 31) AS TOTAL_OPPORTUNITIES
            FROM audit_beneficiary_summary
        )
        SELECT
            'Carrier Claims' AS SUBJECT,
            TOTAL_UNITS,
            TOTAL_DEFECTS,
            TOTAL_OPPORTUNITIES,
            (CAST(TOTAL_DEFECTS AS FLOAT) / TOTAL_OPPORTUNITIES) * 1000000 AS DPMO,
            (1 - (CAST(TOTAL_DEFECTS AS FLOAT) / TOTAL_OPPORTUNITIES)) AS YIELD,
            sigma_level(1 - (CAST(TOTAL_DEFECTS AS FLOAT) / TOTAL_OPPORTUNITIES)) AS SIGMA_LEVEL
        FROM carrier_claim_dpmo
        UNION ALL
        SELECT
            'Beneficiary Summary' AS SUBJECT,
            TOTAL_UNITS,
            TOTAL_DEFECTS,
            TOTAL_OPPORTUNITIES,
            (CAST(TOTAL_DEFECTS AS FLOAT) / TOTAL_OPPORTUNITIES) * 1000000 AS DPMO,
            (1 - (CAST(TOTAL_DEFECTS AS FLOAT) / TOTAL_OPPORTUNITIES)) AS YIELD,
            sigma_level(1 - (CAST(TOTAL_DEFECTS AS FLOAT) / TOTAL_OPPORTUNITIES)) AS SIGMA_LEVEL
        FROM bene_summary_dpmo;

        CREATE OR REPLACE VIEW vw_sigma_analysis_columns AS 
        -- Final Unified Column-Level Audit
        WITH carrier_columns AS (
            UNPIVOT audit_carrier_claims ON COLUMNS(* EXCLUDE (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT )) INTO NAME col VALUE def
        ),
        bene_columns AS (
            UNPIVOT audit_beneficiary_summary ON COLUMNS(* EXCLUDE (DESYNPUF_ID, "YEAR")) INTO NAME col VALUE def
        ),
        stacked_results AS (
            SELECT 'Carrier Claims' AS src, col, def FROM carrier_columns
            UNION ALL
            SELECT 'Beneficiary Summary' AS src, col, def FROM bene_columns
        )
        SELECT 
            src,
            col AS column_name,
            SUM(def) AS TOTAL_DEFECTS,
            COUNT(*) AS TOTAL_OPPORTUNITIES,
            (CAST(SUM(def) AS FLOAT) / COUNT(*)) * 1000000 AS DPMO,
            sigma_level(1 - (CAST(SUM(def) AS FLOAT) / COUNT(*))) AS SIGMA_LEVEL
        FROM stacked_results
        GROUP BY src, col
        ORDER BY src, TOTAL_DEFECTS DESC;

        -- SIX SIGMA ANALYSIS FOR CARRIER CLAIM FIELDS
        CREATE OR REPLACE VIEW vw_sigma_analysis_carrier_columns AS
        WITH base AS (
            SELECT
                src AS SOURCE,
                regexp_replace(column_name, '_[0-9]+$', '') AS FIELD_BASE,
                column_name AS FIELD,
                TOTAL_DEFECTS,
                TOTAL_OPPORTUNITIES,
                DPMO,
                SIGMA_LEVEL
            FROM data_eng.main.vw_sigma_analysis_columns
            WHERE src = 'Carrier Claims'
        ),
        agg AS (
            SELECT
                SOURCE,
                FIELD_BASE AS FIELD,
                sum(TOTAL_DEFECTS) AS TOTAL_DEFECTS,
                sum(TOTAL_DEFECTS) / max(TOTAL_OPPORTUNITIES) * 1000000 AS DPMO,
                sigma_level(1 - (CAST(sum(TOTAL_DEFECTS) AS FLOAT) / max(TOTAL_OPPORTUNITIES))) AS SIGMA_LEVEL
            FROM base
            GROUP BY SOURCE, FIELD_BASE
        ),
        with_running AS (
            SELECT
                SOURCE,
                FIELD AS FIELD_FAMILY,
                TOTAL_DEFECTS,
                DPMO,
                SIGMA_LEVEL,
                sum(TOTAL_DEFECTS) OVER (
                    PARTITION BY SOURCE
                    ORDER BY TOTAL_DEFECTS DESC, DPMO DESC, FIELD ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) / sum(TOTAL_DEFECTS) OVER (PARTITION BY SOURCE) AS RUNNING_PCT_OF_TOTAL
            FROM agg
        )
        SELECT
            SOURCE,
            FIELD_FAMILY,
            TOTAL_DEFECTS,
            RUNNING_PCT_OF_TOTAL,
            DPMO,
            SIGMA_LEVEL
        FROM with_running
        ORDER BY
            TOTAL_DEFECTS DESC,
            DPMO DESC,
            FIELD_FAMILY ASC;

        -- SIX SIGMA ANALYSIS FOR BENEFICIARY SUMMARY FIELDS
        CREATE OR REPLACE VIEW vw_sigma_analysis_beneficiary_columns AS
        WITH base AS (
            SELECT
                src AS SOURCE,
                regexp_replace(column_name, '_[0-9]+$', '') AS FIELD_BASE,
                column_name AS FIELD,
                TOTAL_DEFECTS,
                TOTAL_OPPORTUNITIES,
                DPMO,
                SIGMA_LEVEL
            FROM data_eng.main.vw_sigma_analysis_columns
            WHERE src = 'Beneficiary Summary'
        ),
        agg AS (
            SELECT
                SOURCE,
                FIELD_BASE AS FIELD,
                sum(TOTAL_DEFECTS) AS TOTAL_DEFECTS,
                sum(TOTAL_DEFECTS) / max(TOTAL_OPPORTUNITIES) * 1000000 AS DPMO,
                sigma_level(1 - (CAST(sum(TOTAL_DEFECTS) AS FLOAT) / max(TOTAL_OPPORTUNITIES))) AS SIGMA_LEVEL
            FROM base
            GROUP BY SOURCE, FIELD_BASE
        ),
        with_running AS (
            SELECT
                SOURCE,
                FIELD AS FIELD_FAMILY,
                TOTAL_DEFECTS,
                DPMO,
                SIGMA_LEVEL,
                sum(TOTAL_DEFECTS) OVER (
                    PARTITION BY SOURCE
                    ORDER BY TOTAL_DEFECTS DESC, DPMO DESC, FIELD ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) / sum(TOTAL_DEFECTS) OVER (PARTITION BY SOURCE) AS RUNNING_PCT_OF_TOTAL
            FROM agg
        )
        SELECT
            SOURCE,
            FIELD_FAMILY,
            TOTAL_DEFECTS,
            RUNNING_PCT_OF_TOTAL,
            DPMO,
            SIGMA_LEVEL
        FROM with_running
        ORDER BY
            TOTAL_DEFECTS DESC,
            DPMO DESC,
            FIELD_FAMILY ASC;"""
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Phase 5 SQL transformations completed successfully")
    except Exception as e:
        logger.error(f"Error running Phase 5 SQL transformations: {e}")
        raise

    
    logger.info("Starting Phase 6 SQL transformations.")
    sql_script = """-- Financial Impact Summary
        CREATE OR REPLACE VIEW vw_claim_financial_error_impact AS
        WITH base AS (
            SELECT 
                regexp_replace(
                    regexp_replace(metric_name, '^abs_delta_', ''),
                    '_[0-9]+$', ''
                ) AS FIELD_BASE,
                sum(total_abs_delta) AS FINANCIAL_VARIANCE
            FROM data_eng.main.audit_claim_financial_fields
            GROUP BY FIELD_BASE
        )
        SELECT 
            'Carrier Claims' as DATA_SET,
            FIELD_BASE AS FIELD_FAMILY,
            FINANCIAL_VARIANCE,
            sum(FINANCIAL_VARIANCE) OVER (
                ORDER BY FINANCIAL_VARIANCE DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            / sum(FINANCIAL_VARIANCE) OVER () AS RUNNING_PCT_OF_TOTAL
        FROM base
        ORDER BY FINANCIAL_VARIANCE DESC;

        -- Financial Impact Summary
        CREATE OR REPLACE VIEW vw_beneficiary_financial_error_impact AS
        WITH base AS (
            SELECT 
                regexp_replace(
                    regexp_replace(metric_name, '^abs_delta_', ''),
                    '_[0-9]+$', ''
                ) AS FIELD_BASE,
                sum(total_abs_delta) AS FINANCIAL_VARIANCE
            FROM data_eng.main.audit_beneficiary_financial_fields
            GROUP BY FIELD_BASE
        )
        SELECT 
            'Beneficiary Summary' as DATA_SET,
            FIELD_BASE AS FIELD_FAMILY,
            FINANCIAL_VARIANCE,
            sum(FINANCIAL_VARIANCE) OVER (
                ORDER BY FINANCIAL_VARIANCE DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            / sum(FINANCIAL_VARIANCE) OVER () AS RUNNING_PCT_OF_TOTAL
        FROM base
        ORDER BY FINANCIAL_VARIANCE DESC;"""
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Phase 6 SQL transformations completed successfully")
    except Exception as e:
        logger.error(f"Error running Phase 5 SQL transformations: {e}")
        raise

    logger.info("Starting Database Compression (Vacuum).")
    sql_script = """vacuum;"""
    try:
        execute_sql_script(sql_script)
        logger.info("✅ Database Compression (Vacuum) completed successfully")
    except Exception as e:
        logger.error(f"Error running Database Compression (Vacuum): {e}")
        #raise

    print("✅ Transform completed: Views and audit tables created successfully")
    
if __name__ == "__main__":
    main()