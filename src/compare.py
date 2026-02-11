import pandas as pd
from sqlalchemy import text
from src.db import engine

def get_row_counts():
    """Returns row counts for all tables."""
    with engine.connect() as conn:
        counts = {}
        tables = [
            'src_beneficiary_summary', 'new_beneficiary_summary',
            'src_carrier_claims', 'new_carrier_claims'
        ]
        for table in tables:
            query = text(f"SELECT COUNT(*) FROM {table}")
            counts[table] = conn.execute(query).scalar()
    return counts

def compare_beneficiaries():
    """
    Compares source and new subscriber/beneficiary tables.
    Returns a summary of discrepancies.
    """
    with engine.connect() as conn:
        # 1. Identify missing beneficiaries in New System
        missing_query = text("""
            SELECT s.desynpuf_id, s.year
            FROM src_beneficiary_summary s
            LEFT JOIN new_beneficiary_summary n 
            ON s.desynpuf_id = n.desynpuf_id AND s.year = n.year
            WHERE n.desynpuf_id IS NULL
        """)
        missing_df = pd.read_sql(missing_query, conn)
        
        # 2. Identify extra beneficiaries in New System (unexpected)
        extra_query = text("""
            SELECT n.desynpuf_id, n.year
            FROM new_beneficiary_summary n
            LEFT JOIN src_beneficiary_summary s 
            ON n.desynpuf_id = s.desynpuf_id AND n.year = s.year
            WHERE s.desynpuf_id IS NULL
        """)
        extra_df = pd.read_sql(extra_query, conn)
        
        # 3. Compare overlap for critical columns
        # We'll focus on a few key columns for this exercise
        mismatch_query = text("""
            SELECT 
                s.desynpuf_id, s.year,
                s.bene_birth_dt as src_dob, n.bene_birth_dt as new_dob,
                s.bene_sex_ident_cd as src_sex, n.bene_sex_ident_cd as new_sex,
                s.bene_esrd_ind as src_esrd, n.bene_esrd_ind as new_esrd
            FROM src_beneficiary_summary s
            JOIN new_beneficiary_summary n 
            ON s.desynpuf_id = n.desynpuf_id AND s.year = n.year
            WHERE 
                s.bene_birth_dt IS DISTINCT FROM n.bene_birth_dt OR
                s.bene_sex_ident_cd IS DISTINCT FROM n.bene_sex_ident_cd OR
                s.bene_esrd_ind IS DISTINCT FROM n.bene_esrd_ind
        """)
        mismatch_df = pd.read_sql(mismatch_query, conn)
        
    return {
        "missing_count": len(missing_df),
        "extra_count": len(extra_df),
        "mismatch_count": len(mismatch_df),
        "missing_sample": missing_df.head(),
        "extra_sample": extra_df.head(),
        "mismatch_sample": mismatch_df.head()
    }

def compare_claims():
    """
    Compares source and new carrier claims.
    """
    with engine.connect() as conn:
        # 1. Claims present in Source but missing in New
        missing_query = text("""
            SELECT s.clm_id
            FROM src_carrier_claims s
            LEFT JOIN new_carrier_claims n ON s.clm_id = n.clm_id
            WHERE n.clm_id IS NULL
        """)
        missing_df = pd.read_sql(missing_query, conn)
        
        # 2. Payment Amount Discrepancies
        # Compare Line 1 Payment amount as a proxy for financial accuracy
        payment_diff_query = text("""
            SELECT 
                s.clm_id,
                s.line_nch_pmt_amt_1 as src_pmt,
                n.line_nch_pmt_amt_1 as new_pmt,
                (s.line_nch_pmt_amt_1 - n.line_nch_pmt_amt_1) as diff
            FROM src_carrier_claims s
            JOIN new_carrier_claims n ON s.clm_id = n.clm_id
            WHERE s.line_nch_pmt_amt_1 IS DISTINCT FROM n.line_nch_pmt_amt_1
        """)
        payment_diff_df = pd.read_sql(payment_diff_query, conn)
        
    return {
        "missing_claims_count": len(missing_df),
        "payment_mismatch_count": len(payment_diff_df),
        "payment_mismatch_sample": payment_diff_df.head()
    }

def run_comparison():
    print("--- Row Counts ---")
    counts = get_row_counts()
    for table, count in counts.items():
        print(f"{table}: {count}")
        
    print("\n--- Beneficiary Comparison ---")
    bene_res = compare_beneficiaries()
    print(f"Missing in New: {bene_res['missing_count']}")
    print(f"Extra in New: {bene_res['extra_count']}")
    print(f"Mismatched Attributes: {bene_res['mismatch_count']}")
    
    print("\n--- Claims Comparison ---")
    claims_res = compare_claims()
    print(f"Missing Claims in New: {claims_res['missing_claims_count']}")
    print(f"Payment Mismatches: {claims_res['payment_mismatch_count']}")

if __name__ == "__main__":
    run_comparison()
