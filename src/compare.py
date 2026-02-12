import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import text
from src.db import engine
from src.db import execute_sql_script
import os

# Get the project root directory (parent of src/)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(project_root, "data")

def accurate_sigma_level(yield_value):
    """
    Calculate accurate Six Sigma level using scipy's inverse normal distribution.
    
    Args:
        yield_value: Proportion of defect-free items (0 to 1)
    
    Returns:
        float: Six Sigma level (typically 0 to 6+)
    
    Note:
        This replaces the SQL approximation with the precise inverse normal CDF.
        The 1.5 sigma shift is a standard industry adjustment for long-term drift.
    """
    if pd.isna(yield_value) or yield_value <= 0 or yield_value >= 1:
        return np.nan
    
    try:
        # Calculate Z-score from yield using inverse normal CDF
        # stats.norm.ppf gives us the inverse of the normal cumulative distribution
        z_score = stats.norm.ppf(yield_value)
        
        # Apply 1.5 sigma shift (standard Six Sigma methodology)
        sigma_level = z_score + 1.5
        
        return sigma_level
    except (ValueError, FloatingPointError):
        return np.nan

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

def calc_six_sigma():
    """
    Calculate Six Sigma to score Data Migration Quality.
    Recalculates sigma values using accurate scipy inverse normal distribution.
    """
    with engine.connect() as conn:

        schema_query = text("""
            select * from vw_db_schema order by table_name, column_name
        """)
        schema_df = pd.read_sql(schema_query, conn)
        schema_df.to_csv(os.path.join(data_dir, "schema.csv"), index=False)
        
        # 1. Overall Six Sigma Analysis
        six_sigma_query = text("""
            select * from vw_sigma_analysis
        """)
        six_sigma_df = pd.read_sql(six_sigma_query, conn)
        
        # Recalculate SIX_SIGMA using accurate Python function
        if 'YIELD' in six_sigma_df.columns:
            six_sigma_df['SIX_SIGMA'] = six_sigma_df['YIELD'].apply(accurate_sigma_level)
        
        six_sigma_df.to_csv(os.path.join(data_dir, "six_sigma.csv"), index=False)
        
        # 2. Six Sigma Analysis for Carrier Claim Fields
        six_sigma_carrier_query = text("""
            select * from vw_sigma_analysis_carrier_columns;
        """)
        six_sigma_carrier_df = pd.read_sql(six_sigma_carrier_query, conn)
        
        # Recalculate SIX_SIGMA using accurate Python function
        if 'YIELD' in six_sigma_carrier_df.columns:
            six_sigma_carrier_df['SIX_SIGMA'] = six_sigma_carrier_df['YIELD'].apply(accurate_sigma_level)
        
        six_sigma_carrier_df.to_csv(os.path.join(data_dir, "six_sigma_carrier.csv"), index=False)

        # 3. Six Sigma Analysis for Beneficiary Fields
        six_sigma_beneficiary_query = text("""
            select * from vw_sigma_analysis_beneficiary_columns
        """)
        six_sigma_beneficiary_df = pd.read_sql(six_sigma_beneficiary_query, conn)
        
        # Recalculate SIX_SIGMA using accurate Python function
        if 'YIELD' in six_sigma_beneficiary_df.columns:
            six_sigma_beneficiary_df['SIX_SIGMA'] = six_sigma_beneficiary_df['YIELD'].apply(accurate_sigma_level)
        
        six_sigma_beneficiary_df.to_csv(os.path.join(data_dir, "six_sigma_beneficiary.csv"), index=False)
        
        # 4. Six Sigma Analysis for All Fields (not grouped)
        six_sigma_columns_query = text("""
            select * from vw_sigma_analysis_columns
        """)
        six_sigma_columns_df = pd.read_sql(six_sigma_columns_query, conn)
        
        # Recalculate SIX_SIGMA using accurate Python function
        if 'YIELD' in six_sigma_columns_df.columns:
            six_sigma_columns_df['SIX_SIGMA'] = six_sigma_columns_df['YIELD'].apply(accurate_sigma_level)
        
        six_sigma_columns_df.to_csv(os.path.join(data_dir, "six_sigma_columns.csv"), index=False)
        
    return {
        "six_sigma": six_sigma_df,
        "six_sigma_carrier": six_sigma_carrier_df,
        "six_sigma_beneficiary": six_sigma_beneficiary_df
    }

def calc_financial_impact():
    """
    Calculate Financial Impact of Data Migration Errors
    """
    with engine.connect() as conn:
        # 1. Overall Financial Impact Analysis
        financial_impact_claim_query = text("""
            select * from vw_claim_financial_error_impact
        """)
        financial_impact_claim_df = pd.read_sql(financial_impact_claim_query, conn)
        financial_impact_claim_df.to_csv(os.path.join(data_dir, "financial_impact_carrier_claim.csv"), index=False)
        
        # 2. Financial Impact Analysis for Carrier Claim Fields
        financial_impact_beneficiary_query = text("""
            select * from vw_beneficiary_financial_error_impact;
        """)
        financial_impact_beneficiary_df = pd.read_sql(financial_impact_beneficiary_query, conn)
        financial_impact_beneficiary_df.to_csv(os.path.join(data_dir, "financial_impact_beneficiary.csv"), index=False)
        
    return {
        "financial_impact_claim": financial_impact_claim_df,
        "financial_impact_beneficiary": financial_impact_beneficiary_df
    }

def compare_beneficiaries():
    """
    Compares source and new subscriber/beneficiary tables.
    Returns a summary of discrepancies.
    """
    with engine.connect() as conn:
        # 1. Identify missing beneficiaries in New System
        missing_query_1 = text("""
            select * from vw_beneficiary_errors where finding = 'Error: Beneficiary Missing in New File'
        """)
        missing_df_1 = pd.read_sql(missing_query_1, conn)
        missing_df_1.to_csv(os.path.join(data_dir, "missing_beneficiaries.csv"), index=False)

        # 2. Extra beneficiaries in New System (unexpected)
        missing_query_2 = text("""
            select * from vw_beneficiary_errors where finding = E'rror: Beneficiary Present in New File, Not in Original File'
        """)
        missing_df_2 = pd.read_sql(missing_query_2, conn)
        missing_df_2.to_csv(os.path.join(data_dir, "extra_beneficiaries.csv"), index=False)

        # 3. Identify Beneficiary Attributes that Changed (unexpected)
        mismatch_query = text("""
            select * from vw_beneficiary_attribute_errors
        """)
        mismatch_df = pd.read_sql(mismatch_query, conn)
        mismatch_df.to_csv(os.path.join(data_dir, "beneficiary_attribute_mismatches.csv"), index=False)

        # 4. Focus on Beneficiary Date Differences (unexpected)
        date_diff_query = text("""
            select * from vw_bene_dt_differences
        """)
        date_diff_df = pd.read_sql(date_diff_query, conn)
        date_diff_df.to_csv(os.path.join(data_dir, "beneficiary_date_differences.csv"), index=False)

        # 5. Comprehensive Beneficiary Differences
        comprehensive_query = text("""
            select * from vw_beneficiary_lines_not_identical
        """)
        comprehensive_df = pd.read_sql(comprehensive_query, conn)
        comprehensive_df.to_csv(os.path.join(data_dir, "comprehensive_beneficiary_differences.csv"), index=False)        

        # 6. Audit Beneficiary Summary
        audit_beneficiary_query = text("""
            select * from vw_beneficiary_audit_summary
        """)
        audit_beneficiary_df = pd.read_sql(audit_beneficiary_query, conn)
        audit_beneficiary_df.to_csv(os.path.join(data_dir, "audit_beneficiary_summary.csv"), index=False)
    
    return {
        "bene_records_with_discrepancies": len(audit_beneficiary_df),
        "missing_count": len(missing_df_1) + len(missing_df_2),
        "mismatch_count": len(mismatch_df),
        "date_diff_count": len(date_diff_df),
        "comprehensive_beneficiary_count": len(comprehensive_df),
        "missing_sample": missing_df_1.head(),
        "extra_sample": missing_df_2.head(),
        "bene_date_changes": date_diff_df.head(),
        "mismatch_sample": mismatch_df.head(),
        "comprehensive_sample": comprehensive_df.head(),
        "bene_records_with_discrepancies_sample": audit_beneficiary_df.head()
    }

def compare_claims():
    """
    Compares source and new carrier claims.
    """
    with engine.connect() as conn:
        # 1. Claims present in Source but missing in New
        missing_query = text("""
            select * from vw_claim_mismatches
        """)
        missing_df = pd.read_sql(missing_query, conn)
        missing_df.to_csv(os.path.join(data_dir, "missing_claims.csv"), index=False)        
        # 2. Payment Amount Discrepancies
        # Compare Line 1 Payment amount as a proxy for financial accuracy
        payment_diff_query_sample = text("""
            select * from vw_claim_line_nch_pmt_amt_1_differences;
        """)
        payment_diff_df_sample = pd.read_sql(payment_diff_query_sample, conn)
        payment_diff_df_sample.to_csv(os.path.join(data_dir, "claim_payment_amount_discrepancies.csv"), index=False)
        # 3. Comprehensive List of Orphan Claims
        # When doing a 4 way match(DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT)
        # there are many more orphan claims in the new system
        comprehensive_orphan_claims = text("""
            select * from carrier_claims_orphans
        """)
        comprehensive_orphan_claims_df = pd.read_sql(comprehensive_orphan_claims, conn)
        comprehensive_orphan_claims_df.to_csv(os.path.join(data_dir, "comprehensive_orphan_claims.csv"), index=False)

        # 4. Audit Claim Summary
        audit_claim_query = text("""
            select * from audit_carrier_claims
        """)
        audit_claim_df = pd.read_sql(audit_claim_query, conn)
        audit_claim_df.to_csv(os.path.join(data_dir, "audit_claim_summary.csv"), index=False)

    return {
        "claim_records_with_discrepancies": len(audit_claim_df),
        "missing_claims_count": len(missing_df),
        "payment_mismatch_count": len(payment_diff_df_sample),
        "comprehensive_orphan_claims_count": len(comprehensive_orphan_claims_df),
        "payment_mismatch_sample": payment_diff_df_sample.head(),
        "comprehensive_orphan_claims_sample": comprehensive_orphan_claims_df.head(),
        "claim_records_with_discrepancies_sample": audit_claim_df.head()
    }

def run_comparison():
    print("--- Row Counts ---")
    counts = get_row_counts()
    for table, count in counts.items():
        print(f"{table}: {count}")
        

if __name__ == "__main__":
    run_comparison()
