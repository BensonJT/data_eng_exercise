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
            select * from audit_beneficiary_summary 
            where 
                BENE_BIRTH_DT = 1 or
                BENE_DEATH_DT = 1 or
                BENE_SEX_IDENT_CD = 1 or
                BENE_RACE_CD = 1 or
                BENE_ESRD_IND = 1 or
                SP_STATE_CODE = 1 or
                BENE_COUNTY_CD = 1 or
                BENE_HI_CVRAGE_TOT_MONS = 1 or
                BENE_SMI_CVRAGE_TOT_MONS = 1 or
                BENE_HMO_CVRAGE_TOT_MONS = 1 or
                PLAN_CVRG_MOS_NUM = 1 or
                SP_ALZHDMTA = 1 or
                SP_CHF = 1 or
                SP_CHRNKIDN = 1 or
                SP_CNCR = 1 or
                SP_COPD = 1 or
                SP_DEPRESSN = 1 or
                SP_DIABETES = 1 or
                SP_ISCHMCHT = 1 or
                SP_OSTEOPRS = 1 or
                SP_RA_OA = 1 or
                SP_STRKETIA = 1 or
                MEDREIMB_IP = 1 or
                BENRES_IP = 1 or
                PPPYMT_IP = 1 or
                MEDREIMB_OP = 1 or
                BENRES_OP = 1 or
                PPPYMT_OP = 1 or
                MEDREIMB_CAR = 1 or
                BENRES_CAR = 1 or
                PPPYMT_CAR = 1
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
            SELECT * 
            FROM audit_carrier_claims
            WHERE 
                ICD9_DGNS_CD_1 = 1 OR
                ICD9_DGNS_CD_2 = 1 OR
                ICD9_DGNS_CD_3 = 1 OR
                ICD9_DGNS_CD_4 = 1 OR
                ICD9_DGNS_CD_5 = 1 OR
                ICD9_DGNS_CD_6 = 1 OR
                ICD9_DGNS_CD_7 = 1 OR
                ICD9_DGNS_CD_8 = 1 OR
                PRF_PHYSN_NPI_1 = 1 OR
                PRF_PHYSN_NPI_2 = 1 OR
                PRF_PHYSN_NPI_3 = 1 OR
                PRF_PHYSN_NPI_4 = 1 OR
                PRF_PHYSN_NPI_5 = 1 OR
                PRF_PHYSN_NPI_6 = 1 OR
                PRF_PHYSN_NPI_7 = 1 OR
                PRF_PHYSN_NPI_8 = 1 OR
                PRF_PHYSN_NPI_9 = 1 OR
                PRF_PHYSN_NPI_10 = 1 OR
                PRF_PHYSN_NPI_11 = 1 OR
                PRF_PHYSN_NPI_12 = 1 OR
                PRF_PHYSN_NPI_13 = 1 OR
                TAX_NUM_1 = 1 OR
                TAX_NUM_2 = 1 OR
                TAX_NUM_3 = 1 OR
                TAX_NUM_4 = 1 OR
                TAX_NUM_5 = 1 OR
                TAX_NUM_6 = 1 OR
                TAX_NUM_7 = 1 OR
                TAX_NUM_8 = 1 OR
                TAX_NUM_9 = 1 OR
                TAX_NUM_10 = 1 OR
                TAX_NUM_11 = 1 OR
                TAX_NUM_12 = 1 OR
                TAX_NUM_13 = 1 OR
                HCPCS_CD_1 = 1 OR
                HCPCS_CD_2 = 1 OR
                HCPCS_CD_3 = 1 OR
                HCPCS_CD_4 = 1 OR
                HCPCS_CD_5 = 1 OR
                HCPCS_CD_6 = 1 OR
                HCPCS_CD_7 = 1 OR
                HCPCS_CD_8 = 1 OR
                HCPCS_CD_9 = 1 OR
                HCPCS_CD_10 = 1 OR
                HCPCS_CD_11 = 1 OR
                HCPCS_CD_12 = 1 OR
                HCPCS_CD_13 = 1 OR
                LINE_NCH_PMT_AMT_1 = 1 OR
                LINE_NCH_PMT_AMT_2 = 1 OR
                LINE_NCH_PMT_AMT_3 = 1 OR
                LINE_NCH_PMT_AMT_4 = 1 OR
                LINE_NCH_PMT_AMT_5 = 1 OR
                LINE_NCH_PMT_AMT_6 = 1 OR
                LINE_NCH_PMT_AMT_7 = 1 OR
                LINE_NCH_PMT_AMT_8 = 1 OR
                LINE_NCH_PMT_AMT_9 = 1 OR
                LINE_NCH_PMT_AMT_10 = 1 OR
                LINE_NCH_PMT_AMT_11 = 1 OR
                LINE_NCH_PMT_AMT_12 = 1 OR
                LINE_NCH_PMT_AMT_13 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_1 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_2 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_3 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_4 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_5 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_6 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_7 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_8 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_9 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_10 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_11 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_12 = 1 OR
                LINE_BENE_PTB_DDCTBL_AMT_13 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_1 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_2 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_3 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_4 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_5 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_6 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_7 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_8 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_9 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_10 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_11 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_12 = 1 OR
                LINE_BENE_PRMRY_PYR_PD_AMT_13 = 1 OR
                LINE_COINSRNC_AMT_1 = 1 OR
                LINE_COINSRNC_AMT_2 = 1 OR
                LINE_COINSRNC_AMT_3 = 1 OR
                LINE_COINSRNC_AMT_4 = 1 OR
                LINE_COINSRNC_AMT_5 = 1 OR
                LINE_COINSRNC_AMT_6 = 1 OR
                LINE_COINSRNC_AMT_7 = 1 OR
                LINE_COINSRNC_AMT_8 = 1 OR
                LINE_COINSRNC_AMT_9 = 1 OR
                LINE_COINSRNC_AMT_10 = 1 OR
                LINE_COINSRNC_AMT_11 = 1 OR
                LINE_COINSRNC_AMT_12 = 1 OR
                LINE_COINSRNC_AMT_13 = 1 OR
                LINE_ALOWD_CHRG_AMT_1 = 1 OR
                LINE_ALOWD_CHRG_AMT_2 = 1 OR
                LINE_ALOWD_CHRG_AMT_3 = 1 OR
                LINE_ALOWD_CHRG_AMT_4 = 1 OR
                LINE_ALOWD_CHRG_AMT_5 = 1 OR
                LINE_ALOWD_CHRG_AMT_6 = 1 OR
                LINE_ALOWD_CHRG_AMT_7 = 1 OR
                LINE_ALOWD_CHRG_AMT_8 = 1 OR
                LINE_ALOWD_CHRG_AMT_9 = 1 OR
                LINE_ALOWD_CHRG_AMT_10 = 1 OR
                LINE_ALOWD_CHRG_AMT_11 = 1 OR
                LINE_ALOWD_CHRG_AMT_12 = 1 OR
                LINE_ALOWD_CHRG_AMT_13 = 1 OR
                LINE_PRCSG_IND_CD_1 = 1 OR
                LINE_PRCSG_IND_CD_2 = 1 OR
                LINE_PRCSG_IND_CD_3 = 1 OR
                LINE_PRCSG_IND_CD_4 = 1 OR
                LINE_PRCSG_IND_CD_5 = 1 OR
                LINE_PRCSG_IND_CD_6 = 1 OR
                LINE_PRCSG_IND_CD_7 = 1 OR
                LINE_PRCSG_IND_CD_8 = 1 OR
                LINE_PRCSG_IND_CD_9 = 1 OR
                LINE_PRCSG_IND_CD_10 = 1 OR
                LINE_PRCSG_IND_CD_11 = 1 OR
                LINE_PRCSG_IND_CD_12 = 1 OR
                LINE_PRCSG_IND_CD_13 = 1 OR
                LINE_ICD9_DGNS_CD_1 = 1 OR
                LINE_ICD9_DGNS_CD_2 = 1 OR
                LINE_ICD9_DGNS_CD_3 = 1 OR
                LINE_ICD9_DGNS_CD_4 = 1 OR
                LINE_ICD9_DGNS_CD_5 = 1 OR
                LINE_ICD9_DGNS_CD_6 = 1 OR
                LINE_ICD9_DGNS_CD_7 = 1 OR
                LINE_ICD9_DGNS_CD_8 = 1 OR
                LINE_ICD9_DGNS_CD_9 = 1 OR
                LINE_ICD9_DGNS_CD_10 = 1 OR
                LINE_ICD9_DGNS_CD_11 = 1 OR
                LINE_ICD9_DGNS_CD_12 = 1 OR
                LINE_ICD9_DGNS_CD_13 = 1
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
