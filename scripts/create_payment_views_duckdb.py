import sys
import os
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine

def create_duckdb_views():
    print("Creating DuckDB views for payment formulas...")
    
    # We will create two views: one for source, one for new.
    # Logic:
    # 1. Normalized Line Items (Unpivot)
    # 2. Apply Formula Logic (Case When)
    # 3. Aggregate by Bene and Year (extracted from Claim Date)
    
    tables = [
        ("src_carrier_claims", "view_calc_src_payments"), 
        ("new_carrier_claims", "view_calc_new_payments")
    ]
    
    with engine.connect() as conn:
        for table_name, view_name in tables:
            print(f"Creating {view_name} from {table_name}...")
            
            # Step 1: Normalize via UNION ALL
            # This is verbose but safer than complex UNPIVOT for multiple aligned columns
            
            selects = []
            for i in range(1, 14): # Lines 1 to 13
                # Casts might be needed if types differ, but in our schema they should be consistent
                # Note: Processing Indicator is String, Amounts are numeric.
                selects.append(f"""
                SELECT 
                    DESYNPUF_ID,
                    CLM_FROM_DT,
                    {i} AS LINE_NUM,
                    LINE_NCH_PMT_AMT_{i} AS nch_pmt,
                    LINE_BENE_PTB_DDCTBL_AMT_{i} AS deduct_amt,
                    LINE_COINSRNC_AMT_{i} AS coins_amt,
                    LINE_BENE_PRMRY_PYR_PD_AMT_{i} AS pry_payer_amt,
                    LINE_ALOWD_CHRG_AMT_{i} AS allow_chrg,
                    LINE_PRCSG_IND_CD_{i} AS prcsg_ind
                FROM {table_name}
                """)
            
            union_query = " UNION ALL ".join(selects)
            
            sql = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            WITH normalized_claims AS (
                {union_query}
            ),
            calculated_lines AS (
                SELECT
                    DESYNPUF_ID,
                    -- Extract Year from CLM_FROM_DT (YYYYMMDD) or use a robust date parser
                    CAST(SUBSTR(CAST(CLM_FROM_DT AS VARCHAR), 1, 4) AS INTEGER) AS YEAR,
                    
                    -- Formula 1: MEDREIMB_CAR
                    CASE 
                        WHEN prcsg_ind = 'A' OR (prcsg_ind IN ('R', 'S') AND allow_chrg > 0) 
                        THEN COALESCE(nch_pmt, 0) 
                        ELSE 0 
                    END AS CALC_MEDREIMB_CAR,
                    
                    -- Formula 2: BENRES_CAR
                    CASE 
                        WHEN prcsg_ind = 'A' OR (prcsg_ind IN ('R', 'S') AND allow_chrg > 0) 
                        THEN COALESCE(deduct_amt, 0) + COALESCE(coins_amt, 0)
                        ELSE 0 
                    END AS CALC_BENRES_CAR,
                    
                    -- Formula 3: PPPYMT_CAR
                    CASE 
                        WHEN prcsg_ind = 'A' OR (prcsg_ind IN ('R', 'S') AND allow_chrg > 0) 
                        THEN COALESCE(pry_payer_amt, 0)
                        ELSE 0 
                    END AS CALC_PPPYMT_CAR
                    
                FROM normalized_claims
            )
            SELECT 
                DESYNPUF_ID,
                YEAR,
                SUM(CALC_MEDREIMB_CAR) AS CALC_MEDREIMB_CAR,
                SUM(CALC_BENRES_CAR) AS CALC_BENRES_CAR,
                SUM(CALC_PPPYMT_CAR) AS CALC_PPPYMT_CAR
            FROM calculated_lines
            GROUP BY DESYNPUF_ID, YEAR;
            """
            
            try:
                # Iterate rows to verify? No, the execute is creating a View.
                conn.execute(text(sql))
                conn.commit() 
                print(f"Successfully created view: {view_name}")
            except Exception as e:
                conn.rollback()
                print(f"Error creating view {view_name}: {e}")
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    create_duckdb_views()
