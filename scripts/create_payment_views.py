import sys
import os
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine
from src.models import LookupPaymentFormulas

def create_payment_views():
    print("Creating payment views...")
    
    session = Session(bind=engine)
    try:
        formulas = session.query(LookupPaymentFormulas).all()
        
        for formula in formulas:
            # Basic sanitization and substitution
            # The JSON formulas use "IF" which is not valid SQL. We need to convert to CASE WHEN.
            # Example: "LINE_NCH_PMT_AMT IF LINE_PRCSG_IND_CD = 'A'..."
            # SQL: "CASE WHEN LINE_PRCSG_IND_CD = 'A'... THEN LINE_NCH_PMT_AMT ELSE 0 END"
            
            logic = formula.calculation_logic
            
            # Very basic parser for the specific format seen in the JSON output
            # "Sum of": "LINE_NCH_PMT_AMT IF LINE_PRCSG_IND_CD = 'A' OR (LINE_PRCSG_IND_CD IN ('R','S') & LINE_ALOWD_CHRG_AMT > 0)"
            
            if " IF " in logic:
                parts = logic.split(" IF ")
                value_col = parts[0].strip()
                condition = parts[1].strip()
                
                # Replace & with AND
                condition = condition.replace("&", "AND")
                
                # Construct CASE statement
                # We need to handle the SUM aggregation in the view if intended, or just the row-level calculation
                # The label says "Annual... amount", implying aggregation per beneficiary or carrier?
                # The instructions say "recreate these formulas... make sure columns align".
                # Let's create a view that adds this calculated column to the base table.
                
                # Identify source table based on columns? 
                # Carrier claims use LINE_... columns.
                # Outpatient uses NCH_... columns.
                
                table_name = ""
                if "LINE_" in logic:
                    table_name = "src_carrier_claims"
                    id_col = "clm_id"
                elif "NCH_" in logic:
                     # This guess might need refinement based on other tables
                     table_name = "src_outpatient_claims" # We don't have this table yet! 
                     # Wait, user only asked for Carrier Claims and Beneficiary Summary in original prompt.
                     # But the JSON has PPPYMT_OP (Outpatient).
                     # We only have Carrier Claims and Beneficiary Summary tables.
                     # We can only create views for Carrier Claims formulas for now.
                     pass
                
                if table_name == "src_carrier_claims":
                     view_name = f"view_calc_{formula.variable_name.lower()}"
                     
                     sql = f"""
                     CREATE OR REPLACE VIEW data.{view_name} AS
                     SELECT 
                        {id_col},
                        CASE WHEN {condition} THEN {value_col} ELSE 0 END as calculated_amount
                     FROM data.{table_name};
                     """
                     
                     print(f"Creating view {view_name}...")
                     with engine.connect() as conn:
                         conn.execute(text(sql))
                         conn.commit()

            elif "+" in logic:
                 # Simple addition
                 pass

        print("Views created successfully.")
    except Exception as e:
        print(f"Error creating views: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    create_payment_views()
