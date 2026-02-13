import sys
import os
import pandas as pd
import json
from dotenv import load_dotenv
from sqlalchemy.orm import Session

# Load environment variables
load_dotenv()

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine
from src.models import Base, LookupPaymentFormulas

def ingest_formulas():
    print("Ingesting payment formulas...")
    
    # Ensure table exists (drop first to handle schema change)
    LookupPaymentFormulas.__table__.drop(engine, checkfirst=True)
    Base.metadata.create_all(bind=engine)
    
    # Hardcoded JSON string
    formulas = """[
        {
            "Variable names": "MEDREIMB_IP",
            "Labels": "Inpatient annual Medicare reimbursement amount",
            "Sum of": "CLM_PMT_AMT + CLM_UTLZTN_DAY_CNT * CLM_PASS_THRU_PER_DIEM_AMT",
            "Type": "NUM"
        },
        {
            "Variable names": "BENRES_IP",
            "Labels": "Inpatient annual beneficiary responsibility amount",
            "Sum of": "NCH_BENE_IP_DDCTBL_AMT + NCH_BENE_PTA_COINSRNC_LBLTY_AM + NCH_BENE_BLOOD_DDCTBL_LBLTY_AM",
            "Type": "NUM"
        },
        {
            "Variable names": "PPPYMT_IP",
            "Labels": "Inpatient annual primary payer reimbursement amount",
            "Sum of": "NCH_PRMRY_PYR_CLM_PD_AMT",
            "Type": "NUM"
        },
        {
            "Variable names": "MEDREIMB_OP",
            "Labels": "Outpatient Institutional annual Medicare reimbursement amount",
            "Sum of": "CLM_PMT_AMT",
            "Type": "NUM"
        },
        {
            "Variable names": "BENRES_OP",
            "Labels": "Outpatient Institutional annual beneficiary responsibility amount",
            "Sum of": "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM + NCH_BENE_PTB_DDCTBL_AMT + NCH_BENE_PTB_COINSRNC_AMT",
            "Type": "NUM"
        },
        {
            "Variable names": "PPPYMT_OP",
            "Labels": "Outpatient Institutional annual primary payer reimbursement amount",
            "Sum of": "NCH_PRMRY_PYR_CLM_PD_AMT",
            "Type": "NUM"
        },
        {
            "Variable names": "MEDREIMB_CAR",
            "Labels": "Carrier annual Medicare reimbursement amount",
            "Sum of": "LINE_NCH_PMT_AMT IF LINE_PRCSG_IND_CD = 'A' OR (LINE_PRCSG_IND_CD IN ('R','S') & LINE_ALOWD_CHRG_AMT > 0)",
            "Type": "NUM"
        },
        {
            "Variable names": "BENRES_CAR",
            "Labels": "Carrier annual beneficiary responsibility amount",
            "Sum of": "LINE_BENE_PTB_DDCTBL_AMT + LINE_COINSRNC_AMT IF LINE_PRCSG_IND_CD = 'A' OR (LINE_PRCSG_IND_CD IN ('R','S') & LINE_ALOWD_CHRG_AMT > 0)",
            "Type": "NUM"
        },
        {
            "Variable names": "PPPYMT_CAR",
            "Labels": "Carrier annual primary payer reimbursement amount",
            "Sum of": "LINE_BENE_PRMRY_PYR_PD_AMT IF LINE_PRCSG_IND_CD = 'A' OR (LINE_PRCSG_IND_CD IN ('R','S') & LINE_ALOWD_CHRG_AMT > 0)",
            "Type": "NUM"
        }
        ]"""

    try:
        # Parse JSON string
        data = json.loads(formulas)
            
        records = []
        for item in data:
            record = LookupPaymentFormulas(
                variable_name=item.get("Variable names"),
                label=item.get("Labels"),
                calculation_logic=item.get("Sum of"),
                variable_type=item.get("Type")
            )
            records.append(record)
            
        session = Session(bind=engine)
        try:
            session.add_all(records)
            session.commit()
            print(f"Successfully ingested {len(records)} payment formulas.")
        except Exception as e:
            session.rollback()
            print(f"Error inserting formulas: {e}")
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error parsing JSON: {e}")

if __name__ == "__main__":
    ingest_formulas()
