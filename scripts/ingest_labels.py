import sys
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.orm import Session

# Load environment variables
load_dotenv()

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine
from src.models import Base, LookupVariableLabels

def ingest_labels():
    print("Ingesting variable labels...")
    
    # Ensure table exists (drop first to handle schema change)
    LookupVariableLabels.__table__.drop(engine, checkfirst=True)
    Base.metadata.create_all(bind=engine)
    
    # Embedded CSV data
    labels_data = [
        {"File Source": "Beneficiary Summary", "#": "1", "Variable Names": "DESYNPUF_ID", "Labels": "DESYNPUF: Beneficiary Code"},
        {"File Source": "Beneficiary Summary", "#": "2", "Variable Names": "BENE_BIRTH_DT", "Labels": "DESYNPUF: Date of birth"},
        {"File Source": "Beneficiary Summary", "#": "3", "Variable Names": "BENE_DEATH_DT", "Labels": "DESYNPUF: Date of death"},
        {"File Source": "Beneficiary Summary", "#": "4", "Variable Names": "BENE_SEX_IDENT_CD", "Labels": "DESYNPUF: Sex"},
        {"File Source": "Beneficiary Summary", "#": "5", "Variable Names": "BENE_RACE_CD", "Labels": "DESYNPUF: Beneficiary Race Code"},
        {"File Source": "Beneficiary Summary", "#": "6", "Variable Names": "BENE_ESRD_IND", "Labels": "DESYNPUF: End Stage Renal Disease Indicator"},
        {"File Source": "Beneficiary Summary", "#": "7", "Variable Names": "SP_STATE_CODE", "Labels": "DESYNPUF: State Code"},
        {"File Source": "Beneficiary Summary", "#": "8", "Variable Names": "BENE_COUNTY_CD", "Labels": "DESYNPUF: County Code"},
        {"File Source": "Beneficiary Summary", "#": "9", "Variable Names": "BENE_HI_CVRAGE_TOT_MONS", "Labels": "DESYNPUF: Total number of months of part A coverage for the beneficiary."},
        {"File Source": "Beneficiary Summary", "#": "10", "Variable Names": "BENE_SMI_CVRAGE_TOT_MONS", "Labels": "DESYNPUF: Total number of months of part B coverage for the beneficiary."},
        {"File Source": "Beneficiary Summary", "#": "11", "Variable Names": "BENE_HMO_CVRAGE_TOT_MONS", "Labels": "DESYNPUF: Total number of months of HMO coverage for the beneficiary."},
        {"File Source": "Beneficiary Summary", "#": "12", "Variable Names": "PLAN_CVRG_MOS_NUM", "Labels": "DESYNPUF: Total number of months of part D plan coverage for the beneficiary."},
        {"File Source": "Beneficiary Summary", "#": "13", "Variable Names": "SP_ALZHDMTA", "Labels": "DESYNPUF: Chronic Condition: Alzheimer or related disorders or senile"},
        {"File Source": "Beneficiary Summary", "#": "14", "Variable Names": "SP_CHF", "Labels": "DESYNPUF: Chronic Condition: Heart Failure"},
        {"File Source": "Beneficiary Summary", "#": "15", "Variable Names": "SP_CHRNKIDN", "Labels": "DESYNPUF: Chronic Condition: Chronic Kidney Disease"},
        {"File Source": "Beneficiary Summary", "#": "16", "Variable Names": "SP_CNCR", "Labels": "DESYNPUF: Chronic Condition: Cancer"},
        {"File Source": "Beneficiary Summary", "#": "17", "Variable Names": "SP_COPD", "Labels": "DESYNPUF: Chronic Condition: Chronic Obstructive Pulmonary Disease"},
        {"File Source": "Beneficiary Summary", "#": "18", "Variable Names": "SP_DEPRESSN", "Labels": "DESYNPUF: Chronic Condition: Depression"},
        {"File Source": "Beneficiary Summary", "#": "19", "Variable Names": "SP_DIABETES", "Labels": "DESYNPUF: Chronic Condition: Diabetes"},
        {"File Source": "Beneficiary Summary", "#": "20", "Variable Names": "SP_ISCHMCHT", "Labels": "DESYNPUF: Chronic Condition: Ischemic Heart Disease"},
        {"File Source": "Beneficiary Summary", "#": "21", "Variable Names": "SP_OSTEOPRS", "Labels": "DESYNPUF: Chronic Condition: Osteoporosis"},
        {"File Source": "Beneficiary Summary", "#": "22", "Variable Names": "SP_RA_OA", "Labels": "DESYNPUF: Chronic Condition: rheumatoid arthritis and osteoarthritis (RA/OA)"},
        {"File Source": "Beneficiary Summary", "#": "23", "Variable Names": "SP_STRKETIA", "Labels": "DESYNPUF: Chronic Condition: Stroke/transient Ischemic Attack"},
        {"File Source": "Beneficiary Summary", "#": "24", "Variable Names": "MEDREIMB_IP", "Labels": "DESYNPUF: Inpatient annual Medicare reimbursement amount"},
        {"File Source": "Beneficiary Summary", "#": "25", "Variable Names": "BENRES_IP", "Labels": "DESYNPUF: Inpatient annual beneficiary responsibility amount"},
        {"File Source": "Beneficiary Summary", "#": "26", "Variable Names": "PPPYMT_IP", "Labels": "DESYNPUF: Inpatient annual primary payer reimbursement amount"},
        {"File Source": "Beneficiary Summary", "#": "27", "Variable Names": "MEDREIMB_OP", "Labels": "DESYNPUF: Outpatient Institutional annual Medicare reimbursement amount"},
        {"File Source": "Beneficiary Summary", "#": "28", "Variable Names": "BENRES_OP", "Labels": "DESYNPUF: Outpatient Institutional annual beneficiary responsibility amount"},
        {"File Source": "Beneficiary Summary", "#": "29", "Variable Names": "PPPYMT_OP", "Labels": "DESYNPUF: Outpatient Institutional annual primary payer reimbursement amount"},
        {"File Source": "Beneficiary Summary", "#": "30", "Variable Names": "MEDREIMB_CAR", "Labels": "DESYNPUF: Carrier annual Medicare reimbursement amount"},
        {"File Source": "Beneficiary Summary", "#": "31", "Variable Names": "BENRES_CAR", "Labels": "DESYNPUF: Carrier annual beneficiary responsibility amount"},
        {"File Source": "Beneficiary Summary", "#": "32", "Variable Names": "PPPYMT_CAR", "Labels": "DESYNPUF: Carrier annual primary payer reimbursement amount"},
        {"File Source": "Inpatient Claims", "#": "1", "Variable Names": "DESYNPUF_ID", "Labels": "DESYNPUF: Beneficiary Code"},
        {"File Source": "Inpatient Claims", "#": "2", "Variable Names": "CLM_ID", "Labels": "DESYNPUF: Claim ID"},
        {"File Source": "Inpatient Claims", "#": "3", "Variable Names": "SEGMENT", "Labels": "DESYNPUF: Claim Line Segment"},
        {"File Source": "Inpatient Claims", "#": "4", "Variable Names": "CLM_FROM_DT", "Labels": "DESYNPUF: Claims start date"},
        {"File Source": "Inpatient Claims", "#": "5", "Variable Names": "CLM_THRU_DT", "Labels": "DESYNPUF: Claims end date"},
        {"File Source": "Inpatient Claims", "#": "6", "Variable Names": "PRVDR_NUM", "Labels": "DESYNPUF: Provider Institution"},
        {"File Source": "Inpatient Claims", "#": "7", "Variable Names": "CLM_PMT_AMT", "Labels": "DESYNPUF: Claim Payment Amount"},
        {"File Source": "Inpatient Claims", "#": "8", "Variable Names": "NCH_PRMRY_PYR_CLM_PD_AMT", "Labels": "DESYNPUF: NCH Primary Payer Claim Paid Amount"},
        {"File Source": "Inpatient Claims", "#": "9", "Variable Names": "AT_PHYSN_NPI", "Labels": "DESYNPUF: Attending Physician - National Provider Identifier Number"},
        {"File Source": "Inpatient Claims", "#": "10", "Variable Names": "OP_PHYSN_NPI", "Labels": "DESYNPUF: Operating Physician - National Provider Identifier Number"},
        {"File Source": "Inpatient Claims", "#": "11", "Variable Names": "OT_PHYSN_NPI", "Labels": "DESYNPUF: Other Physician - National Provider Identifier Number"},
        {"File Source": "Inpatient Claims", "#": "12", "Variable Names": "CLM_ADMSN_DT", "Labels": "DESYNPUF: Inpatient admission date"},
        {"File Source": "Inpatient Claims", "#": "13", "Variable Names": "ADMTNG_ICD9_DGNS_CD", "Labels": "DESYNPUF: Claim Admitting Diagnosis Code"},
        {"File Source": "Inpatient Claims", "#": "14", "Variable Names": "CLM_PASS_THRU_PER_DIEM_AMT", "Labels": "DESYNPUF: Claim Pass Thru Per Diem Amount"},
        {"File Source": "Inpatient Claims", "#": "15", "Variable Names": "NCH_BENE_IP_DDCTBL_AMT", "Labels": "DESYNPUF: NCH Beneficiary Inpatient Deductible Amount"},
        {"File Source": "Inpatient Claims", "#": "16", "Variable Names": "NCH_BENE_PTA_COINSRNC_LBLTY_AM", "Labels": "DESYNPUF: NCH Beneficiary Part A Coinsurance Liability Amount"},
        {"File Source": "Inpatient Claims", "#": "17", "Variable Names": "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM", "Labels": "DESYNPUF: NCH Beneficiary Blood Deductible Liability Amount"},
        {"File Source": "Inpatient Claims", "#": "18", "Variable Names": "CLM_UTLZTN_DAY_CNT", "Labels": "DESYNPUF: Claim Utilization Day Count"},
        {"File Source": "Inpatient Claims", "#": "19", "Variable Names": "NCH_BENE_DSCHRG_DT", "Labels": "DESYNPUF: Inpatient discharged date"},
        {"File Source": "Inpatient Claims", "#": "20", "Variable Names": "CLM_DRG_CD", "Labels": "DESYNPUF: Claim Diagnosis Related Group Code"},
        {"File Source": "Inpatient Claims", "#": "21-30", "Variable Names": "ICD9_DGNS_CD_1 – ICD9_DGNS_CD_10", "Labels": "DESYNPUF: Claim Diagnosis Code 1 - Claim Diagnosis Code 10"},
        {"File Source": "Inpatient Claims", "#": "31-36", "Variable Names": "ICD9_PRCDR_CD_1 – ICD9_PRCDR_CD_6", "Labels": "DESYNPUF: Claim Procedure Code 1 - Claim Procedure Code 6"},
        {"File Source": "Inpatient Claims", "#": "37-81", "Variable Names": "HCPCS_CD_1 – HCPCS_CD_45", "Labels": "DESYNPUF: Revenue Center HCFA Common Procedure Coding System 1 - Revenue Center HCFA Common Procedure Coding System 45"},
        {"File Source": "Outpatient Claims", "#": "1", "Variable Names": "DESYNPUF_ID", "Labels": "DESYNPUF: Beneficiary Code"},
        {"File Source": "Outpatient Claims", "#": "2", "Variable Names": "CLM_ID", "Labels": "DESYNPUF: Claim ID"},
        {"File Source": "Outpatient Claims", "#": "3", "Variable Names": "SEGMENT", "Labels": "DESYNPUF: Claim Line Segment"},
        {"File Source": "Outpatient Claims", "#": "4", "Variable Names": "CLM_FROM_DT", "Labels": "DESYNPUF: Claims start date"},
        {"File Source": "Outpatient Claims", "#": "5", "Variable Names": "CLM_THRU_DT", "Labels": "DESYNPUF: Claims end date"},
        {"File Source": "Outpatient Claims", "#": "6", "Variable Names": "PRVDR_NUM", "Labels": "DESYNPUF: Provider Institution"},
        {"File Source": "Outpatient Claims", "#": "7", "Variable Names": "CLM_PMT_AMT", "Labels": "DESYNPUF: Claim Payment Amount"},
        {"File Source": "Outpatient Claims", "#": "8", "Variable Names": "NCH_PRMRY_PYR_CLM_PD_AMT", "Labels": "DESYNPUF: NCH Primary Payer Claim Paid Amount"},
        {"File Source": "Outpatient Claims", "#": "9", "Variable Names": "AT_PHYSN_NPI", "Labels": "DESYNPUF: Attending Physician - National Provider Identifier Number"},
        {"File Source": "Outpatient Claims", "#": "10", "Variable Names": "OP_PHYSN_NPI", "Labels": "DESYNPUF: Operating Physician - National Provider Identifier Number"},
        {"File Source": "Outpatient Claims", "#": "11", "Variable Names": "OT_PHYSN_NPI", "Labels": "DESYNPUF: Other Physician - National Provider Identifier Number"},
        {"File Source": "Outpatient Claims", "#": "12", "Variable Names": "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM", "Labels": "DESYNPUF: NCH Beneficiary Blood Deductible Liability Amount"},
        {"File Source": "Outpatient Claims", "#": "13-22", "Variable Names": "ICD9_DGNS_CD_1 – ICD9_DGNS_CD_10", "Labels": "DESYNPUF: Claim Diagnosis Code 1 - Claim Diagnosis Code 10"},
        {"File Source": "Outpatient Claims", "#": "23-28", "Variable Names": "ICD9_PRCDR_CD_1 – ICD9_PRCDR_CD_6", "Labels": "DESYNPUF: Claim Procedure Code 1 - Claim Procedure Code 6"},
        {"File Source": "Outpatient Claims", "#": "29", "Variable Names": "NCH_BENE_PTB_DDCTBL_AMT", "Labels": "DESYNPUF: NCH Beneficiary Part B Deductible Amount"},
        {"File Source": "Outpatient Claims", "#": "30", "Variable Names": "NCH_BENE_PTB_COINSRNC_AMT", "Labels": "DESYNPUF: NCH Beneficiary Part B Coinsurance Amount"},
        {"File Source": "Outpatient Claims", "#": "31", "Variable Names": "ADMTNG_ICD9_DGNS_CD", "Labels": "DESYNPUF: Claim Admitting Diagnosis Code"},
        {"File Source": "Outpatient Claims", "#": "32-76", "Variable Names": "HCPCS_CD_1 – HCPCS_CD_45", "Labels": "DESYNPUF: Revenue Center HCFA Common Procedure Coding System 1 - Revenue Center HCFA Common Procedure Coding System 45"},
        {"File Source": "Carrier Claims", "#": "1", "Variable Names": "DESYNPUF_ID", "Labels": "DESYNPUF: Beneficiary Code"},
        {"File Source": "Carrier Claims", "#": "2", "Variable Names": "CLM_ID", "Labels": "DESYNPUF: Claim ID"},
        {"File Source": "Carrier Claims", "#": "3", "Variable Names": "CLM_FROM_DT", "Labels": "DESYNPUF: Claims start date"},
        {"File Source": "Carrier Claims", "#": "4", "Variable Names": "CLM_THRU_DT", "Labels": "DESYNPUF: Claims end date"},
        {"File Source": "Carrier Claims", "#": "5-12", "Variable Names": "ICD9_DGNS_CD_1 – ICD9_DGNS_CD_8", "Labels": "DESYNPUF: Claim Diagnosis Code 1 - Claim Diagnosis Code 8"},
        {"File Source": "Carrier Claims", "#": "13-25", "Variable Names": "PRF_PHYSN_NPI_1 - PRF_PHYSN_NPI_13", "Labels": "DESYNPUF: Provider Physician - National Provider Identifier Number"},
        {"File Source": "Carrier Claims", "#": "26-38", "Variable Names": "TAX_NUM_1 – TAX_NUM_13", "Labels": "DESYNPUF: Provider Institution Tax Number"},
        {"File Source": "Carrier Claims", "#": "39-51", "Variable Names": "HCPCS_CD_1 – HCPCS_CD_13", "Labels": "DESYNPUF: Line HCFA Common Procedure Coding System 1 - Line HCFA Common Procedure Coding System 13"},
        {"File Source": "Carrier Claims", "#": "52-64", "Variable Names": "LINE_NCH_PMT_AMT_1 - LINE_NCH_PMT_AMT_13", "Labels": "DESYNPUF: Line NCH Payment Amount 1 - Line NCH Payment Amount 13"},
        {"File Source": "Carrier Claims", "#": "65-77", "Variable Names": "LINE_BENE_PTB_DDCTBL_AMT_1 – LINE_BENE_PTB_DDCTBL_AMT_13", "Labels": "DESYNPUF: Line Beneficiary Part B Deductible Amount 1 - Line Beneficiary Part B Deductible Amount 13"},
        {"File Source": "Carrier Claims", "#": "78-90", "Variable Names": "LINE_BENE_PRMRY_PYR_PD_AMT_1 - LINE_BENE_PRMRY_PYR_PD_AMT_13", "Labels": "DESYNPUF: Line Beneficiary Primary Payer Paid Amount 1 - Line Beneficiary Primary Payer Paid Amount 13"},
        {"File Source": "Carrier Claims", "#": "91-103", "Variable Names": "LINE_COINSRNC_AMT_1 – LINE_COINSRNC_AMT_13", "Labels": "DESYNPUF: Line Coinsurance Amount 1 - Line Coinsurance Amount 13"},
        {"File Source": "Carrier Claims", "#": "104-116", "Variable Names": "LINE_ALOWD_CHRG_AMT_1 - LINE_ALOWD_CHRG_AMT_13", "Labels": "DESYNPUF: Line Allowed Charge Amount 1 - Line Allowed Charge Amount 13"},
        {"File Source": "Carrier Claims", "#": "117-129", "Variable Names": "LINE_PRCSG_IND_CD_1 - LINE_PRCSG_IND_CD_13", "Labels": "DESYNPUF: Line Processing Indicator Code 1 - Line Processing Indicator Code 13"},
        {"File Source": "Carrier Claims", "#": "130-142", "Variable Names": "LINE_ICD9_DGNS_CD_1 – LINE_ICD9_DGNS_CD_13", "Labels": "DESYNPUF: Line Diagnosis Code 1 - Line Diagnosis Code 13"},
        {"File Source": "Prescription Drug Events", "#": "1", "Variable Names": "DESYNPUF_ID", "Labels": "DESYNPUF: Beneficiary Code"},
        {"File Source": "Prescription Drug Events", "#": "2", "Variable Names": "PDE_ID", "Labels": "DESYNPUF: CCW Part D Event Number"},
        {"File Source": "Prescription Drug Events", "#": "3", "Variable Names": "SRVC_DT", "Labels": "DESYNPUF: RX Service Date"},
        {"File Source": "Prescription Drug Events", "#": "4", "Variable Names": "PROD_SRVC_ID", "Labels": "DESYNPUF: Product Service ID"},
        {"File Source": "Prescription Drug Events", "#": "5", "Variable Names": "QTY_DSPNSD_NUM", "Labels": "DESYNPUF: Quantity Dispensed"},
        {"File Source": "Prescription Drug Events", "#": "6", "Variable Names": "DAYS_SUPLY_NUM", "Labels": "DESYNPUF: Days Supply"},
        {"File Source": "Prescription Drug Events", "#": "7", "Variable Names": "PTNT_PAY_AMT", "Labels": "DESYNPUF: Patient Pay Amount"},
        {"File Source": "Prescription Drug Events", "#": "8", "Variable Names": "TOT_RX_CST_AMT", "Labels": "DESYNPUF: Gross Drug Cost"}
    ]

    try:
        records = []
        for item in labels_data:
            record = LookupVariableLabels(
                source_file=item.get('File Source'),
                original_order=item.get('#'),
                variable_name=item.get('Variable Names'),
                label=item.get('Labels')
            )
            records.append(record)
            
        session = Session(bind=engine)
        try:
            session.add_all(records)
            session.commit()
            print(f"Successfully ingested {len(records)} variable labels.")
        except Exception as e:
            session.rollback()
            print(f"Error inserting labels: {e}")
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error processing labels: {e}")

if __name__ == "__main__":
    ingest_labels()
