import sys
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
import logging

# Load environment variables
load_dotenv()

# Add project root to python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_sum(table_name, column_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT SUM({column_name}) FROM {table_name}")).scalar()
        return result or 0

def get_csv_sum(file_paths, column_name):
    total_sum = 0
    for file_path in file_paths:
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            continue
            
        logger.info(f"Processing {os.path.basename(file_path)}...")
        # Use chunks to avoid memory issues
        for chunk in pd.read_csv(file_path, usecols=[column_name], chunksize=50000):
            total_sum += chunk[column_name].sum()
    return total_sum

def validate():
    logger.info("Starting validation...")
    
    # Get data directories from environment variables
    source_data_dir = os.getenv('SOURCE_DATA_DIR')
    new_data_dir = os.getenv('NEW_DATA_DIR')
    
    if not source_data_dir or not new_data_dir:
        raise ValueError(
            "SOURCE_DATA_DIR and NEW_DATA_DIR environment variables must be set. "
            "Please configure .env file."
        )
    
    validations = [
        {
            "table": "src_beneficiary_summary",
            "files": [
                os.path.join(source_data_dir, "DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"),
                os.path.join(source_data_dir, "DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv"),
                os.path.join(source_data_dir, "DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv")
            ],
            "column": "BENE_HI_CVRAGE_TOT_MONS",
            "desc": "Source Beneficiary Coverage Months"
        },
        {
            "table": "new_beneficiary_summary",
            "files": [
                os.path.join(new_data_dir, "DE1_0_2008_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv"),
                os.path.join(new_data_dir, "DE1_0_2009_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv"),
                os.path.join(new_data_dir, "DE1_0_2010_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv")
            ],
            "column": "BENE_HI_CVRAGE_TOT_MONS",
            "desc": "New Beneficiary Coverage Months"
        },
        {
            "table": "src_carrier_claims",
            "files": [
                os.path.join(source_data_dir, "DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv"),
                os.path.join(source_data_dir, "DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv")
            ],
             # Some CSVs might have slightly different headers, but usually consistent in this dataset
            "column": "LINE_NCH_PMT_AMT_1",
            "desc": "Source Carrier Claims Payment Amount (Line 1)"
        },
        {
            "table": "new_carrier_claims",
            "files": [
                os.path.join(new_data_dir, "DE1_0_2008_to_2010_Carrier_Claims_Sample_1A_NEWSYSTEM.csv"),
                os.path.join(new_data_dir, "DE1_0_2008_to_2010_Carrier_Claims_Sample_1B_NEWSYSTEM.csv")
            ],
            "column": "LINE_NCH_PMT_AMT_1",
            "desc": "New Carrier Claims Payment Amount (Line 1)"
        }
    ]

    for v in validations:
        logger.info(f"Validating {v['desc']}...")
        
        # CSV Sum
        csv_total = get_csv_sum(v['files'], v['column'])
        
        # DB Sum
        db_total = get_db_sum(v['table'], v['column'])
        
        diff = abs(csv_total - db_total)
        # Allow small float diff
        match = diff < 0.1
        
        logger.info(f"  CSV Sum: {csv_total:,.2f}")
        logger.info(f"  DB Sum:  {db_total:,.2f}")
        logger.info(f"  Diff:    {diff:,.2f}")
        logger.info(f"  Match:   {'✅' if match else '❌'}")
        print("-" * 50)

if __name__ == "__main__":
    validate()
