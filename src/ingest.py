import pandas as pd
import sys
import os

# Add project root to python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
from sqlalchemy.orm import Session
from src.db import engine, SessionLocal
from src.models import (
    SrcBeneficiarySummary, SrcCarrierClaims,
    NewBeneficiarySummary, NewCarrierClaims
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BATCH_SIZE = 10000

def ingest_csv(file_path, model_class, year=None, extra_cols=None):
    """
    Ingests a CSV file into the database using chunked processing.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Starting ingestion for {file_path} into {model_class.__tablename__}")
    
    try:
        # Use pandas to read in chunks
        with pd.read_csv(file_path, chunksize=BATCH_SIZE, dtype=str) as reader:
            db: Session = SessionLocal()
            try:
                total_rows = 0
                
                for chunk in reader:
                    # Basic cleaning: Replace NaN with None
                    chunk = chunk.where(pd.notnull(chunk), None)
                    
                    # Add year if provided
                    if year:
                        chunk['YEAR'] = year
                    
                    # Add any other extra columns
                    if extra_cols:
                        for col, val in extra_cols.items():
                            chunk[col] = val

                    # Convert specific columns to numeric if needed, or rely on SQLAlchemy to cast
                    columns_to_inspect = chunk.columns
                    records = chunk.to_dict(orient='records')
                    
                    # Bulk insert might be faster, but let's use add_all for simplicity with ORM
                    # ideally we use bulk_insert_mappings for performance
                    try:
                        db.bulk_insert_mappings(model_class, records)
                        db.commit()
                        total_rows += len(records)
                        logger.info(f"Ingested {total_rows} rows...")
                    except Exception as e:
                        db.rollback()
                        logger.error(f"Error inserting chunk: {e}")
                        raise e
            finally:
                db.close()
            
            logger.info(f"Finished ingestion for {file_path}. Total rows: {total_rows}")

    except Exception as e:
        logger.error(f"Failed to ingest {file_path}: {e}")

def run_ingestion():
    # Source Data Paths
    src_bene_2008 = "/mnt/e/Data Eng Exercise/source/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"
    src_bene_2009 = "/mnt/e/Data Eng Exercise/source/DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv"
    src_bene_2010 = "/mnt/e/Data Eng Exercise/source/DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv"
    
    src_claims_a = "/mnt/e/Data Eng Exercise/source/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv"
    src_claims_b = "/mnt/e/Data Eng Exercise/source/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv"

    # New System Data Paths
    new_bene_2008 = "/mnt/e/Data Eng Exercise/new/DE1_0_2008_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv"
    new_bene_2009 = "/mnt/e/Data Eng Exercise/new/DE1_0_2009_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv"
    new_bene_2010 = "/mnt/e/Data Eng Exercise/new/DE1_0_2010_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv"

    new_claims_a = "/mnt/e/Data Eng Exercise/new/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A_NEWSYSTEM.csv"
    new_claims_b = "/mnt/e/Data Eng Exercise/new/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B_NEWSYSTEM.csv"

    # Ingest Source Beneficiary Data
    ingest_csv(src_bene_2008, SrcBeneficiarySummary, year=2008)
    ingest_csv(src_bene_2009, SrcBeneficiarySummary, year=2009)
    ingest_csv(src_bene_2010, SrcBeneficiarySummary, year=2010)

    # Ingest Source Claims Data
    ingest_csv(src_claims_a, SrcCarrierClaims)
    ingest_csv(src_claims_b, SrcCarrierClaims)

    # Ingest New System Beneficiary Data
    ingest_csv(new_bene_2008, NewBeneficiarySummary, year=2008)
    ingest_csv(new_bene_2009, NewBeneficiarySummary, year=2009)
    ingest_csv(new_bene_2010, NewBeneficiarySummary, year=2010)

    # Ingest New System Claims Data
    ingest_csv(new_claims_a, NewCarrierClaims)
    ingest_csv(new_claims_b, NewCarrierClaims)

if __name__ == "__main__":
    run_ingestion()
