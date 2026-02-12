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
    
    # Get source data directory from environment variable
    source_data_dir =os.getenv('SOURCE_DATA_DIR')
    if not source_data_dir:
        raise ValueError(
            "SOURCE_DATA_DIR environment variable is not set. "
            "Please configure .env file."
        )
    
    json_path = os.path.join(source_data_dir, "lookup_payment_formulas.json")
    if not os.path.exists(json_path):
        print(f"Error: File not found at {json_path}")
        return

    try:
        # Read JSON
        with open(json_path, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
            
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
        print(f"Error reading JSON: {e}")

if __name__ == "__main__":
    ingest_formulas()
