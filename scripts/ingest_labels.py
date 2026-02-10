import sys
import os
import pandas as pd
from sqlalchemy.orm import Session

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine
from src.models import Base, LookupVariableLabels

def ingest_labels():
    print("Ingesting variable labels...")
    
    # Ensure table exists (drop first to handle schema change)
    LookupVariableLabels.__table__.drop(engine, checkfirst=True)
    Base.metadata.create_all(bind=engine)
    
    csv_path = "/mnt/e/Data Eng Exercise/lookup_variable_labels.csv"
    if not os.path.exists(csv_path):
        print(f"Error: File not found at {csv_path}")
        return

    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Renaissance mapping/cleaning
        # Expected columns: 'File Source', '#', 'Variable Names', 'Labels'
        # Normalize headers if needed (strip whitespace)
        df.columns = [c.strip() for c in df.columns]
        
        records = []
        for _, row in df.iterrows():
            record = LookupVariableLabels(
                source_file=row.get('File Source'),
                original_order=row.get('#'),
                variable_name=row.get('Variable Names'),
                label=row.get('Labels')
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
        print(f"Error reading CSV: {e}")

if __name__ == "__main__":
    ingest_labels()
