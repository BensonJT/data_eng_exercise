import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Construct the database URL
# DuckDB file path on the external drive
# Note: 4 slashes for absolute path in SQLAlchemy
DATABASE_URL = "duckdb:////mnt/e/Data Eng Exercise/data_eng.duckdb"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
