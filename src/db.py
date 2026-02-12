import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
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

def execute_sql_script(sql_script: str) -> None:
    """
    Execute a SQL script containing one or more SQL statements.
    
    Args:
        sql_script: String containing one or more SQL statements separated by semicolons.
                   Can include CREATE TABLE, DROP, SELECT, and other SQL commands.
    
    Example:
        >>> sql = '''
        ... CREATE TABLE test AS SELECT 1 as id;
        ... SELECT * FROM test;
        ... '''
        >>> execute_sql_script(sql)
    """
    with engine.connect() as conn:
        # Split the script into individual statements
        # Remove empty statements and strip whitespace
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
        
        for statement in statements:
            # Execute each statement
            conn.execute(text(statement))
        
        # Commit all changes
        conn.commit()
