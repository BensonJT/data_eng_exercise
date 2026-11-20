import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Construct the database URL from environment variable
# DuckDB file path should be configured in .env file
# Note: SQLAlchemy requires 4 slashes for absolute path (duckdb:////)
DUCKDB_PATH = os.getenv('DUCKDB_PATH')

if not DUCKDB_PATH:
    raise ValueError(
        "DUCKDB_PATH environment variable is not set. "
        "Please copy .env.example to .env and configure the database path."
    )

DATABASE_URL = f"duckdb:///{DUCKDB_PATH}"

# Cap DuckDB memory and spill to disk on E: so large transforms don't run out of memory.
DUCKDB_MEMORY_LIMIT = os.getenv('DUCKDB_MEMORY_LIMIT', '4GB')
DUCKDB_TEMP_DIR = os.getenv('DUCKDB_TEMP_DIR', '/mnt/e/duckdb_tmp')
os.makedirs(DUCKDB_TEMP_DIR, exist_ok=True)

engine = create_engine(DATABASE_URL, connect_args={"config": {
    "memory_limit": DUCKDB_MEMORY_LIMIT,
    "temp_directory": DUCKDB_TEMP_DIR,
    "preserve_insertion_order": False,
}})
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
