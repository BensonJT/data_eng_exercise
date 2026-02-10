# Data Engineer Take-Home Assessment

## Overview
This project is a data comparison and validation tool designed to compare healthcare claims processing outcomes between a legacy system and a new system. It ingests large CSV datasets into a PostgreSQL database, identifies discrepancies, and generates a summary report.

## Project Structure
- `src/`: Source code modules.
    - `models.py`: SQLAlchemy models defining the database schema.
    - `ingest.py`: Data ingestion logic using pandas chunking.
    - `compare.py`: SQL-based comparison logic.
    - `report.py`: Markdown report generator.
- `scripts/`: Helper scripts.
- `data/`: Directory for data storage (ignored in git).
- `main.py`: Entry point for the pipeline.

## Setup

1. **Environment**:
   Ensure you have Python 3.10+ installed.
   ```bash
   # Using pip
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   
   # Using conda
   conda create -n data_eng python=3.13
   conda activate data_eng
   pip install -r requirements.txt
   ```

2. **Database**:
   Ensure PostgreSQL is running. Configure connection details in `.env` (copy from `.env.example`).
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

## Usage

Run the full pipeline (Initialize DB, Ingest, Compare, Report):
```bash
python main.py --all
```

Run specific steps:
```bash
python main.py --init-db   # Create tables
python main.py --ingest    # Ingest data
python main.py --compare   # Run comparison logic
python main.py --report    # Generate report
```

## Output
The final report will be generated as `report.md` in the project root.
