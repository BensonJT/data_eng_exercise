# Data Engineer Take-Home Assessment

## Overview

This project implements a data comparison and validation tool for healthcare claims processing systems. It compares outputs from a legacy claims processing system with a new system to identify discrepancies, quantify their impact, and surface data quality issues before migration.

The tool ingests CMS synthetic public use files, performs comprehensive ETL processing, identifies and quantifies discrepancies using statistical methods (Six Sigma analysis), calculates financial impact, and generates detailed reports.

## Architecture

### Pipeline Stages

The pipeline consists of four main stages executed in sequence:

1. **Initialize Database** (`--init-db`)
   - Creates DuckDB database and table schema
   - Defines tables for source and new system data

2. **Ingest Data** (`--ingest`)
   - Loads 10 CSV files (5 source + 5 new system) into database
   - **Uses chunked streaming** (10,000 rows per batch) for memory efficiency
   - Processes files iteratively without loading entire datasets into memory
   - Transformation runs automatically after ingestion (unless using --validate)

3. **Validate Ingestion** (`--validate`, `--ingest`) - *Optional but recommended*
   - Validates data was ingested correctly by comparing database sums to CSV file sums
   - Should be run AFTER `--ingest` and BEFORE transformation
   - Checks 4 key metrics across source and new system datasets
   - Helps catch any data corruption or incomplete ingestion early

4. **Transform** (`--transform`, `--ingest`, `--validate`)
   - **Executes SQL Scripts* 
   - Creates analytical views and audit tables
   - Performs complex SQL transformations
   - Prepares data for comparison

5. **Compare & Report** (`--compare`, `--report`, `--transform`, `--ingest`, `--validate`)
   - Executes comparison logic across multiple dimensions
   - Generates 17 output CSV files with detailed analysis
   - Creates Markdown and HTML reports

### Technology Stack

- **Database**: DuckDB (lightweight, embedded analytical database)
- **ORM**: SQLAlchemy 2.0 with DuckDB driver
- **Data Processing**: pandas with chunked reading for memory efficiency
- **Statistical Analysis**: scipy for accurate Six Sigma calculations (inverse normal distribution)
- **Configuration**: python-dotenv for environment variable management
- **Python**: 3.10+

## Prerequisites

- **Python**: Version 3.10 or higher
- **Disk Space**: Minimum 5GB for data files and database
- **Memory**: Recommended 8GB RAM for processing large CSV files
- **Data Files**: 10 CSV files from CMS (see [Input Files](#input-files-required) section)

## Setup Instructions

### 1. Clone Repository

```bash
git clone <repository-url>
cd data_eng
```

### 2. Create Virtual Environment

**Using conda (recommended):**
```bash
conda create -n data_eng python=3.13
conda activate data_eng
pip install -r requirements.txt
```

**Using venv:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Download Required Data Files

Download all required files from the CMS website:
https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1

**Required Downloads:**
- DE1.0 Sample 1 2008 Beneficiary Summary File (ZIP)
- DE1.0 Sample 1 2009 Beneficiary Summary File (ZIP)
- DE1.0 Sample 1 2010 Beneficiary Summary File (ZIP)
- DE1.0 Sample 1 2008-2010 Carrier Claims 1 (CSV)
- DE1.0 Sample 1 2008-2010 Carrier Claims 2 (CSV)

Download the new system outputs from the provided link (password protected).

Extract all files and organize them into two directories:
- **source/** - Original CMS files (5 files)
- **new/** - New system outputs with `_NEWSYSTEM` suffix (5 files)

### 4. Configure Environment Variables

Copy the example environment file and customize the paths:

```bash
cp .env.example .env
```

Edit `.env` and update the following variables with **absolute paths**:

```bash
# Path where the DuckDB database will be created
DUCKDB_PATH=/path/to/your/data_eng.duckdb

# Directory containing the 5 source (legacy) system CSV files
SOURCE_DATA_DIR=/path/to/source

# Directory containing the 5 new system CSV files
NEW_DATA_DIR=/path/to/new
```

**Example Configuration:**
```bash
DUCKDB_PATH="/mnt/e/Data Eng Exercise/data_eng.duckdb"
SOURCE_DATA_DIR="/mnt/e/Data Eng Exercise/source"
NEW_DATA_DIR="/mnt/e/Data Eng Exercise/new"
```

> **Important**: Use absolute paths (not relative paths) to ensure consistency across different execution contexts.

## Input Files Required

Your `SOURCE_DATA_DIR` directory must contain these 5 files:

1. `DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv` - Beneficiary demographics and enrollment (2008)
2. `DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv` - Beneficiary demographics and enrollment (2009)
3. `DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv` - Beneficiary demographics and enrollment (2010)
4. `DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv` - Carrier claims part A (physician/outpatient)
5. `DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv` - Carrier claims part B (physician/outpatient)

Your `NEW_DATA_DIR` directory must contain these 5 files:

1. `DE1_0_2008_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv`
2. `DE1_0_2009_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv`
3. `DE1_0_2010_Beneficiary_Summary_File_Sample_1_NEWSYSTEM.csv`
4. `DE1_0_2008_to_2010_Carrier_Claims_Sample_1A_NEWSYSTEM.csv`
5. `DE1_0_2008_to_2010_Carrier_Claims_Sample_1B_NEWSYSTEM.csv`

## Usage

### Run Full Pipeline (Recommended)

Execute all stages in sequence:

```bash
python main.py --all
```

This will:
1. Initialize the database and create tables
2. Ingest all 10 CSV files
3. **Validate the ingestion** (optional but recommended)
4. **Load lookup tables** (state, sex, race codes, formulas, labels)
5. Run transformation to create analytical views
6. Execute comparison logic
7. Generate both Markdown and HTML reports

**Expected Runtime**: 6 - 8 hours depending on system performance for initial run (-- all)

### Run Individual Stages

For development or troubleshooting, you can run stages separately:

```bash
# Initialize database only
 python main.py --init-db  

# Ingest data and run transformations
python main.py --ingest

# Validate ingestion (AFTER --ingest, BEFORE transformation)
# This is optional but recommended to ensure data integrity
python main.py --validate

# Run comparison only (requires data to be ingested and transformed)
python main.py --compare

# Generate reports only
python main.py --report
```

> **Note**: The `--validate` flag should be run **after** `--ingest` but **before** any comparison or reporting. It validates that CSV data was correctly loaded into the database by comparing checksums.

### Recommended Workflow for First-Time Users

1. **Initialize and Ingest**:
   ```bash
   python main.py --init-db --ingest
   ```

2. **Validate** (recommended before proceeding):
   ```bash
   python main.py --validate
   ```
   - If validation passes, proceed to comparison
   - If validation fails, check `.env` paths and re-run `--ingest`

3. **Compare and Report**:
   ```bash
   python main.py --compare --report
   ```

## Output Files

The pipeline generates multiple output files for comprehensive analysis.

### CSV Output Files

Provides details for manual exploration of the data analysis, including deep dives. The report.md and report.html are summaries of these findings. All CSV files are written to the `data/` directory:

#### Six Sigma Quality Metrics

1. **`six_sigma.csv`** - Overall six sigma quality score for the migration
   - These metrics provide single number summaries of the quality of the migration
   - Metrics Include: Total Defects, Total Opportunities, DPMO, Process Yield, and Sigma Level
   - Total Defects is the aggregated count of all fields that do not match between the source and new systems (excludes key fields)
   - Total Opportunities are the sum total of all fields on every row (excludes key fields)
   - DPMO is Defects Per Million Opportunities:
      - DPMO = ((Total Defects / Total Opportunities) × 1,000,000)
   - Process Yield is the percentage of opportunities that are defect free 
      - Yield = (((Total Opportunities - Total Defects) / Total Opportunities) * 100)
   - Sigma Level is the number of standard deviations from the mean (3 is 6 sigma) 
      - Z-Score = Φ⁻¹(Yield)  # Inverse Normal CDF
         - Where: Φ⁻¹ (Phi inverse) = Inverse of the Standard Normal Cumulative Distribution Function
         - And 1.5 sigma shift = Standard industry adjustment for long-term process drift
      - Sigma Level = Z-score + 1.5

2. **`six_sigma_carrier.csv`** - Field-level six sigma analysis for carrier claims
   - Shows quality metrics for each claim field grouped by field family
   - Identifies which field groups have the most errors

3. **`six_sigma_beneficiary.csv`** - Field-level six sigma analysis for beneficiaries
   - Shows quality metrics for each beneficiary field grouped by field family
   - Identifies which field groups have the most errors
   - Identifies demographic/enrollment data quality issues

4. **`six_sigma_columns.csv`** - Field-level six sigma analysis for all fields (not grouped)
   - Shows quality metrics for each field
   - Identifies which specific fields have the most errors

#### Financial Impact Analysis

5. **`financial_impact_carrier_claim.csv`** - Financial impact of claim discrepancies
   - Quantifies dollar impact of payment mismatches
   - Aggregates total financial exposure
   - Financial impact represents the absolute value of the sum of variances (new - source)

6. **`financial_impact_beneficiary.csv`** - Financial impact of beneficiary errors
   - Estimates cost impact of beneficiary data errors 
   - Helps prioritize remediation efforts
   - Financial impact represents the absolute value of the sum of variances (new - source)

#### Beneficiary Discrepancies

7. **`missing_beneficiaries.csv`** - Beneficiaries present in source but missing in new system
   - Critical errors: data loss in migration (expected to be empty)

8. **`extra_beneficiaries.csv`** - Beneficiaries in new system that weren't in source
   - Unexpected additions (expected to be empty)

9. **`beneficiary_attribute_mismatches.csv`** - Beneficiary attributes that changed unexpectedly
   - Demographics, dates, or other fields that differ (expected to be empty)

10. **`beneficiary_date_differences.csv`** - Date field discrepancies for beneficiaries
   - Birth dates, death dates, enrollment dates that don't match (expected to be empty)

11. **`comprehensive_beneficiary_differences.csv`** - All beneficiaries with any discrepancy in any field
    - Complete list for detailed investigation (expected to be empty)

12. **`audit_beneficiary_summary.csv`** - Beneficiary audit summary
    - Provides a detailed file to browse to see exactly which fields did not match between the source and new systems (expected to have 0s in all fields)
    - Values of 0 mean the fields matched, values of 1 mean the fields did not match
    - Rows with at least one 1 are included (rows that matched are excluded)
    - The total number of discrepancies is the sum of all values in the file, this is used to produce the total_defect analysis for Six Sigma Calculations.

#### Claims Discrepancies

13. **`missing_claims.csv`** - Claims present in source but missing in new system
    - Data loss in migration (critical)

14. **`claim_payment_amount_discrepancies.csv`** - Payment amount differences
    - Financial discrepancies in claim line payments
    - Largest file - typically thousands of records

15. **`comprehensive_orphan_claims.csv`** - Orphaned claims in new system not present in source system
    - Claims that don't match on 4-way key (ID, Claim ID, From Date, Through Date)
    - Indicates  data integrity issues (these keys are not in the source system)

16. **`audit_claim_summary.csv`** - Claim audit summary
    - Provides a detailed file to browse to see exactly which fields did not match between the source and new systems (expected to have 0s in all fields)
    - Values of 0 mean the fields matched, values of 1 mean the fields did not match
    - Rows with at least one 1 are included (rows that matched are excluded)
    - The total number of discrepancies is the sum of all values in the file, this is used to produce the total_defect analysis for Six Sigma Calculations.

#### Database Schema

17. **`schema.csv`** - Database schema
    - Shows table and column names for each table and view in the duckdb database
    - Used to showcase the architecture of this automated data migration analysis

### Report Files

#### `report.md` - Comprehensive Markdown Report

Detailed analysis report including:
- **Executive Summary**: High-level findings and recommendations
- **Data Ingestion Summary**: Row counts and completeness checks
- **Discrepancy Analysis**: Detailed breakdown by category
- **Six Sigma Quality Metrics**: Statistical quality assessment
- **Financial Impact**: Dollar amounts at risk due to discrepancies
- **Trend Analysis**: Patterns in errors and discrepancies
- **Recommendations**: Prioritized remediation actions

#### `report.html` - HTML Version of Report

- Converted from Markdown for easy viewing in web browsers
- Includes styling and formatting for better readability
- Can be shared with stakeholders via email or web hosting

## Project Structure

```
data_eng/
├── .env                      # Your local configuration (not in git)
├── .env.example              # Template for configuration
├── main.py                   # Pipeline entry point
├── convert_report_to_html.py # Convert Markdown report to HTML
├── requirements.txt          # Python dependencies
├── README.md                 # This file
├── FEEDBACK.md               # Assignment feedback
│
├── src/                      # Main application code
│   ├── db.py                 # Database connection and utilities
│   ├── models.py             # SQLAlchemy ORM models
│   ├── ingest.py             # CSV ingestion logic
│   ├── transform.py          # SQL transformation views
│   ├── compare.py            # Comparison logic and metrics
│   └── report.py             # Report generation
│
├── scripts/                  # Helper scripts
│   ├── create_tables.py      # Database initialization
│   ├── validate_ingestion.py # Data validation utilities
│   └── ...                   # Additional utility scripts
│
├── data/                     # Output directory (created at runtime)
│   ├── *.csv                 # 17 output CSV files (for deeper dive analysis)
│   └── ...
│
├── report.md                 # Generated Markdown report
└── report.html               # Generated HTML report (better formatting)
```

## Troubleshooting

### Configuration Errors

**Error: `DUCKDB_PATH environment variable is not set`**
- **Solution**: Copy `.env.example` to `.env` and configure the database path

**Error: `SOURCE_DATA_DIR environment variable is not set`**
- **Solution**: Set `SOURCE_DATA_DIR` in your `.env` file to the directory containing source CSV files

**Error: `Source data directory not found: /path/to/dir`**
- **Solution**: Verify the path exists and is an absolute path (not relative)

### File Errors

**Error: `File not found: /path/to/file.csv`**
- **Solution**: Ensure all 10 CSV files are present in the configured directories
- Check that filenames match exactly (case-sensitive)

### Memory Issues

**Error: Process killed or out of memory**
- **Solution**: The pipeline uses chunked processing, but very large files may still require significant RAM
- Close other applications to free up memory
- Consider increasing BATCH_SIZE in `src/ingest.py` if you have more RAM available

### Database Errors

**Error: Database locked or connection issues**
- **Solution**: Ensure no other process is accessing the DuckDB file
- Delete the database file and re-run `--init-db` if corruption is suspected

### Import Errors

**Error: `ModuleNotFoundError`**
- **Solution**: Ensure virtual environment is activated and dependencies are installed:
  ```bash
  pip install -r requirements.txt
  ```

## Performance Notes

- **Ingestion**: Processes ~10M rows total across all files
- **Transformation**: Creates 50+ analytical views and audit tables
- **Comparison**: Executes complex (advanced) SQL joins and aggregations
- **Expected Total Runtime**: 6 - 8 hours on average personal computers (depends on system performance)

## Additional Resources

- [CMS DE-SynPUF Documentation](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

## Support

For questions or issues:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review log output for specific error messages
3. Verify `.env` configuration matches expected format
4. Ensure all 10 input CSV files are present and readable
