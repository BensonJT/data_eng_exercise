import argparse
import sys
import os
import logging

# Add the project root to the python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scripts.create_tables import create_tables
from src.ingest import run_ingestion
from src.compare import run_comparison, compare_beneficiaries, compare_claims
from src.report import generate_report_md

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Data Engineer Take-Home Assessment Pipeline")
    parser.add_argument("--init-db", action="store_true", help="Initialize the database tables")
    parser.add_argument("--ingest", action="store_true", help="Run data ingestion")
    parser.add_argument("--compare", action="store_true", help="Run data comparison")
    parser.add_argument("--report", action="store_true", help="Generate report")
    parser.add_argument("--all", action="store_true", help="Run full pipeline (init, ingest, compare, report)")
    
    args = parser.parse_args()
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    if args.all or args.init_db:
        logger.info("Initializing database...")
        create_tables()

    if args.all or args.ingest:
        logger.info("Running data ingestion...")
        run_ingestion()

    if args.all or args.compare or args.report:
        logger.info("Running comparison...")
        # We need the results for the report, so we capture them
        print("Comparing Beneficiaries...")
        bene_res = compare_beneficiaries()
        print("Comparing Claims...")
        claims_res = compare_claims()
        
        # Print summary to console
        print(f"Beneficiary Discrepancies: {bene_res['mismatch_count']}")
        print(f"Claims Payment Discrepancies: {claims_res['payment_mismatch_count']}")

        if args.all or args.report:
            logger.info("Generating report...")
            generate_report_md(bene_res, claims_res)

if __name__ == "__main__":
    main()
