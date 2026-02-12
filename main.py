import argparse
import sys
import os
import logging

# Add the project root to the python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scripts.create_tables import create_tables
from src.ingest import run_ingestion
from src.transform import main as run_transform
from src.compare import run_comparison, compare_beneficiaries, compare_claims, calc_six_sigma, calc_financial_impact
from src.report import generate_report_md
from scripts.validate_ingestion import validate as run_validation

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Data Engineer Take-Home Assessment Pipeline")
    parser.add_argument("--init-db", action="store_true", help="Initialize the database tables")
    parser.add_argument("--ingest", action="store_true", help="Run data ingestion")
    parser.add_argument("--validate", action="store_true", help="Validate ingestion (run after --ingest, before transformation)")
    parser.add_argument("--transform", action="store_true", help="Run data transformation (run after --ingest, before comparison)")
    parser.add_argument("--compare", action="store_true", help="Run data comparison")
    parser.add_argument("--report", action="store_true", help="Generate report")
    parser.add_argument("--all", action="store_true", help="Run full pipeline (init, ingest, validate, transform, compare, report)")
    
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
        logger.info("✅ Ingestion complete")

    if args.all or args.validate:
        logger.info("Validating data ingestion...")
        run_validation()
        logger.info("✅ Validation complete")
        
    # Only run transformation if we've completed ingestion (and optionally validation)
    if args.all or (not args.validate and args.ingest) 
        or (args.validate and not args.ingest)
        or (args.transform):
        # Automatically run transformation after ingestion
        logger.info("Running data transformation (creating views and audit tables)...")
        run_transform()
        logger.info("✅ Transformation complete")

    if args.all or args.compare or args.report:
        logger.info("Running comparison...")
        run_comparison()
        logger.info("✅ Comparison complete")
        
        six_sigma_res = calc_six_sigma()
        print("\n--- Six Sigma Analysis ---")
        print(f"Six Sigma: {six_sigma_res}")
        print(f"Six Sigma Carrier: {six_sigma_res['six_sigma_carrier']}")
        print(f"Six Sigma Beneficiary: {six_sigma_res['six_sigma_beneficiary']}")
        logger.info("✅ Six Sigma complete")

        financial_impact_res = calc_financial_impact()
        print("\n--- Financial Impact Analysis ---")
        print(f"Financial Impact Claim: {financial_impact_res['financial_impact_claim']}")
        print(f"Financial Impact Beneficiary: {financial_impact_res['financial_impact_beneficiary']}")
        logger.info("✅ Financial Impact complete")

        print("\n--- Beneficiary Comparison ---")
        bene_res = compare_beneficiaries()
        print(f"Records with Discrepancies: {bene_res['bene_records_with_discrepancies']}")
        print(f"Missing Beneficiaries in New: {bene_res['missing_count']}")
        print(f"Extra Beneficiaries in New: {bene_res['extra_sample']}")
        print(f"Mismatched Beneficiary Attributes: {bene_res['mismatch_count']}")
        print(f"Beneficiary Date Differences: {bene_res['date_diff_count']}")
        print(f"Comprehensive Beneficiary Differences: {bene_res['comprehensive_beneficiary_count']}")
        print(f"Missing Beneficiary Sample: {bene_res['missing_sample']}")
        print(f"Beneficiary Date Changes: {bene_res['bene_date_changes']}")
        print(f"Mismatch Sample: {bene_res['mismatch_sample']}")
        print(f"Comprehensive Beneficiary Sample: {bene_res['comprehensive_sample']}")
        print(f"Records with Discrepancies Sample: {bene_res['bene_records_with_discrepancies_sample']}")
        logger.info("✅ Beneficiary Comparison complete")

        print("\n--- Claims Comparison ---")
        claims_res = compare_claims()
        print(f"Records with Discrepancies: {claims_res['claim_records_with_discrepancies']}")
        print(f"Missing Claims in New: {claims_res['missing_claims_count']}")
        print(f"Payment Mismatches: {claims_res['payment_mismatch_count']}")
        print(f"Compreh ensive Orphan Claims: {claims_res['comprehensive_orphan_claims_count']}")
        print(f"Payment Mismatch Sample: {claims_res['payment_mismatch_sample']}")
        print(f"Comprehensive Orphan Claims Sample: {claims_res['comprehensive_orphan_claims_sample']}")
        print(f"Records with Discrepancies Sample: {claims_res['claim_records_with_discrepancies_sample']}")
        logger.info("✅ Claims Comparison complete")
        
        if args.all or args.report:
            logger.info("Generating report...")
            generate_report_md(bene_res, claims_res, six_sigma_res, financial_impact_res)
            
            # Also generate HTML version
            logger.info("Generating HTML report...")
            import subprocess
            try:
                subprocess.run(["python3", "convert_report_to_html.py"], check=True, cwd=os.path.dirname(os.path.abspath(__file__)))
                logger.info("✅ Both reports generated: report.md and report.html")
            except Exception as e:
                logger.warning(f"HTML conversion failed: {e}. Markdown report still available.")
        
            logger.info("✅ Report complete")

if __name__ == "__main__":
    main()
