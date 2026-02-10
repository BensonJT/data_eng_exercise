import pandas as pd

def generate_report_md(bene_res, claims_res, output_path="report.md"):
    """
    Generates a Markdown report from the comparison results.
    """
    # Fetch variable labels if available
    from src.db import engine
    from sqlalchemy import text
    
    labels = {}
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT variable_name, label FROM data.lookup_variable_labels"))
            for row in result:
                labels[row.variable_name.strip()] = row.label.strip()
    except Exception:
        pass # Fallback if table doesn't exist or is empty

    def get_label(col):
        return f"{col} ({labels.get(col, '')})" if col in labels else col

    with open(output_path, "w") as f:
        f.write("# Data Comparison Report\n\n")
        f.write("## Executive Summary\n")
        f.write("This report compares the legacy healthcare claims processing system (Source) with the new system (New). ")
        f.write("The goal is to identify discrepancies and data quality issues.\n\n")
        
        f.write("## 1. Beneficiary Summary Analysis\n")
        f.write(f"- **Missing Beneficiaries in New System**: {bene_res['missing_count']}\n")
        f.write(f"- **Unexpected Extra Beneficiaries in New System**: {bene_res['extra_count']}\n")
        f.write(f"- **Beneficiaries with Attribute Mismatches**: {bene_res['mismatch_count']}\n\n")
        
        if bene_res['mismatch_count'] > 0:
            f.write("### Sample Attribute Mismatches\n")
            # Rename columns in sample for clarity using lookups
            sample = bene_res['mismatch_sample'].copy()
            # Try to map columns if possible, but the sample has specific aliased columns (src_dob, etc.)
            # We can just explain the columns used in comparison:
            f.write("Columns compared:\n")
            f.write(f"- {get_label('BENE_BIRTH_DT')}\n")
            f.write(f"- {get_label('BENE_SEX_IDENT_CD')}\n")
            f.write(f"- {get_label('BENE_ESRD_IND')}\n\n")
            
            f.write(sample.to_markdown(index=False))
            f.write("\n\n")
            
        f.write("## 2. Carrier Claims Analysis\n")
        f.write(f"- **Missing Claims in New System**: {claims_res['missing_claims_count']}\n")
        f.write(f"- **Claims with Payment Discrepancies**: {claims_res['payment_mismatch_count']}\n\n")
        
        if claims_res['payment_mismatch_count'] > 0:
            f.write("### Sample Payment Mismatches\n")
            f.write(claims_res['payment_mismatch_sample'].to_markdown(index=False))
            f.write("\n\n")
            
        f.write("## 3. Conclusion\n")
        f.write("The comparison highlights several areas of concern. ")
        if bene_res['mismatch_count'] > 0 or claims_res['payment_mismatch_count'] > 0:
            f.write("There are significant data mismatches that need to be addressed before decommissioning the old system.")
        else:
            f.write("The systems appear to be largely in sync, though further testing is recommended.")
            
    print(f"Report generated at {output_path}")

if __name__ == "__main__":
    # Dummy data for testing
    generate_report_md(
        {"missing_count": 5, "extra_count": 2, "mismatch_count": 3, "mismatch_sample": pd.DataFrame({"id": [1], "col": ["val"]})},
        {"missing_claims_count": 10, "payment_mismatch_count": 4, "payment_mismatch_sample": pd.DataFrame({"id": [1], "diff": [10.0]})}
    )
