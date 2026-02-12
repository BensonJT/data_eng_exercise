import pandas as pd
import os

def html_table_header(headers, col_widths=None):
    """Generate HTML table header with styling"""
    html = '<table style="border-collapse: collapse; width: 100%; margin: 20px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">\n'
    html += '  <thead style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">\n'
    html += '    <tr>\n'
    for i, header in enumerate(headers):
        width = f' width="{col_widths[i]}"' if col_widths and i < len(col_widths) else ''
        html += f'      <th style="padding: 14px; text-align: left; font-weight: 600; border-bottom: 3px solid #4a5568;"{width}>{header}</th>\n'
    html += '    </tr>\n'
    html += '  </thead>\n'
    html += '  <tbody>\n'
    return html

def html_table_row(cells, row_num, status_col=None):
    """Generate HTML table row with alternating colors and status highlighting"""
    bg_color = '#f7fafc' if row_num % 2 == 0 else '#ffffff'
    html = f'    <tr style="background-color: {bg_color};">\n'
    
    for i, cell in enumerate(cells):
        # Apply status-specific styling
        cell_style = f'padding: 12px; border-bottom: 1px solid #e2e8f0;'
        
    if status_col is not None and i == status_col:
        # Color-code status column
        if '🏆' in str(cell) or 'World-class' in str(cell):
            cell_style += ' background-color: #ffda3a; color: black; font-weight: bold; text-align: center;'  # gold
        elif '✅' in str(cell) or 'Excellent' in str(cell):
            cell_style += ' background-color: #48bb78; color: white; font-weight: bold; text-align: center;'  # green
        elif '☑️' in str(cell) or 'Good' in str(cell):
            cell_style += ' background-color: #68d391; color: white; font-weight: bold; text-align: center;'  # lighter green
        elif '⚠️' in str(cell) or 'Acceptable' in str(cell) or 'Average' in str(cell) or 'Review' in str(cell):
            cell_style += ' background-color: #ed8936; color: white; font-weight: bold; text-align: center;'  # orange
        elif '❌' in str(cell) or 'Critical' in str(cell) or 'Needs Attention' in str(cell):
            cell_style += ' background-color: #f56565; color: white; font-weight: bold; text-align: center;'  # red
        
        # Right-align numbers
        if isinstance(cell, (int, float)) or (isinstance(cell, str) and any(c.isdigit() for c in cell) and '$' not in cell):
            cell_style += ' text-align: right;'
        
        html += f'      <td style="{cell_style}">{cell}</td>\n'
    
    html += '    </tr>\n'
    return html

def html_table_footer():
    """Close HTML table"""
    return '  </tbody>\n</table>\n\n'

def generate_report_md(bene_res, claims_res, six_sigma_res, financial_impact_res, output_path="report.md"):
    """
    Generates a comprehensive Markdown report from the comparison results.
    
    Args:
        bene_res: Beneficiary comparison results dictionary
        claims_res: Claims comparison results dictionary
        six_sigma_res: Six Sigma analysis results dictionary
        financial_impact_res: Financial impact analysis results dictionary
        output_path: Path to output markdown file
    """
    # Get project root for relative CSV paths
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    with open(output_path, "w") as f:
        f.write("# Data Migration Quality Assessment Report\n\n")
        
        # Executive Summary
        f.write("## Executive Summary\n\n")
        f.write("This report evaluates the quality of the healthcare claims data migration from the legacy ")
        f.write("system (Source) to the new system (New). The analysis employs Six Sigma methodology to ")
        f.write("quantify data quality and identifies areas requiring remediation.\n\n")
        
        # Migration Scorecard
        f.write("### Migration Scorecard\n\n")
        f.write("| Metric | Value | Status |\n")
        f.write("|--------|-------|--------|\n")
        
        # Extract Six Sigma overall metrics
        six_sigma_overall = six_sigma_res['six_sigma']
        for _, row in six_sigma_overall.iterrows():
            subject = row['SUBJECT']
            sigma = row['SIGMA_LEVEL']
            dpmo = int(row['DPMO'])
            yield_pct = row['YIELD'] * 100
            
            # Determine status emoji
            if sigma >= 6.0:
                status = "🏆 World-class"
            elif sigma >= 5.0:
                status = "✅ Excellent"
            elif sigma >= 4.0:
                status = "☑️ Good"
            elif sigma >= 3.0:
                status = "⚠️ Acceptable"
            else:
                status = "❌ Needs Attention"
            
            f.write(f"| {subject} Quality | {sigma:.2f}σ ({dpmo:,} DPMO, {yield_pct:.2f}% yield) | {status} |\n")
        
        f.write(f"| Missing Beneficiaries | {bene_res['missing_count']:,} | {'❌ Critical' if bene_res['missing_count'] > 100 else '⚠️ Review'} |\n")
        f.write(f"| Beneficiary Records with Defects | {bene_res['bene_records_with_discrepancies']:,} | {'❌ Critical' if bene_res['bene_records_with_discrepancies'] > 1000 else '⚠️ Review'} |\n")
        f.write(f"| Missing Claims | {claims_res['missing_claims_count']:,} | {'❌ Critical' if claims_res['missing_claims_count'] > 1000 else '⚠️ Review'} |\n")
        f.write(f"| Claim Records with Defects | {claims_res['claim_records_with_discrepancies']:,} | {'❌ Critical' if claims_res['claim_records_with_discrepancies'] > 1000 else '⚠️ Review'} |\n")
        f.write(f"| Payment Discrepancies | {claims_res['payment_mismatch_count']:,} | {'❌ Critical' if claims_res['payment_mismatch_count'] > 10000 else '⚠️ Review'} |\n")
        f.write("\n")
        
        # Six Sigma Quality Analysis Section
        f.write("---\n\n")
        f.write("## 1. Six Sigma Quality Analysis\n\n")
        f.write("**Six Sigma** measures data quality in defects per million opportunities (DPMO). ")
        f.write("Higher sigma levels indicate better quality:\n")
        f.write("- **6σ**: 3.4 DPMO (99.9997% yield) - World-class\n")
        f.write("- **5σ**: 233 DPMO (99.98% yield) - Excellent\n")
        f.write("- **4σ**: 6,210 DPMO (99.38% yield) - Good\n")
        f.write("- **3σ**: 66,807 DPMO (93.32% yield) - Acceptable\n")
        f.write("- **2σ**: 308,538 DPMO (69.15% yield) - Poor\n\n")
        
        f.write("### Overall Migration Quality\n\n")
        f.write("| Dataset | Total Units | Total Defects | Sigma Level | DPMO | Yield % |\n")
        f.write("|---------|-------------|---------------|-------------|------|----------|\n")
        for _, row in six_sigma_overall.iterrows():
            f.write(f"| {row['SUBJECT']} | {int(row['TOTAL_UNITS']):,} | {int(row['TOTAL_DEFECTS']):,} | ")
            f.write(f"{row['SIGMA_LEVEL']:.2f}σ | {int(row['DPMO']):,} | {row['YIELD']*100:.2f}% |\n")
        f.write("\n")
        
        f.write("📊 **Detailed Analysis**: \n\n")
        f.write("**[six_sigma.csv](data/six_sigma.csv)** - Overall quality metrics\n")
        f.write("- Contains: SUBJECT, TOTAL_UNITS, TOTAL_DEFECTS, TOTAL_OPPORTUNITIES, DPMO, YIELD, SIGMA_LEVEL\n")
        f.write("- Use this to: Track overall migration quality score and identify which dataset (Carrier Claims vs Beneficiary) needs priority attention\n\n")
        f.write("- Defects in the beneficiary dataset may drive defects in the carrier claims dataset\n\n")
        
        f.write("**[six_sigma_carrier.csv](data/six_sigma_carrier.csv)** - Field-level carrier claim defects (102 fields analyzed)\n")
        f.write("- Contains: SOURCE, FIELD_FAMILY, TOTAL_DEFECTS, TOTAL_OPPORTUNITIES, DPMO, SIGMA_LEVEL\n")
        f.write("- Use this to: Identify which specific carrier claim fields have the highest error rates\n")
        f.write("- Fields analyzed include: ICD9 diagnosis codes, NPI identifiers, tax numbers, HCPCS codes, payment amounts, deductibles, coinsurance, processing indicators\n")
        f.write("- To keep the initial data set higher level, we aggregated the six sigma scores for all fields in a field group together\n")
        f.write("- CSV files are provided that give the detailed scores per field (without grouping them)\n")
        f.write("- Sort by TOTAL_DEFECTS descending to prioritize remediation efforts\n\n")
        
        f.write("**[six_sigma_beneficiary.csv](data/six_sigma_beneficiary.csv)** - Field-level beneficiary defects (31 fields analyzed)\n")
        f.write("- Contains: SOURCE, FIELD_FAMILY, TOTAL_DEFECTS, TOTAL_OPPORTUNITIES, DPMO, SIGMA_LEVEL\n")
        f.write("- Use this to: Identify which beneficiary attributes have the highest error rates\n")
        f.write("- Fields analyzed include: Demographics (birth/death dates, sex, race), coverage months, chronic conditions (11 conditions), financial fields (reimbursements, benefits, payments)\n")
        f.write("- To keep the initial data set higher level, we aggregated the six sigma scores for all fields in a field group together\n")
        f.write("- CSV files are provided that give the detailed scores per field (without grouping them)\n")
        f.write("- Focus on fields with DPMO > 100,000 (below 3σ) for immediate remediation\n\n")
        
        f.write("**[six_sigma_columns.csv](data/six_sigma_columns.csv)** - Field-level carrier claim defects (all fields analyzed except keys)\n")
        f.write("- Contains: SOURCE, FIELD_FAMILY, TOTAL_DEFECTS, TOTAL_OPPORTUNITIES, DPMO, SIGMA_LEVEL\n")
        f.write("- Use this to: Identify which specific fields have the highest error rates\n")
        f.write("- This is the specific CSV that is used to generate the six sigma scores for each field (without grouping them)\n")
        f.write("- Sort by TOTAL_DEFECTS descending to prioritize remediation efforts, filter on SOURCE to focus on carrier claims or beneficiary\n\n")
        
        # Top defect fields
        f.write("### Top Defect Fields\n\n")
        
        # Carrier Claims top defects
        carrier_sigma = six_sigma_res['six_sigma_carrier'].nlargest(5, 'TOTAL_DEFECTS')
        f.write("**Carrier Claims - Highest Error Fields:**\n\n")
        f.write("| Field Family | Total Defects | DPMO | Sigma Level |\n")
        f.write("|--------------|---------------|------|-------------|\n")
        for _, row in carrier_sigma.iterrows():
            f.write(f"| {row['FIELD_FAMILY']} | {int(row['TOTAL_DEFECTS']):,} | {int(row['DPMO']):,} | {row['SIGMA_LEVEL']:.2f}σ |\n")
        f.write("\n")
        
        # Beneficiary top defects
        bene_sigma = six_sigma_res['six_sigma_beneficiary'].nlargest(5, 'TOTAL_DEFECTS')
        f.write("**Beneficiary Summary - Highest Error Fields:**\n\n")
        f.write("| Field Family | Total Defects | DPMO | Sigma Level |\n")
        f.write("|--------------|---------------|------|-------------|\n")
        for _, row in bene_sigma.iterrows():
            f.write(f"| {row['FIELD_FAMILY']} | {int(row['TOTAL_DEFECTS']):,} | {int(row['DPMO']):,} | {row['SIGMA_LEVEL']:.2f}σ |\n")
        f.write("\n")
        
        # Financial Impact Section
        f.write("---\n\n")
        f.write("## 2. Financial Impact Analysis\n\n")
        f.write("**Pareto Analysis** identifies which field families contribute most to financial errors. ")
        f.write("The 80/20 rule typically applies: ~80% of financial variance comes from ~20% of fields.\n\n")
        
        # Carrier Claims Financial Impact
        claim_financial = financial_impact_res['financial_impact_claim']
        f.write("### Carrier Claims Financial Variance\n\n")
        f.write("| Field Family | Financial Variance | % of Total | Cumulative % |\n")
        f.write("|--------------|-------------------|------------|---------------|\n")
        total_variance = 0
        for _, row in claim_financial.iterrows():
            variance = row['FINANCIAL_VARIANCE']
            pct = row['RUNNING_PCT_OF_TOTAL'] * 100
            total_variance += variance
            f.write(f"| {row['FIELD_FAMILY']} | ${variance:,.2f} | {(variance/claim_financial['FINANCIAL_VARIANCE'].sum())*100:.1f}% | {pct:.1f}% |\n")
        f.write(f"| **TOTAL** | **${total_variance:,.2f}** | **100%** | - |\n")
        f.write("\n📊 **Detailed Data**: **[financial_impact_carrier_claim.csv](data/financial_impact_carrier_claim.csv)**\n\n")
        f.write("**CSV Contents:**\n")
        f.write("- Columns: DATA_SET, FIELD_FAMILY, FINANCIAL_VARIANCE, RUNNING_PCT_OF_TOTAL\n")
        f.write("- Contains absolute dollar variance aggregated by field family\n")
        f.write("- Pre-sorted by financial impact (highest to lowest)\n\n")
        f.write("**How to Use:**\n")
        f.write("- Identify the 20% of fields causing 80% of financial variance (Pareto principle)\n")
        f.write("- Focus remediation on field families with RUNNING_PCT_OF_TOTAL < 0.8 (first ~80% of errors)\n")
        f.write("- Use this to justify resource allocation for data quality improvement efforts\n")
        f.write("- Cross-reference with six_sigma_carrier.csv to find fields that are both high-defect AND high-cost\n\n")
        
        # Beneficiary Financial Impact
        bene_financial = financial_impact_res['financial_impact_beneficiary']
        f.write("### Beneficiary Summary Financial Variance\n\n")
        f.write("| Field Family | Financial Variance | % of Total | Cumulative % |\n")
        f.write("|--------------|-------------------|------------|---------------|\n")
        total_variance = 0
        for _, row in bene_financial.iterrows():
            variance = row['FINANCIAL_VARIANCE']
            pct = row['RUNNING_PCT_OF_TOTAL'] * 100
            total_variance += variance
            f.write(f"| {row['FIELD_FAMILY']} | ${variance:,.2f} | {(variance/bene_financial['FINANCIAL_VARIANCE'].sum())*100:.1f}% | {pct:.1f}% |\n")
        f.write(f"| **TOTAL** | **${total_variance:,.2f}** | **100%** | - |\n")
        f.write("\n📊 **Detailed Data**: **[financial_impact_beneficiary.csv](data/financial_impact_beneficiary.csv)**\n\n")
        f.write("**CSV Contents:**\n")
        f.write("- Columns: DATA_SET, FIELD_FAMILY, FINANCIAL_VARIANCE, RUNNING_PCT_OF_TOTAL\n")
        f.write("- Financial variance across 9 beneficiary reimbursement fields (IP, OP, CAR × MEDREIMB, BENRES, PPPYMT)\n")
        f.write("- Aggregates all absolute dollar differences between source and new system\n\n")
        f.write("**How to Use:**\n")
        f.write("- Identify which payment types (Inpatient, Outpatient, Carrier) have highest variance\n")
        f.write("- Compare MEDREIMB (Medicare reimbursement) vs BENRES (beneficiary responsibility) vs PPPYMT (primary payer) errors\n")
        f.write("- Use for financial reconciliation and to estimate potential claim adjustment volume\n\n")
        
        # Beneficiary Analysis Section
        f.write("---\n\n")
        f.write("## 3. Beneficiary Data Quality\n\n")
        
        f.write("### Summary of Beneficiary Discrepancies\n\n")
        f.write("| Discrepancy Type | Count | CSV Export |\n")
        f.write("|------------------|-------|------------|\n")
        f.write(f"| Missing in New System | {bene_res['missing_count']:,} | [missing_beneficiaries.csv](data/missing_beneficiaries.csv) |\n")
        f.write(f"| Extra in New System | {len(bene_res['extra_sample']):,} | [extra_beneficiaries.csv](data/extra_beneficiaries.csv) |\n")
        f.write(f"| Attribute Mismatches | {bene_res['mismatch_count']:,} | [beneficiary_attribute_mismatches.csv](data/beneficiary_attribute_mismatches.csv) |\n")
        f.write(f"| Date Differences (DOB/DOD) | {bene_res['date_diff_count']:,} | [beneficiary_date_differences.csv](data/beneficiary_date_differences.csv) |\n")
        f.write(f"| Comprehensive Line Differences | {bene_res['comprehensive_beneficiary_count']:,} | [comprehensive_beneficiary_differences.csv](data/comprehensive_beneficiary_differences.csv) |\n")
        f.write(f"| Beneficiary Audit Sample | {bene_res['bene_records_with_discrepancies']:,} | [audit_beneficiary_summary.csv](data/audit_beneficiary_summary.csv) |\n")
        f.write("\n")
        
        if bene_res['mismatch_count'] > 0:
            f.write("### Sample Attribute Mismatches\n\n")
            f.write("The following fields were compared: Birth Date, Sex, Race, ESRD Indicator\n\n")
            sample_df = bene_res['mismatch_sample'][['DESYNPUF_ID', 'YEAR', 'src_BENE_BIRTH_DT', 'new_BENE_BIRTH_DT', 
                                                       'src_BENE_SEX_IDENT_CD', 'new_BENE_SEX_IDENT_CD']].head(5)
            f.write(sample_df.to_markdown(index=False))
            f.write("\n\n")
            f.write(f"_Showing 5 of {bene_res['mismatch_count']:,} total mismatches. See CSV for complete list._\n\n")
        
        # Detailed CSV Documentation for Beneficiaries
        f.write("### Detailed CSV Exports - Beneficiary Analysis\n\n")
        
        f.write("**1. [missing_beneficiaries.csv](data/missing_beneficiaries.csv)** ({:,} records)\n\n".format(bene_res['missing_count']))
        f.write("- **Columns**: DESYNPUF_ID, BENE_DEATH_YEAR, YEAR, FINDING\n")
        f.write("- **What it shows**: Beneficiaries present in source system but missing in new system\n")
        f.write("- **Key filter**: Only includes beneficiaries who either (a) had no death date, or (b) death year is after the record year (should still be active)\n")
        f.write("- **Action required**: Investigate ETL process - these records may indicate data loss during migration\n")
        f.write("- **Business impact**: Missing beneficiaries = missing claims = potential revenue loss\n\n")
        
        f.write("**2. [extra_beneficiaries.csv](data/extra_beneficiaries.csv)** ({:,} records)\n\n".format(len(bene_res['extra_sample'])))
        f.write("- **Columns**: DESYNPUF_ID, BENE_DEATH_YEAR, YEAR, FINDING\n")
        f.write("- **What it shows**: Beneficiaries present in new system but NOT in source system\n")
        f.write("- **Possible causes**: Duplicate creation, test data leakage, or incorrect source extraction\n")
        f.write("- **Action required**: Review and potentially purge if these are erroneous records\n\n")
        
        f.write("**3. [beneficiary_attribute_mismatches.csv](data/beneficiary_attribute_mismatches.csv)** ({:,} records)\n\n".format(bene_res['mismatch_count']))
        f.write("- **Columns**: DESYNPUF_ID, YEAR, src_BENE_BIRTH_DT, new_BENE_BIRTH_DT, src_BENE_SEX_IDENT_CD, new_BENE_SEX_IDENT_CD, src_BENE_RACE_CD, new_BENE_RACE_CD, src_ERSD_IND, new_ERSD_IND, FINDING\n")
        f.write("- **What it shows**: Beneficiaries where core demographic attributes changed between systems\n")
        f.write("- **Why this matters**: Demographics should be immutable - changes indicate transformation errors\n")
        f.write("- **Action required**: \n")
        f.write("  - Birth date changes: Review date parsing logic (format conversions, timezone issues)\n")
        f.write("  - Sex/Race changes: Check lookup table mappings and code standardization\n")
        f.write("  - ESRD flag changes: Verify chronic condition derivation logic\n\n")
        
        f.write("**4. [beneficiary_date_differences.csv](data/beneficiary_date_differences.csv)** ({:,} records)\n\n".format(bene_res['date_diff_count']))
        f.write("- **Columns**: DESYNPUF_ID, src_dob, new_dob, src_dod, new_dod\n")
        f.write("- **What it shows**: Focused view of birth and death date discrepancies\n")
        f.write("- **Common patterns to look for**:\n")
        f.write("  - Date or attribute changes (not matched)\n")
        f.write("  - Date format issues (YYYYMMDD vs YYYY-MM-DD)\n")
        f.write("  - Day/month transposition (01/06 vs 06/01)\n")
        f.write("  - Year arithmetic errors (+/- 1 year patterns)\n")
        f.write("- **Action required**: Sample records, identify systematic vs random errors, fix transformation logic\n\n")
        
        f.write("**5. [comprehensive_beneficiary_differences.csv](data/comprehensive_beneficiary_differences.csv)** ({:,} records)\n\n".format(bene_res['comprehensive_beneficiary_count']))
        f.write("- **Columns**: All 33 beneficiary summary fields (DESYNPUF_ID, YEAR, demographics, coverage, conditions, financials)\n")
        f.write("- **What it shows**: ANY row where ANY field differs between source and new (SQL EXCEPT operation)\n")
        f.write("- **Size**: Largest beneficiary CSV - this is your master list of all differences\n")
        f.write("- **How to use**:\n")
        f.write("  - Import into Excel/SQL and pivot by specific columns to find patterns\n")
        f.write("  - Compare financial fields (MEDREIMB_*, BENRES_*, PPPYMT_*) to quantify dollar impact per beneficiary\n")
        f.write("  - Cross-reference DESYNPUF_IDs with the other CSVs to understand root causes\n")
        f.write("  - Use for detailed reconciliation and auditing\n\n")

        f.write("**6. [audit_beneficiary_summary.csv](data/audit_beneficiary_summary.csv)** ({:,} records)\n\n".format(bene_res['bene_records_with_discrepancies_sample']))
        f.write("- **Columns**: All 33 claim summary fields (DESYNPUF_ID, YEAR, demographics, coverage, conditions, financials)\n")
        f.write("- **What it shows**: ANY row where ANY field differs between source and new (SQL EXCEPT operation)\n")
        f.write("- **Size**: Largest claim CSV - this is your master list of all differences (0 equals match, 1 equals no match)\n")
        f.write("- **How to use**:\n")
        f.write("  - Import into Excel/SQL and pivot by specific columns to find patterns\n")
        f.write("  - Compare financial fields (MEDREIMB_*, BENRES_*, PPPYMT_*) to quantify volume of defects for each field\n")
        f.write("  - Cross-reference DESYNPUF_IDs with the other CSVs to understand root causes\n")
        f.write("  - Use for detailed reconciliation, auditing, and six sigma calculations\n\n")
        
        # Claims Analysis Section
        f.write("---\n\n")
        f.write("## 4. Carrier Claims Data Quality\n\n")
        
        f.write("### Summary of Claims Discrepancies\n\n")
        f.write("| Discrepancy Type | Count | CSV Export |\n")
        f.write("|------------------|-------|------------|\n")
        f.write(f"| Missing Claims | {claims_res['missing_claims_count']:,} | [missing_claims.csv](data/missing_claims.csv) |\n")
        f.write(f"| Payment Amount Discrepancies | {claims_res['payment_mismatch_count']:,} | [claim_payment_amount_discrepancies.csv](data/claim_payment_amount_discrepancies.csv) |\n")
        f.write(f"| Orphan Claims (4-way match) | {claims_res['comprehensive_orphan_claims_count']:,} | [comprehensive_orphan_claims.csv](data/comprehensive_orphan_claims.csv) |\n")
        f.write(f"| Claim Audit Sample | {claims_res['claim_records_with_discrepancies']:,} | [claim_records_with_discrepancies_sample.csv](data/audit_claim_summary.csv) |\n")
        f.write("\n")
        
        if claims_res['payment_mismatch_count'] > 0:
            f.write("### Sample Payment Discrepancies\n\n")
            sample_df = claims_res['payment_mismatch_sample'][['CLM_ID', 'CLM_FROM_DT', 'src_pmt', 'new_pmt', 'variance']].head(5)
            f.write(sample_df.to_markdown(index=False))
            f.write("\n\n")
            f.write(f"_Showing 5 of {claims_res['payment_mismatch_count']:,} total payment discrepancies. See CSV for complete list._\n\n")
        
        # Detailed CSV Documentation for Claims
        f.write("### Detailed CSV Exports - Claims Analysis\n\n")
        
        f.write("**1. [missing_claims.csv](data/missing_claims.csv)** ({:,} records)\n\n".format(claims_res['missing_claims_count']))
        f.write("- **Columns**: CLM_ID, FINDING\n")
        f.write("- **What it shows**: Claims present in source OR new system (UNION of both directions)\n")
        f.write("- **FINDING values**:\n")
        f.write("  - 'Error: Claim Missing in New File' = Claims lost during migration\n")
        f.write("  - 'Error: Claim Present in New File, Not in Original File' = Claims created erroneously\n")
        f.write("- **Business impact**: Missing claims = unbilled services = revenue loss\n")
        f.write("- **Action required**: Group by CLM_FROM_DT (claim date) to identify if losses are concentrated in specific time periods\n\n")
        
        f.write("**2. [claim_payment_amount_discrepancies.csv](data/claim_payment_amount_discrepancies.csv)** ({:,} records, 5.2 MB)\n\n".format(claims_res['payment_mismatch_count']))
        f.write("- **Columns**: CLM_ID, record_status, STATE_NAME, CLM_FROM_DT, src_pmt, new_pmt, processing_ind, allowed_amt, business_rule_status, variance\n")
        f.write("- **What it shows**: Claims where LINE_NCH_PMT_AMT_1 (NCH payment amount line 1) differs between systems\n")
        f.write("- **Critical field: business_rule_status**:\n")
        f.write("  - 'Valid' = Payment should match based on business rules (allowed_amt, processing_ind)\n")
        f.write("  - 'Invalid' = Payment difference may be expected due to processing rules\n")
        f.write("- **How to use**:\n")
        f.write("  - Filter WHERE business_rule_status = 'Valid' AND variance > threshold (e.g., $10)\n")
        f.write("  - Group by STATE_NAME to identify geographic patterns\n")
        f.write("  - Sort by ABS(variance) descending to prioritize high-dollar errors\n")
        f.write("  - Calculate total financial exposure: SUM(ABS(variance))\n")
        f.write("- **Contains**: Full claim-level detail for payment reconciliation and adjustment processing\n\n")
        
        f.write("**3. [comprehensive_orphan_claims.csv](data/comprehensive_orphan_claims.csv)** ({:,} records, 3.9 MB)\n\n".format(claims_res['comprehensive_orphan_claims_count']))
        f.write("- **Columns**: All 142 carrier claim fields (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT, diagnosis codes, procedures, NPIs, financials, etc.)\n")
        f.write("- **What it shows**: Claims in new system that don't match on 4-way key (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT)\n")
        f.write("- **Why this matters**: Matching on CLM_ID alone found fewer orphans - using 4-way match finds date mismatches\n")
        f.write("- **Possible causes**:\n")
        f.write("  - Date transformation errors (CLM_FROM_DT or CLM_THRU_DT changed)\n")
        f.write("  - Beneficiary ID transformation errors (DESYNPUF_ID changed)\n")
        f.write("  - Claim ID transformation errors (CLM_ID changed)\n")
        f.write("  - Combination of above = claim exists but with wrong metadata\n")
        f.write("- **How to investigate**:\n")
        f.write("  - Check if CLM_ID exists in source with different dates\n")
        f.write("  - Check if DESYNPUF_ID mapping is correct\n")
        f.write("  - Look for CLM_FROM_DT = '20231332' (invalid dates that should be '20080304')\n")
        f.write("  - These claims may need manual reconciliation or reprocessing\n\n")

        f.write("**4. [audit_claim_summary.csv](data/audit_claim_summary.csv)** ({:,} records)\n\n".format(claims_res['claim_records_with_discrepancies_sample']))
        f.write("- **Columns**: All 142 carrier claim fields (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT, diagnosis codes, procedures, NPIs, financials, etc.)\n")
        f.write("- **What it shows**: ANY row where ANY field differs between source and new (SQL EXCEPT operation)\n")
        f.write("- **Size**: Largest claim CSV - this is your master list of all claim-level differences (0 equals match, 1 equals no match)\n")
        f.write("- **How to use**:\n")
        f.write("  - Import into Excel/SQL and pivot by specific columns to find patterns\n")
        f.write("  - Compare financial fields (MEDREIMB_*, BENRES_*, PPPYMT_*) to quantify volume of defects for each field\n")
        f.write("  - Cross-reference DESYNPUF_IDs with the other CSVs to understand root causes\n")
        f.write("  - Use for detailed reconciliation, auditing, and six sigma calculations\n\n")
        
        # Recommendations Section
        f.write("---\n\n")
        f.write("## 5. Recommendations\n\n")
        
        # Generate data-driven recommendations based on findings
        recommendations = []
        
        # Six Sigma based recommendations
        for _, row in six_sigma_overall.iterrows():
            if row['SIGMA_LEVEL'] < 3.0:
                recommendations.append(f"**CRITICAL**: {row['SUBJECT']} quality is below acceptable threshold ({row['SIGMA_LEVEL']:.2f}σ). "
                                     f"Immediate root cause analysis required for {int(row['TOTAL_DEFECTS']):,} defects.")
            elif row['SIGMA_LEVEL'] < 4.0:
                recommendations.append(f"**HIGH PRIORITY**: {row['SUBJECT']} quality should be improved ({row['SIGMA_LEVEL']:.2f}σ). "
                                     f"Target 4σ or higher before production deployment.")
        
        # Field-specific recommendations
        top_carrier_field = six_sigma_res['six_sigma_carrier'].nlargest(1, 'TOTAL_DEFECTS').iloc[0]
        recommendations.append(f"**FOCUS AREA**: {top_carrier_field['FIELD_FAMILY']} has {int(top_carrier_field['TOTAL_DEFECTS']):,} defects. "
                             f"Prioritize data cleansing and transformation logic review for this field.")
        
        # Financial impact recommendations
        top_financial = claim_financial.iloc[0]
        if top_financial['RUNNING_PCT_OF_TOTAL'] > 0.3:
            recommendations.append(f"**FINANCIAL PRIORITY**: {top_financial['FIELD_FAMILY']} accounts for "
                                 f"{top_financial['RUNNING_PCT_OF_TOTAL']*100:.1f}% of total financial variance "
                                 f"(${top_financial['FINANCIAL_VARIANCE']:,.2f}). Address this field first for maximum impact.")
        
        # Missing data recommendations
        if bene_res['missing_count'] > 100:
            recommendations.append(f"**DATA LOSS**: {bene_res['missing_count']:,} beneficiaries missing in new system. "
                                 f"Verify ETL completeness and investigate potential data loss.")

        if bene_res['bene_records_with_discrepancies'] > 1000:
            recommendations.append(f"**DATA DISCREPANCIES**: {bene_res['bene_records_with_discrepancies']:,} beneficiaries data discrepancies in new system. "
                                 f"Review ingestion logs and verify source-to-target mapping at the field level.")
        
        if claims_res['missing_claims_count'] > 1000:
            recommendations.append(f"**DATA LOSS**: {claims_res['missing_claims_count']:,} claims missing in new system. "
                                 f"Review ingestion logs and verify source-to-target mapping.")

        if claims_res['claim_records_with_discrepancies'] > 1000:
            recommendations.append(f"**DATA DISCREPANCIES**: {claims_res['claim_records_with_discrepancies']:,} claims data discrepancies in new system. "
                                 f"Review ingestion logs and verify source-to-target mapping at the field level.")
        
        # Output recommendations
        for i, rec in enumerate(recommendations, 1):
            f.write(f"{i}. {rec}\n\n")
        
        # Conclusion
        f.write("---\n\n")
        f.write("## Conclusion\n\n")
        
        overall_sigma_avg = six_sigma_overall['SIGMA_LEVEL'].mean()
        if overall_sigma_avg >= 6.0:
            f.write("The migration demonstrates **world-class quality** with an average sigma level of {:.2f}σ. ".format(overall_sigma_avg))
            f.write("No remedial action required.\n\n")
        elif overall_sigma_avg >= 5.0:
            f.write("The migration demonstrates **excellent overall quality** with an average sigma level of {:.2f}σ. ".format(overall_sigma_avg))
            f.write("Little remedial action required depending on severity of identified discrepancies.\n\n")
        elif overall_sigma_avg >= 4.0:
            f.write("The migration demonstrates **good overall quality** with an average sigma level of {:.2f}σ. ".format(overall_sigma_avg))
            f.write("Remedial action required.\n\n")
        elif overall_sigma_avg >= 3.0:
            f.write("The migration shows **acceptable quality** with an average sigma level of {:.2f}σ, ".format(overall_sigma_avg))
            f.write("however, significant improvements are needed in specific areas before production deployment.\n\n")
        else:
            f.write("The migration quality is **below acceptable standards** with an average sigma level of {:.2f}σ. ".format(overall_sigma_avg))
            f.write("**DO NOT PROCEED** with production cutover until critical issues are resolved.\n\n")
        
        f.write("**Next Steps:**\n")
        f.write("1. Review detailed CSV exports for complete defect lists\n")
        f.write("2. Prioritize remediation based on Six Sigma and Financial Impact analysis\n")
        f.write("3. Re-run comparison after implementing fixes\n")
        f.write("4. Target minimum 4σ quality before production deployment\n\n")
        
        f.write("---\n\n")
        f.write("_Report generated by Data Migration Quality Assessment Pipeline_\n")
        
    print(f"✅ Enhanced report generated at {output_path}")
    print(f"   - Includes Six Sigma quality metrics")
    print(f"   - Includes Financial Impact Pareto analysis")
    print(f"   - References 14 CSV exports in data/ directory")
    print(f"   - Includes high level and extremely detailed pre- and post- migration analysis.")
    print(f"   - Detailed files show exactly which rows are not matching and what fields are changed after the migration.")


if __name__ == "__main__":
    # This is for testing only - actual execution happens through main.py
    print("This module should be called from main.py")
