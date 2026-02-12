# Data Migration Quality Assessment Report

## Executive Summary

This report evaluates the quality of the healthcare claims data migration from the legacy system (Source) to the new system (New). The analysis employs Six Sigma methodology to quantify data quality and identifies areas requiring remediation.

### Migration Scorecard

| Metric | Value | Status |
|--------|-------|--------|
| Carrier Claims Quality | 4.58σ (1,095 DPMO, 99.89% yield) | ✅ Excellent |
| Beneficiary Summary Quality | 4.68σ (768 DPMO, 99.92% yield) | ✅ Excellent |
| Missing Beneficiaries | 159 | ❌ Critical |
| Missing Claims | 4,777 | ❌ Critical |
| Payment Discrepancies | 82,699 | ❌ Critical |

---

## 1. Six Sigma Quality Analysis

**Six Sigma** measures data quality in defects per million opportunities (DPMO). Higher sigma levels indicate better quality:
- **6σ**: 3.4 DPMO (99.9997% yield) - World-class
- **5σ**: 233 DPMO (99.98% yield) - Excellent
- **4σ**: 6,210 DPMO (99.38% yield) - Good
- **3σ**: 66,807 DPMO (93.32% yield) - Acceptable
- **2σ**: 308,538 DPMO (69.15% yield) - Poor

### Overall Migration Quality

| Dataset | Total Units | Total Defects | Sigma Level | DPMO | Yield % |
|---------|-------------|---------------|-------------|------|----------|
| Carrier Claims | 4,746,577 | 530,376 | 4.58σ | 1,095 | 99.89% |
| Beneficiary Summary | 343,803 | 8,193 | 4.68σ | 768 | 99.92% |

📊 **Detailed Analysis**: 

**[six_sigma.csv](data/six_sigma.csv)** - Overall quality metrics
- Contains: SUBJECT, TOTAL_UNITS, TOTAL_DEFECTS, TOTAL_OPPORTUNITIES, DPMO, YIELD, SIGMA_LEVEL
- Use this to: Track overall migration quality score and identify which dataset (Carrier Claims vs Beneficiary) needs priority attention

**[six_sigma_carrier.csv](data/six_sigma_carrier.csv)** - Field-level carrier claim defects (102 fields analyzed)
- Contains: SOURCE, FIELD_FAMILY, TOTAL_DEFECTS, TOTAL_OPPORTUNITIES, DPMO, SIGMA_LEVEL
- Use this to: Identify which specific carrier claim fields have the highest error rates
- Fields analyzed include: ICD9 diagnosis codes, NPI identifiers, tax numbers, HCPCS codes, payment amounts, deductibles, coinsurance, processing indicators
- Sort by TOTAL_DEFECTS descending to prioritize remediation efforts

**[six_sigma_beneficiary.csv](data/six_sigma_beneficiary.csv)** - Field-level beneficiary defects (31 fields analyzed)
- Contains: SOURCE, FIELD_FAMILY, TOTAL_DEFECTS, TOTAL_OPPORTUNITIES, DPMO, SIGMA_LEVEL
- Use this to: Identify which beneficiary attributes have the highest error rates
- Fields analyzed include: Demographics (birth/death dates, sex, race), coverage months, chronic conditions (11 conditions), financial fields (reimbursements, benefits, payments)
- Focus on fields with DPMO > 100,000 (below 3σ) for immediate remediation

### Top Defect Fields

**Carrier Claims - Highest Error Fields:**

| Field Family | Total Defects | DPMO | Sigma Level |
|--------------|---------------|------|-------------|
| LINE_PRCSG_IND_CD | 75,497 | 15,905 | 3.66σ |
| LINE_ICD9_DGNS_CD | 75,238 | 15,851 | 3.66σ |
| TAX_NUM | 75,176 | 15,837 | 3.66σ |
| PRF_PHYSN_NPI | 75,169 | 15,836 | 3.66σ |
| HCPCS_CD | 75,106 | 15,823 | 3.66σ |

**Beneficiary Summary - Highest Error Fields:**

| Field Family | Total Defects | DPMO | Sigma Level |
|--------------|---------------|------|-------------|
| BENE_BIRTH_DT | 496 | 1,442 | 4.50σ |
| BENE_HI_CVRAGE_TOT_MONS | 337 | 980 | 4.61σ |
| BENE_SMI_CVRAGE_TOT_MONS | 322 | 936 | 4.63σ |
| BENE_COUNTY_CD | 318 | 924 | 4.63σ |
| BENE_DEATH_DT | 318 | 924 | 4.63σ |

---

## 2. Financial Impact Analysis

**Pareto Analysis** identifies which field families contribute most to financial errors. The 80/20 rule typically applies: ~80% of financial variance comes from ~20% of fields.

### Carrier Claims Financial Variance

| Field Family | Financial Variance | % of Total | Cumulative % |
|--------------|-------------------|------------|---------------|
| LINE_NCH_PMT_AMT | $158,480.91 | 36.5% | 36.5% |
| LINE_BENE_PRMRY_PYR_PD_AMT | $98,772.86 | 22.7% | 59.2% |
| LINE_BENE_PTB_DDCTBL_AMT | $96,806.45 | 22.3% | 81.5% |
| LINE_ALOWD_CHRG_AMT | $62,816.30 | 14.5% | 96.0% |
| LINE_COINSRNC_AMT | $17,507.34 | 4.0% | 100.0% |
| **TOTAL** | **$434,383.86** | **100%** | - |

📊 **Detailed Data**: **[financial_impact_carrier_claim.csv](data/financial_impact_carrier_claim.csv)**

**CSV Contents:**
- Columns: DATA_SET, FIELD_FAMILY, FINANCIAL_VARIANCE, RUNNING_PCT_OF_TOTAL
- Contains absolute dollar variance aggregated by field family
- Pre-sorted by financial impact (highest to lowest)

**How to Use:**
- Identify the 20% of fields causing 80% of financial variance (Pareto principle)
- Focus remediation on field families with RUNNING_PCT_OF_TOTAL < 0.8 (first ~80% of errors)
- Use this to justify resource allocation for data quality improvement efforts
- Cross-reference with six_sigma_carrier.csv to find fields that are both high-defect AND high-cost

### Beneficiary Summary Financial Variance

| Field Family | Financial Variance | % of Total | Cumulative % |
|--------------|-------------------|------------|---------------|
| MEDREIMB_OP | $10,924.62 | 30.7% | 30.7% |
| MEDREIMB_IP | $7,004.07 | 19.7% | 50.3% |
| MEDREIMB_CAR | $4,458.42 | 12.5% | 62.9% |
| BENRES_IP | $3,231.46 | 9.1% | 71.9% |
| BENRES_CAR | $2,466.78 | 6.9% | 78.9% |
| PPPYMT_IP | $2,128.46 | 6.0% | 84.8% |
| PPPYMT_CAR | $1,996.32 | 5.6% | 90.5% |
| BENRES_OP | $1,907.74 | 5.4% | 95.8% |
| PPPYMT_OP | $1,491.36 | 4.2% | 100.0% |
| **TOTAL** | **$35,609.23** | **100%** | - |

📊 **Detailed Data**: **[financial_impact_beneficiary.csv](data/financial_impact_beneficiary.csv)**

**CSV Contents:**
- Columns: DATA_SET, FIELD_FAMILY, FINANCIAL_VARIANCE, RUNNING_PCT_OF_TOTAL
- Financial variance across 9 beneficiary reimbursement fields (IP, OP, CAR × MEDREIMB, BENRES, PPPYMT)
- Aggregates all absolute dollar differences between source and new system

**How to Use:**
- Identify which payment types (Inpatient, Outpatient, Carrier) have highest variance
- Compare MEDREIMB (Medicare reimbursement) vs BENRES (beneficiary responsibility) vs PPPYMT (primary payer) errors
- Use for financial reconciliation and to estimate potential claim adjustment volume

---

## 3. Beneficiary Data Quality

### Summary of Beneficiary Discrepancies

| Discrepancy Type | Count | CSV Export |
|------------------|-------|------------|
| Missing in New System | 159 | [missing_beneficiaries.csv](data/missing_beneficiaries.csv) |
| Extra in New System | 0 | [extra_beneficiaries.csv](data/extra_beneficiaries.csv) |
| Attribute Mismatches | 178 | [beneficiary_attribute_mismatches.csv](data/beneficiary_attribute_mismatches.csv) |
| Date Differences (DOB/DOD) | 178 | [beneficiary_date_differences.csv](data/beneficiary_date_differences.csv) |
| Comprehensive Line Differences | 1,322 | [comprehensive_beneficiary_differences.csv](data/comprehensive_beneficiary_differences.csv) |

### Sample Attribute Mismatches

The following fields were compared: Birth Date, Sex, Race, ESRD Indicator

| DESYNPUF_ID      |   YEAR |   src_BENE_BIRTH_DT |   new_BENE_BIRTH_DT |   src_BENE_SEX_IDENT_CD |   new_BENE_SEX_IDENT_CD |
|:-----------------|-------:|--------------------:|--------------------:|------------------------:|------------------------:|
| 283EE6C929EA6E2A |   2010 |            19310601 |            19300903 |                       1 |                       1 |
| 2B868D4AF1ED6CB9 |   2010 |            19390401 |            19380503 |                       2 |                       2 |
| 2C606D7AF2DA6CDF |   2010 |            19230301 |            19221105 |                       2 |                       2 |
| 2D6220567C59C1A7 |   2010 |            19350701 |            19360325 |                       1 |                       1 |
| 30C7F0B30079FAFC |   2010 |            19480801 |            19471219 |                       1 |                       1 |

_Showing 5 of 178 total mismatches. See CSV for complete list._

### Detailed CSV Exports - Beneficiary Analysis

**1. [missing_beneficiaries.csv](data/missing_beneficiaries.csv)** (159 records)

- **Columns**: DESYNPUF_ID, BENE_DEATH_YEAR, YEAR, FINDING
- **What it shows**: Beneficiaries present in source system but missing in new system
- **Key filter**: Only includes beneficiaries who either (a) had no death date, or (b) death year is after the record year (should still be active)
- **Action required**: Investigate ETL process - these records may indicate data loss during migration
- **Business impact**: Missing beneficiaries = missing claims = potential revenue loss

**2. [extra_beneficiaries.csv](data/extra_beneficiaries.csv)** (0 records)

- **Columns**: DESYNPUF_ID, BENE_DEATH_YEAR, YEAR, FINDING
- **What it shows**: Beneficiaries present in new system but NOT in source system
- **Possible causes**: Duplicate creation, test data leakage, or incorrect source extraction
- **Action required**: Review and potentially purge if these are erroneous records

**3. [beneficiary_attribute_mismatches.csv](data/beneficiary_attribute_mismatches.csv)** (178 records)

- **Columns**: DESYNPUF_ID, YEAR, src_BENE_BIRTH_DT, new_BENE_BIRTH_DT, src_BENE_SEX_IDENT_CD, new_BENE_SEX_IDENT_CD, src_BENE_RACE_CD, new_BENE_RACE_CD, src_ERSD_IND, new_ERSD_IND, FINDING
- **What it shows**: Beneficiaries where core demographic attributes changed between systems
- **Why this matters**: Demographics should be immutable - changes indicate transformation errors
- **Action required**: 
  - Birth date changes: Review date parsing logic (format conversions, timezone issues)
  - Sex/Race changes: Check lookup table mappings and code standardization
  - ESRD flag changes: Verify chronic condition derivation logic

**4. [beneficiary_date_differences.csv](data/beneficiary_date_differences.csv)** (178 records)

- **Columns**: DESYNPUF_ID, src_dob, new_dob, src_dod, new_dod
- **What it shows**: Focused view of birth and death date discrepancies
- **Common patterns to look for**:
  - Date format issues (YYYYMMDD vs YYYY-MM-DD)
  - Day/month transposition (01/06 vs 06/01)
  - Year arithmetic errors (+/- 1 year patterns)
- **Action required**: Sample records, identify systematic vs random errors, fix transformation logic

**5. [comprehensive_beneficiary_differences.csv](data/comprehensive_beneficiary_differences.csv)** (1,322 records)

- **Columns**: All 33 beneficiary summary fields (DESYNPUF_ID, YEAR, demographics, coverage, conditions, financials)
- **What it shows**: ANY row where ANY field differs between source and new (SQL EXCEPT operation)
- **Size**: Largest beneficiary CSV - this is your master list of all differences
- **How to use**:
  - Import into Excel/SQL and pivot by specific columns to find patterns
  - Compare financial fields (MEDREIMB_*, BENRES_*, PPPYMT_*) to quantify dollar impact per beneficiary
  - Cross-reference DESYNPUF_IDs with the other CSVs to understand root causes
  - Use for detailed reconciliation and auditing

---

## 4. Carrier Claims Data Quality

### Summary of Claims Discrepancies

| Discrepancy Type | Count | CSV Export |
|------------------|-------|------------|
| Missing Claims | 4,777 | [missing_claims.csv](data/missing_claims.csv) |
| Payment Amount Discrepancies | 82,699 | [claim_payment_amount_discrepancies.csv](data/claim_payment_amount_discrepancies.csv) |
| Orphan Claims (4-way match) | 5,242 | [comprehensive_orphan_claims.csv](data/comprehensive_orphan_claims.csv) |

### Sample Payment Discrepancies

|          CLM_ID |   CLM_FROM_DT |   src_pmt |   new_pmt |   variance |
|----------------:|--------------:|----------:|----------:|-----------:|
| 887183387821152 |      20101109 |        30 |        27 |          3 |
| 887473386966448 |      20080531 |         0 |         5 |         -5 |
| 887883389139678 |      20100629 |         0 |         5 |         -5 |
| 887913386549027 |      20090407 |        60 |        54 |          6 |
| 887723388459696 |      20080324 |        20 |        18 |          2 |

_Showing 5 of 82,699 total payment discrepancies. See CSV for complete list._

### Detailed CSV Exports - Claims Analysis

**1. [missing_claims.csv](data/missing_claims.csv)** (4,777 records)

- **Columns**: CLM_ID, FINDING
- **What it shows**: Claims present in source OR new system (UNION of both directions)
- **FINDING values**:
  - 'Error: Claim Missing in New File' = Claims lost during migration
  - 'Error: Claim Present in New File, Not in Original File' = Claims created erroneously
- **Business impact**: Missing claims = unbilled services = revenue loss
- **Action required**: Group by CLM_FROM_DT (claim date) to identify if losses are concentrated in specific time periods

**2. [claim_payment_amount_discrepancies.csv](data/claim_payment_amount_discrepancies.csv)** (82,699 records, 5.2 MB)

- **Columns**: CLM_ID, record_status, STATE_NAME, CLM_FROM_DT, src_pmt, new_pmt, processing_ind, allowed_amt, business_rule_status, variance
- **What it shows**: Claims where LINE_NCH_PMT_AMT_1 (NCH payment amount line 1) differs between systems
- **Critical field: business_rule_status**:
  - 'Valid' = Payment should match based on business rules (allowed_amt, processing_ind)
  - 'Invalid' = Payment difference may be expected due to processing rules
- **How to use**:
  - Filter WHERE business_rule_status = 'Valid' AND variance > threshold (e.g., $10)
  - Group by STATE_NAME to identify geographic patterns
  - Sort by ABS(variance) descending to prioritize high-dollar errors
  - Calculate total financial exposure: SUM(ABS(variance))
- **Contains**: Full claim-level detail for payment reconciliation and adjustment processing

**3. [comprehensive_orphan_claims.csv](data/comprehensive_orphan_claims.csv)** (5,242 records, 3.9 MB)

- **Columns**: All 142 carrier claim fields (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT, diagnosis codes, procedures, NPIs, financials, etc.)
- **What it shows**: Claims in new system that don't match on 4-way key (DESYNPUF_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT)
- **Why this matters**: Matching on CLM_ID alone found fewer orphans - using 4-way match finds date mismatches
- **Possible causes**:
  - Date transformation errors (CLM_FROM_DT or CLM_THRU_DT changed)
  - Beneficiary ID transformation errors (DESYNPUF_ID changed)
  - Claim ID transformation errors (CLM_ID changed)
  - Combination of above = claim exists but with wrong metadata
- **How to investigate**:
  - Check if CLM_ID exists in source with different dates
  - Check if DESYNPUF_ID mapping is correct
  - Look for CLM_FROM_DT = '20231332' (invalid dates that should be '20080304')
  - These claims may need manual reconciliation or reprocessing

---

## 5. Recommendations

1. **FOCUS AREA**: LINE_PRCSG_IND_CD has 75,497 defects. Prioritize data cleansing and transformation logic review for this field.

2. **FINANCIAL PRIORITY**: LINE_NCH_PMT_AMT accounts for 36.5% of total financial variance ($158,480.91). Address this field first for maximum impact.

3. **DATA LOSS**: 159 beneficiaries missing in new system. Verify ETL completeness and investigate potential data loss.

4. **DATA LOSS**: 4,777 claims missing in new system. Review ingestion logs and verify source-to-target mapping.

---

## Conclusion

The migration demonstrates **good overall quality** with an average sigma level of 4.63σ. However, address the identified discrepancies before final production cutover.

**Next Steps:**
1. Review detailed CSV exports for complete defect lists
2. Prioritize remediation based on Six Sigma and Financial Impact analysis
3. Re-run comparison after implementing fixes
4. Target minimum 4σ quality before production deployment

---

_Report generated by Data Migration Quality Assessment Pipeline_
