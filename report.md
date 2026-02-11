# Data Comparison Report

## Executive Summary
This report compares the legacy healthcare claims processing system (Source) with the new system (New). The goal is to identify discrepancies and data quality issues.

## 1. Beneficiary Summary Analysis
- **Missing Beneficiaries in New System**: 159
- **Unexpected Extra Beneficiaries in New System**: 159
- **Beneficiaries with Attribute Mismatches**: 178

### Sample Attribute Mismatches
Columns compared:
- BENE_BIRTH_DT
- BENE_SEX_IDENT_CD
- BENE_ESRD_IND

| DESYNPUF_ID      |   YEAR |   src_dob |   new_dob |   src_sex |   new_sex | src_esrd   | new_esrd   |
|:-----------------|-------:|----------:|----------:|----------:|----------:|:-----------|:-----------|
| 283EE6C929EA6E2A |   2010 |  19310601 |  19300903 |         1 |         1 | Y          | Y          |
| 2B868D4AF1ED6CB9 |   2010 |  19390401 |  19380503 |         2 |         2 | 0          | 0          |
| 2C606D7AF2DA6CDF |   2010 |  19230301 |  19221105 |         2 |         2 | 0          | 0          |
| 2D6220567C59C1A7 |   2010 |  19350701 |  19360325 |         1 |         1 | 0          | 0          |
| 30C7F0B30079FAFC |   2010 |  19480801 |  19471219 |         1 |         1 | 0          | 0          |

## 2. Carrier Claims Analysis
- **Missing Claims in New System**: 0
- **Claims with Payment Discrepancies**: 10411

### Sample Payment Mismatches
|          CLM_ID |   src_pmt |   new_pmt |   diff |
|----------------:|----------:|----------:|-------:|
| 887873386722784 |        20 |         0 |     20 |
| 887063386754159 |        20 |         0 |     20 |
| 887763385123853 |        20 |         0 |     20 |
| 887133387096664 |        60 |        54 |      6 |
| 887293385394790 |       260 |       234 |     26 |

## 3. Conclusion
The comparison highlights several areas of concern. There are significant data mismatches that need to be addressed before decommissioning the old system.

The more complete analysis is included in the "Data Migration Analysis.pptx" presentation.