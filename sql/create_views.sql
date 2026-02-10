
-- Compare row counts for both table sets
DROP VIEW IF EXISTS vw_source_counts;
CREATE VIEW vw_source_counts as
SELECT 
    'beneficiary_summary' AS table_name,
    (SELECT COUNT(*) FROM src_beneficiary_summary) AS src_count,
    (SELECT COUNT(*) FROM new_beneficiary_summary) AS new_count,
    (SELECT COUNT(*) FROM src_beneficiary_summary) - (SELECT COUNT(*) FROM new_beneficiary_summary) AS diff
UNION ALL
SELECT 
    'carrier_claims',
    (SELECT COUNT(*) FROM src_carrier_claims),
    (SELECT COUNT(*) FROM new_carrier_claims),
    (SELECT COUNT(*) FROM src_carrier_claims) - (SELECT COUNT(*) FROM new_carrier_claims) AS diff;


DROP VIEW IF EXISTS vw_missing_rows;
CREATE VIEW vw_missing_rows as
-- Rows in SRC but missing in NEW
SELECT 'Missing in New' AS status, s.DESYNPUF_ID, s."YEAR"
FROM src_beneficiary_summary s
LEFT JOIN new_beneficiary_summary n 
    ON s.DESYNPUF_ID = n.DESYNPUF_ID AND s."YEAR" = n."YEAR"
WHERE n.DESYNPUF_ID IS NULL
UNION ALL
-- Rows in NEW but missing in SRC
SELECT 'Extra in New' AS status, n.DESYNPUF_ID, n."YEAR"
FROM new_beneficiary_summary n
LEFT JOIN src_beneficiary_summary s 
    ON s.DESYNPUF_ID = n.DESYNPUF_ID AND s."YEAR" = n."YEAR"
WHERE s.DESYNPUF_ID IS NULL;

DROP VIEW IF EXISTS vw_amount_differences;
CREATE VIEW vw_amount_differences as
-- Find claims where the total line payment amount doesn't match
SELECT 
    s.CLM_ID,
    s.LINE_NCH_PMT_AMT_1 AS src_amt,
    n.LINE_NCH_PMT_AMT_1 AS new_amt,
    ABS(s.LINE_NCH_PMT_AMT_1 - n.LINE_NCH_PMT_AMT_1) AS variance
FROM src_carrier_claims s
JOIN new_carrier_claims n ON s.CLM_ID = n.CLM_ID
WHERE s.LINE_NCH_PMT_AMT_1 <> n.LINE_NCH_PMT_AMT_1;

DROP VIEW IF EXISTS vw_not_identical;
CREATE VIEW vw_not_identical as
-- This returns all rows that are NOT identical across all columns
(SELECT * FROM src_beneficiary_summary EXCEPT SELECT * FROM new_beneficiary_summary)
UNION ALL
(SELECT * FROM new_beneficiary_summary EXCEPT SELECT * FROM src_beneficiary_summary);


DROP VIEW IF EXISTS vw_dob_differences;
CREATE VIEW vw_dob_differences as
SELECT s.DESYNPUF_ID, s.BENE_BIRTH_DT as src_dob, n.BENE_BIRTH_DT as new_dob
    FROM src_beneficiary_summary s
    JOIN new_beneficiary_summary n ON s.DESYNPUF_ID = n.DESYNPUF_ID AND s."YEAR" = n."YEAR"
    WHERE s.BENE_BIRTH_DT <> n.BENE_BIRTH_DT;