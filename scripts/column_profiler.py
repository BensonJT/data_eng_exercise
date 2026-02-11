import duckdb
import os

# --- WSL PATH CONVERSION ---
DB_PATH = '/mnt/e/Data Eng Exercise/data_eng.duckdb'

if not os.path.exists(DB_PATH):
    print(f"❌ CRITICAL ERROR: Database not found at: {DB_PATH}")
    print("Check if your E: drive is mounted. Run: ls /mnt/e/")
    exit()

con = duckdb.connect(DB_PATH)

def analyze_migration_full(table_base_name, join_keys):
    src_table = f"src_{table_base_name}"
    new_table = f"new_{table_base_name}"
    
    print(f"\n🚀 Analyzing {table_base_name}...")

    # 1. ORPHAN CHECK & REVERSE ORPHAN CHECK
    join_clause = " AND ".join([f's."{k}" = n."{k}"' for k in join_keys])
    
    # Rows in SRC but missing in NEW (Orphans)
    orphan_query = f"""
        CREATE OR REPLACE TABLE {table_base_name}_orphans AS
        SELECT s.* FROM "{src_table}" s
        LEFT JOIN "{new_table}" n ON {join_clause}
        WHERE n."{join_keys[0]}" IS NULL
    """
    con.execute(orphan_query)
    orphan_count = con.execute(f"SELECT COUNT(*) FROM {table_base_name}_orphans").fetchone()[0]

    # Rows in NEW but missing in SRC (Extra rows)
    extra_query = f"""
        CREATE OR REPLACE TABLE {table_base_name}_extra_rows AS
        SELECT n.* FROM "{new_table}" n
        LEFT JOIN "{src_table}" s ON {join_clause}
        WHERE s."{join_keys[0]}" IS NULL
    """
    con.execute(extra_query)
    extra_count = con.execute(f"SELECT COUNT(*) FROM {table_base_name}_extra_rows").fetchone()[0]
    
    # 2. Get Total Rows for Percentage Calculation
    total_rows_src = con.execute(f'SELECT COUNT(*) FROM "{src_table}"').fetchone()[0]
    total_rows_new = con.execute(f'SELECT COUNT(*) FROM "{new_table}"').fetchone()[0]
    
    # FIXED: Use Python's built-in round() for float objects
    orphan_rate = round((orphan_count / total_rows_src * 100), 4) if total_rows_src > 0 else 0
    
    # 3. Get Column List (Excluding Keys)
    excluded_keys = []
    for k in join_keys:
        excluded_keys.extend([f"'{k.upper()}'", f"'{k.lower()}'"])

    cols_query = f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = '{src_table.lower()}' 
        AND column_name NOT IN ({','.join(excluded_keys)})
    """
    columns = [row[0] for row in con.execute(cols_query).fetchall()]
    
    # 4. Calculate Data Mismatches
    case_parts = [f'SUM(CASE WHEN s."{c}" IS DISTINCT FROM n."{c}" THEN 1 ELSE 0 END) AS "{c}"' for c in columns]
    
    # We only compare records that exist in BOTH (inner join)
    check_query = f"""
        SELECT {', '.join(case_parts)} 
        FROM "{src_table}" s 
        JOIN "{new_table}" n ON {join_clause}
    """
    
    # TRANSFORMING THE DATAFRAME TO INCLUDE FIELD NAMES
    summary_df = con.execute(check_query).df().T
    summary_df.columns = ['error_count']
    
    # Resetting index to turn the field names into a column
    summary_df = summary_df.reset_index().rename(columns={'index': 'field_name'})
    
    summary_df['error_pct'] = (summary_df['error_count'] / total_rows_src * 100).round(2)
    error_summary = summary_df[summary_df['error_count'] > 0].sort_values('error_count', ascending=False)

    # 5. Persist and Create View
    if not error_summary.empty:
        con.execute(f"CREATE OR REPLACE TABLE {table_base_name}_mismatch_summary AS SELECT * FROM error_summary")
        
        error_cols = error_summary['field_name'].tolist() 
        select_list = [f's."{k}"' for k in join_keys]
        for col in error_cols:
            select_list.append(f's."{col}" AS "{col}_src"')
            select_list.append(f'n."{col}" AS "{col}_new"')

        where_clause = " OR ".join([f's."{c}" IS DISTINCT FROM n."{c}"' for c in error_cols])
        con.execute(f"""
            CREATE OR REPLACE VIEW v_{table_base_name}_mismatches AS
            SELECT {', '.join(select_list)}
            FROM "{src_table}" s
            JOIN "{new_table}" n ON {join_clause}
            WHERE {where_clause}
        """)

    # --- PRINT RESULTS ---
    print(f"📊 Total Records (Source): {total_rows_src} | Total Records (New): {total_rows_new}")
    print(f"🚨 Orphans (Missing): {orphan_count} ({orphan_rate}%)")
    print(f"➕ Extra Rows (In New): {extra_count}")
    if not error_summary.empty:
        print(f"❌ Mismatched Columns: {len(error_summary)}")
        print(error_summary[['field_name', 'error_count', 'error_pct']].head(10)) 
    else:
        print("✅ No data value mismatches found for existing records.")

# --- PERSIST GLOBAL VIEWS ---
def create_global_reporting_views():
    print("\n📝 Creating Global Reporting Views...")
    
    con.execute("""
        CREATE OR REPLACE VIEW vw_source_counts AS
        SELECT 'beneficiary_summary' AS table_name, 
               (SELECT COUNT(*) FROM src_beneficiary_summary) AS src_count, 
               (SELECT COUNT(*) FROM new_beneficiary_summary) AS new_count,
               (SELECT COUNT(*) FROM src_beneficiary_summary) - (SELECT COUNT(*) FROM new_beneficiary_summary) AS diff
        UNION ALL
        SELECT 'carrier_claims', (SELECT COUNT(*) FROM src_carrier_claims), (SELECT COUNT(*) FROM new_carrier_claims),
               (SELECT COUNT(*) FROM src_carrier_claims) - (SELECT COUNT(*) FROM new_carrier_claims) AS diff;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW vw_missing_rows AS
        SELECT 'Missing in New' AS status, DESYNPUF_ID, "YEAR" FROM beneficiary_summary_orphans
        UNION ALL
        SELECT 'Extra in New' AS status, DESYNPUF_ID, "YEAR" FROM beneficiary_summary_extra_rows;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW vw_amount_differences AS
        SELECT s.CLM_ID, s.LINE_NCH_PMT_AMT_1 AS src_amt, n.LINE_NCH_PMT_AMT_1 AS new_amt, 
               ABS(s.LINE_NCH_PMT_AMT_1 - n.LINE_NCH_PMT_AMT_1) AS variance
        FROM src_carrier_claims s JOIN new_carrier_claims n ON s.CLM_ID = n.CLM_ID
        WHERE s.LINE_NCH_PMT_AMT_1 <> n.LINE_NCH_PMT_AMT_1;
    """)

# --- RUN EXECUTION ---
analyze_migration_full("beneficiary_summary", ["DESYNPUF_ID", "YEAR"])
analyze_migration_full("carrier_claims", ["CLM_ID"])
create_global_reporting_views()
print("\n✅ All migration analysis tables and views are ready in DuckDB.")