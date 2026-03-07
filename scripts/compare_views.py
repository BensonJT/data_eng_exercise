import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine

def safe_fmt(val):
    if val is None: return '0.00'
    return f'{val:,.2f}'

def run_comparison():
    with engine.connect() as conn:
        print('--- Source System Comparison ---')
        # Filter source bene summary to just 2008-2010 to match claims
        # Actually claims are 2008-2010. Bene summary has rows for 2008, 2009, 2010.
        # Verify correct aggregation.
        
        # Source Sytem
        actual = conn.execute(text('SELECT SUM(MEDREIMB_CAR) FROM src_beneficiary_summary WHERE YEAR BETWEEN 2008 AND 2010')).scalar()
        calc = conn.execute(text('SELECT SUM(CALC_MEDREIMB_CAR) FROM view_calc_src_payments')).scalar()
        
        print(f'Actual Annual Reimbursement: ${safe_fmt(actual)}')
        print(f'Calculated via Formula:    ${safe_fmt(calc)}')
        
        val_actual = actual or 0
        val_calc = calc or 0
        diff = abs(val_actual - val_calc)
        print(f'Difference:                ${safe_fmt(diff)}')
        
        print('\n--- New System Comparison ---')
        actual_new = conn.execute(text('SELECT SUM(MEDREIMB_CAR) FROM new_beneficiary_summary WHERE YEAR BETWEEN 2008 AND 2010')).scalar()
        calc_new = conn.execute(text('SELECT SUM(CALC_MEDREIMB_CAR) FROM view_calc_new_payments')).scalar()
        
        print(f'Actual Annual Reimbursement: ${safe_fmt(actual_new)}')
        print(f'Calculated via Formula:    ${safe_fmt(calc_new)}')
        
        val_actual_new = actual_new or 0
        val_calc_new = calc_new or 0
        diff_new = abs(val_actual_new - val_calc_new)
        print(f'Difference:                ${safe_fmt(diff_new)}')

if __name__ == "__main__":
    run_comparison()
