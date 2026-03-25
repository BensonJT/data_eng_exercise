#!/bin/bash
# Launch the Data Migration Quality Dashboard
# Usage: ./run_dashboard.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Initialize conda for this shell session
source /home/bensonjt/anaconda3/etc/profile.d/conda.sh

conda activate data_eng

echo "Launching dashboard at http://localhost:8501 ..."
streamlit run "$SCRIPT_DIR/dashboard.py"
