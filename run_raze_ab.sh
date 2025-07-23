#!/bin/zsh
set -euo pipefail

# 1) Go to project
cd /Users/cmarikos/pyspark/razea/actblue-pipeline

# 2) Activate venv
source parsons_env/bin/activate

# 3) Run
python cloud_function/main.py dummy dummy >> logs/actblue.log 2>&1
