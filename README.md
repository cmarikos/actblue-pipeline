# actblue-pipeline



**(Parsons fetch → Spark load → BigQuery table)**
**
This document explains how we automatically grab donation data from ActBlue, turn it into a table, and put that table into Google BigQuery so we can analyze it. We use three main tools to do this:

* **Parsons** to talk to ActBlue’s API and download the data.
* **Spark** to load that data into BigQuery (because Spark is already set up and approved for us).
* **BigQuery** as the final home for the data.

---

## 0. TL;DR (Quickstart)

```bash
# 1. Clone repo & create .env (see Section 3)
# 2. Create/activate venv & install deps
python -m venv parsons_env && source parsons_env/bin/activate
pip install -r requirements.txt

# 3. Test locally
python cloud_function/main.py dummy dummy

# 4. Schedule hourly
crontab -e    # add the line from Section 7

# 5. Check results
SELECT COUNT(*) FROM `prod-organize-arizon-4e1c0a83.actblue_pipeline.donations`;
```

If you just want to get things running:

1. Make a `.env` file with your secrets (Section 3).
2. Turn on a Python virtual environment and install packages.
3. Run the script once to test it.
4. Use `crontab` to have it run automatically every hour.
5. Look in BigQuery to confirm rows are there.

---

## 1. What This Pipeline Does

1. **Pulls ActBlue contribution data** (CSV API) for a given date range using the Parsons `ActBlue` connector.
2. **Transforms rows to Pandas → Spark DataFrame** locally.
3. **Writes to BigQuery** using the Spark BigQuery connector (with a GCS staging bucket).
4. **Runs hourly via local cron** (for now) to stay fresh.
5. **Logs output** to `logs/actblue.log`.

> We chose Spark for loading to BigQuery because it was already set up and approved (“it was a fight to get with engineering”). Parsons *can* write to BQ, but we’re standardizing on Spark for this part.


* We ask ActBlue for a spreadsheet of donations from specific dates.
* We briefly turn that spreadsheet into data structures Python and Spark understand.
* Then Spark sends it to BigQuery (it uses a temporary Google Cloud Storage bucket behind the scenes).
* A simple computer “alarm clock” (cron) runs this script again every hour.
* Everything the script prints goes into a log file so you can see if it worked.
* We’re using Spark to write to BigQuery mainly because it’s already configured and approved internally; Parsons could do it, but one reliable method is better than mixing both.

---

## 2. High-Level Architecture

```
         ┌──────────────┐
         │   cron (mac) │  hourly
         └─────┬────────┘
               │ runs run_actblue.sh
      ┌────────▼─────────┐
      │  main.py (Python)│
      │  - Parsons fetch │
      │  - Pandas → Spark│
      └────────┬─────────┘
               │ Spark write (GCS temp)
               ▼
       ┌────────────────────────────┐
       │ BigQuery: actblue_pipeline │
       │  table: donations          │
       └────────────────────────────┘
```

Think of it like this:

* Your computer’s scheduler (cron) calls a shell script on a timer.
* That script runs `main.py`, which downloads data and prepares it.
* Spark sends the data to BigQuery.
* BigQuery stores the final table. Done.

---

## 3. Environment & Secrets

Create a `.env` in repo root:

```env
# ActBlue credentials (from ActBlue API portal)
ACTBLUE_CLIENT_UUID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
ACTBLUE_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# GCP credentials (local dev ONLY)
GOOGLE_APPLICATION_CREDENTIALS=/Users/<you>/path/to/secrets/spark-bigquery-sa.json

# BigQuery destination (dataset.table)
BQ_TABLE_ID=actblue_pipeline.donations

# GCS bucket for Spark temp files
GCS_TEMP_BUCKET=cmarikos-sparky-temp
```

**Important:**

* The service account in `GOOGLE_APPLICATION_CREDENTIALS` needs **BigQuery Data Editor** (or better) on the dataset and **Storage Object Admin** on `GCS_TEMP_BUCKET`.
* Do **not** commit your `.env` or JSON key.


A `.env` file is where we safely store passwords and keys so they don’t get committed to GitHub.

* You get your ActBlue keys from your ActBlue account.
* `GOOGLE_APPLICATION_CREDENTIALS` points to a file that lets your script act as your Google project.
* `BQ_TABLE_ID` tells the script where in BigQuery to put the data.
* `GCS_TEMP_BUCKET` is just a temporary place in Google Cloud Storage that Spark uses during upload.

---

## 4. Code Layout

```
actblue-pipeline/
│
├── cloud_function/
│   └── main.py                # The pipeline entrypoint
│
├── secrets/                   # local JSON key (ignored in git)
│
├── logs/
│   └── actblue.log            # cron output (created at runtime)
│
├── run_actblue.sh             # wrapper script for cron
├── requirements.txt
├── .env
└── README.md (this file)
```


This shows where everything lives in the project:

* `main.py` is the script that actually does the work.
* `secrets` stores the private Google key file (but is not checked into git).
* `logs` holds the log file so you can see what happened when cron ran.
* `run_actblue.sh` is the small script cron runs.
* `requirements.txt` lists all the Python packages we need.
* `.env` is your local secrets file.
* `README.md` is this documentation.

---

## 5. main.py (Current Working Version)

```python
from parsons import ActBlue
import os
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import findspark

# Point findspark to your Spark install
findspark.init("/Users/cmarikos/spark-3.5.4-bin-hadoop3")

def main(event, context):
    # Load env
    load_dotenv(override=True)
    print("✅ Loaded .env")

    # GCP creds
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print(f"🔐 Using credentials from: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")

    # Fetch ActBlue
    ab = ActBlue(
        actblue_client_uuid=os.getenv("ACTBLUE_CLIENT_UUID"),
        actblue_client_secret=os.getenv("ACTBLUE_CLIENT_SECRET")
    )

    # NOTE: ActBlue max range is 6 months
    donations = ab.get_contributions(
        csv_type='paid_contributions',
        date_range_start='2025-01-01',
        date_range_end='2025-06-01'
    )

    rows = donations.to_dicts()
    print(f"📥 Retrieved {len(rows)} donations from ActBlue.")
    print(rows[:1])

    # Pandas → Spark
    df_pd = pd.DataFrame(rows)
    spark = (SparkSession.builder
             .config("spark.driver.bindAddress", "127.0.0.1")  # fix port/bind issues on mac
             .config("spark.app.name", "actblue_to_bq")
             .getOrCreate())
    df_spark = spark.createDataFrame(df_pd)

    # Write to BigQuery
    df_spark.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", os.environ["GCS_TEMP_BUCKET"]) \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save("prod-organize-arizon-4e1c0a83.actblue_pipeline.donations")

    print("✅ Wrote to BigQuery via Spark")

if __name__ == "__main__":
    main({}, {})
```


* We load environment variables so the script knows our passwords and target locations.
* We tell Spark where its installation is (`findspark.init`).
* We connect to ActBlue with our keys, ask for donations in a certain timeframe (keep it under 6 months each call).
* We convert the data into Pandas and then Spark format.
* We tell Spark to send it to BigQuery, overwriting whatever was there.
* If everything works, we print a success message.

---

## 6. Local Execution

### 6.1 One-off manual run

```bash
source parsons_env/bin/activate
python cloud_function/main.py dummy dummy
```

### 6.2 Logs

* All stdout/stderr are appended to `logs/actblue.log` in cron runs.
* For quick tailing:

  ```bash
  tail -f logs/actblue.log
  ```


To run once: turn on your virtual environment and run the Python file.
When cron runs it, you won’t see the output in your terminal, so check the log file with `tail -f` to see live updates.

---

## 7. Scheduling (Local cron)

1. **Wrapper script** `run_actblue.sh` (repo root):

```bash
#!/bin/zsh
set -euo pipefail

# 1) Go to project
cd /Users/cmarikos/pyspark/razea/actblue-pipeline

# 2) Activate venv
source parsons_env/bin/activate

# 3) Run
python cloud_function/main.py dummy dummy >> logs/actblue.log 2>&1
```

2. Make executable & create logs dir **once**:

```bash
chmod +x run_actblue.sh
mkdir -p logs
```

3. **Add to crontab** (hourly):

```bash
crontab -e
```

Then paste:

```
0 * * * * /bin/zsh -c '/Users/cmarikos/pyspark/razea/actblue-pipeline/run_actblue.sh'
```

Save & exit. Confirm:

```bash
crontab -l
```


Cron is a built-in scheduler - KNOWN ISSUE currently not running correctly will work on this soon

* The little `run_actblue.sh` script is what cron calls.
* `chmod +x` just makes the script runnable.
* `crontab -e` opens your cron schedule—put in a line that says “run this at the start of every hour.”
* `crontab -l` shows what’s currently scheduled.

---

## 8. BigQuery Side

* **Dataset**: `actblue_pipeline` (must exist).
* **Table**: `donations` (Spark will create/overwrite).
* **Schema**: inferred on write. To enforce schema, define it in Spark or pre-create table.
* Optional: Connect repo to BigQuery Repositories for SQL/Python versioning (see separate doc/slide).

Tell BigQuery where to put the data (dataset + table). If the table doesn’t exist, Spark makes it. If you care about specific column types, make the table first or define schema in Spark. Optional bonus: you can connect your GitHub repo to BigQuery to version your SQL scripts.

---

## 9. Common Errors & Fixes

| Error / Symptom                                                           | Likely Cause                                                   | Fix                                                              |
| ------------------------------------------------------------------------- | -------------------------------------------------------------- | ---------------------------------------------------------------- |
| `DefaultCredentialsError: File … not found`                               | `GOOGLE_APPLICATION_CREDENTIALS` path wrong or .env not loaded | Correct path, call `load_dotenv(override=True)` early            |
| `Date range must be 6 months or less` (422)                               | ActBlue API limit                                              | Split into ≤6‑month windows or run incrementally                 |
| `The destination table has no schema`                                     | Using `insert_rows_json` on empty table                        | Use Spark overwrite or precreate schema                          |
| `Column … not found in schema` / `Field reserved already exists`          | Mismatch / duplicates when Parsons wrote earlier               | Drop/rename columns; let Spark overwrite table cleanly           |
| `ModuleNotFoundError: pandas`                                             | Missing dep in venv                                            | `pip install pandas` or add to requirements.txt                  |
| `Service 'sparkDriver' failed to bind` / `BlockManagerId` NPE             | Spark can’t bind to port / wrong bind address                  | Set `spark.driver.bindAddress=127.0.0.1` in SparkSession builder |
| `Not found: Dataset …`                                                    | Dataset name wrong or service account missing perms            | Verify dataset exists; grant BigQuery permissions                |
| `Dataform’s default service account cannot access secret` (BigQuery repo) | Secret Manager IAM missing                                     | Add `Secret Manager Secret Accessor` to Dataform SA              |

This table is your quick “why is this breaking?” cheat sheet. Match your error, see the likely cause, and try the suggested fix. 90% of the pain points we hit are listed here.

---

## 10. Monitoring & Maintenance

* **Logs**: Review `logs/actblue.log` for failures.
* **Alerting**: (Optional) wrap cron script with `if ! …; then mail -s "ActBlue job failed" you@org.org; fi`.
* **Data freshness check**: Add a daily query in Looker Studio or BigQuery scheduled query to confirm new rows.
* **Key rotation**: Document how/when to rotate ActBlue & GCP keys; update `.env` secrets accordingly.

Keep an eye on it! Check the log file now and then. If you want an email when it fails, you can add an if/then to the cron script. You might also build a dashboard or scheduled check to make sure yesterday’s data arrived. Remember to update keys when they expire.

---

## 11. Data Dictionary (example snippet)

| field\_name  | type      | source      | description                 | example                           |
| ------------ | --------- | ----------- | --------------------------- | --------------------------------- |
| receipt\_id  | STRING    | ActBlue CSV | Unique receipt per donation | AB266200948                       |
| date         | TIMESTAMP | ActBlue CSV | Donation datetime           | 2025-01-06 03:20:00               |
| amount       | NUMERIC   | ActBlue CSV | Contribution amount (USD)   | 2.00                              |
| donor\_email | STRING    | ActBlue CSV | Contributor’s email         | [foo@bar.com](mailto:foo@bar.com) |
| …            | …         | …           | …                           | …                                 |

Maintain this in a Sheet or Markdown for clarity. (Original CSV headers have spaces; Spark lowers none unless we do. Consider normalizing column names before write.)

This is a simple table that explains what each column means. It helps future you (or a teammate) understand what `receipt_id` or `donor_email` is. You can keep this as a spreadsheet or markdown file. If you want cleaner column names, use pandas to rename them before writing to BigQuery.

---

## 12. Change Management & Versioning

* **Code changes**: PRs on GitHub (`main` branch) + tag releases.
* **Schema changes**: Document in README + bump a version file (`schema_version.txt`).
* **Cron cadence changes**: Update `crontab -e`, commit note to repo, and notify team.

**For future improvement:**
When you change the code, use GitHub pull requests so others can review. If you change the table structure, write it down and maybe keep a version number. If you change how often the job runs, tell people and note it somewhere.

---

## 13. Future Improvements

* Move scheduling to **Cloud Scheduler + Cloud Functions/Run** (no local cron dependency).
* Parameterize date ranges (run “yesterday only”).
* Add retry/backoff and Slack alerts on failure.
* Consider using Parsons to push to BQ if Spark complicates local env (but keep one consistent method).
* Add unit tests for transform logic (pytests for date parsing, schema normalization).


