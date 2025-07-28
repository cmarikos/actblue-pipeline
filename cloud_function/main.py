from parsons import ActBlue
import os, re
import pandas as pd
from dotenv import load_dotenv
import findspark
findspark.init("/Users/cmarikos/spark-3.5.4-bin-hadoop3")

from pyspark.sql import SparkSession

def clean_col(c: str) -> str:
    c = re.sub(r"[^0-9A-Za-z_]", "_", c).strip("_")
    if c[:1].isdigit():
        c = "_" + c
    return c.lower()

def main(event, context):
    # 1) Env & creds
    load_dotenv(override=True)
    print("✅ Loaded .env")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print(f"🔐 Using credentials from: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")

    # 2) Pull ActBlue
    ab = ActBlue(
        actblue_client_uuid=os.getenv("ACTBLUE_CLIENT_UUID"),
        actblue_client_secret=os.getenv("ACTBLUE_CLIENT_SECRET")
    )
    donations = ab.get_contributions(
        csv_type="paid_contributions",
        date_range_start="2025-01-01",
        date_range_end="2025-06-01"
    )
    rows = donations.to_dicts()
    print(f"📥 Retrieved {len(rows)} donations from ActBlue.")
    print(rows[:1])

    # 3) Pandas clean headers add date
    df_pd = pd.DataFrame(rows)
    df_pd.columns = [clean_col(c) for c in df_pd.columns]
    if "reserved" in df_pd.columns:
        df_pd.rename(columns={"reserved": "reserved_col"}, inplace=True)

    # 4) Spark session (bind to localhost & fixed ports, add BQ connector)
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("actblue_to_bq")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.port", "7077")
        .config("spark.blockManager.port", "7079")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
        .getOrCreate()
    )

    df_spark = spark.createDataFrame(df_pd)

    # 5) Write to BigQuery
    df_spark.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", os.environ["GCS_TEMP_BUCKET"]) \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save("prod-organize-arizon-4e1c0a83.actblue_pipeline.donations")

    print("✅ Wrote to BigQuery via Spark")
    spark.stop()

if __name__ == "__main__":
    # Helpful when run locally
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    main({}, {})
