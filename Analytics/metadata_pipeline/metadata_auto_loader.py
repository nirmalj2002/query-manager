"""
metadata_auto_loader.py
---------------------------------------------------------
Automatically discovers and incrementally updates Parquet metadata
for all Hive-partitioned tables under:
  s3://app_data/data-assist/hive_data/<table_name>/
Partitions: region=<region>/eod_date=<yyyy-mm-dd>/
Schema is fixed (no drift).
Usage:
    python metadata_auto_loader.py
"""

import duckdb
import pandas as pd
from datetime import datetime

# --- CONFIG ---
S3_BUCKET_ROOT = "s3://app_data/data-assist/hive_data"
DUCKDB_FILE = "lake_metadata.duckdb"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = "YOUR_ACCESS_KEY"
S3_SECRET_KEY = "YOUR_SECRET_KEY"


# --- CONNECTION ---
def connect_duckdb():
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{S3_REGION}';")
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")

    con.execute("""
    CREATE TABLE IF NOT EXISTS parquet_file_metadata (
        table_name TEXT,
        file_name TEXT PRIMARY KEY,
        file_size BIGINT,
        row_group INTEGER,
        column_name TEXT,
        physical_type TEXT,
        logical_type TEXT,
        num_values BIGINT,
        compression TEXT,
        statistics_min TEXT,
        statistics_max TEXT,
        region TEXT,
        eod_date TEXT,
        last_updated TIMESTAMP
    );
    """)
    return con


# --- DISCOVERY HELPERS ---
def discover_tables(con):
    df = con.execute(f"SELECT file FROM glob('{S3_BUCKET_ROOT}/*')").df()
    df["table_name"] = df["file"].str.extract(r"hive_data/([^/]+)")
    return df["table_name"].dropna().unique().tolist()


def discover_s3_eod_dates(con, table):
    path = f"{S3_BUCKET_ROOT}/{table}/region=*/eod_date=*/"
    df = con.execute(f"SELECT file FROM glob('{path}')").df()
    df["eod_date"] = df["file"].str.extract(r"eod_date=(\\d{4}-\\d{2}-\\d{2})")
    return sorted(df["eod_date"].dropna().unique().tolist())


def discover_existing_eod_dates(con, table):
    df = con.execute(f"SELECT DISTINCT eod_date FROM parquet_file_metadata WHERE table_name='{table}'").df()
    return sorted(df["eod_date"].dropna().unique().tolist())


# --- CORE UPDATE ---
def update_metadata_for_table_date(con, table, eod_date):
    path = f"{S3_BUCKET_ROOT}/{table}/region=*/eod_date={eod_date}/*.parquet"
    print(f"📅 Updating {table} for {eod_date} ...")

    files_df = con.execute(f"SELECT file AS file_name, size AS file_size FROM glob('{path}')").df()
    if files_df.empty:
        print("⚠️ No files found.")
        return

    existing = con.execute(
        f"SELECT file_name, file_size FROM parquet_file_metadata WHERE table_name='{table}' AND eod_date='{eod_date}'"
    ).df()

    merged = files_df.merge(existing, on="file_name", how="left", suffixes=("", "_existing"))
    new_files = merged[
        merged["file_size_existing"].isna() | (merged["file_size"] != merged["file_size_existing"])
    ][["file_name", "file_size"]]

    if new_files.empty:
        print("✅ No new or changed files.")
        return

    print(f"🆕 Found {len(new_files)} new/updated files.")
    file_list = ", ".join([f"'{f}'" for f in new_files["file_name"].tolist()])

    meta_query = f"""
        SELECT file_name, file_size, row_group, column_name,
               physical_type, logical_type, num_values, compression,
               statistics_min, statistics_max
        FROM parquet_metadata(ARRAY[{file_list}]);
    """
    meta_df = con.execute(meta_query).df()
    meta_df["table_name"] = table
    meta_df["region"] = meta_df["file_name"].str.extract(r'region=([^/]+)')
    meta_df["eod_date"] = eod_date
    meta_df["last_updated"] = datetime.utcnow()

    con.register("meta_new", meta_df)
    con.execute("INSERT OR REPLACE INTO parquet_file_metadata SELECT * FROM meta_new;")
    print(f"✅ Updated {len(meta_df)} metadata rows for {table} - {eod_date}")


# --- ORCHESTRATOR ---
def auto_update_all_metadata():
    con = connect_duckdb()
    tables = discover_tables(con)
    print(f"📦 Discovered tables: {tables}")

    for table in tables:
        s3_dates = discover_s3_eod_dates(con, table)
        existing_dates = discover_existing_eod_dates(con, table)
        missing_dates = sorted(list(set(s3_dates) - set(existing_dates)))

        if not missing_dates:
            print(f"✅ {table}: Up to date ({len(existing_dates)} dates).")
            continue

        print(f"📈 {table}: Found {len(missing_dates)} new dates → {missing_dates}")
        for eod_date in missing_dates:
            update_metadata_for_table_date(con, table, eod_date)

    print("\n🎯 Metadata fully synchronized!")


if __name__ == "__main__":
    auto_update_all_metadata()
