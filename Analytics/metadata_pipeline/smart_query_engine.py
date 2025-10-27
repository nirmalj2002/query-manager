"""
smart_query_engine.py
---------------------------------------------------------
Optimized querying using DuckDB metadata pruning.
"""

import duckdb
import pandas as pd

DUCKDB_FILE = "lake_metadata.duckdb"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = "YOUR_ACCESS_KEY"
S3_SECRET_KEY = "YOUR_SECRET_KEY"


class SmartQueryEngine:
    def __init__(self, db_file=DUCKDB_FILE):
        self.con = duckdb.connect(db_file)
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute(f"SET s3_region='{S3_REGION}';")
        self.con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
        self.con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")

    def _get_files(self, table, region=None, date_range=None, column_filter=None):
        cond = [f"table_name='{table}'"]
        if region:
            cond.append(f"region='{region}'")
        if date_range:
            cond.append(f"eod_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
        if column_filter:
            col, op, val = column_filter
            cond.append(f"column_name='{col}' AND CAST(statistics_max AS DOUBLE) {op} {val}")
        where = " AND ".join(cond)
        q = f"SELECT DISTINCT file_name FROM parquet_file_metadata WHERE {where}"
        return self.con.execute(q).df()["file_name"].tolist()

    def query(self, table, sql_filter=None, region=None, date_range=None, column_filter=None):
        files = self._get_files(table, region, date_range, column_filter)
        if not files:
            print("⚠️ No matching files found.")
            return pd.DataFrame()
        file_list = "ARRAY" + str(files)
        q = f"SELECT * FROM read_parquet({file_list})"
        if sql_filter:
            q += f" WHERE {sql_filter}"
        print(f"🚀 Querying {len(files)} files for {table}")
        return self.con.execute(q).df()
