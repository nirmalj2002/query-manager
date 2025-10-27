"""
metadata_utils.py
---------------------------------------------------------
Utility helpers for inspecting metadata health and volume.
"""

import duckdb
import pandas as pd

def summarize_metadata(con):
    return con.execute("""
        SELECT table_name, region, eod_date,
               COUNT(DISTINCT file_name) AS num_files,
               SUM(num_values) AS total_rows
        FROM parquet_file_metadata
        GROUP BY table_name, region, eod_date
        ORDER BY eod_date DESC;
    """).df()

def list_tracked_dates(con, table_name):
    df = con.execute(f"SELECT DISTINCT eod_date FROM parquet_file_metadata WHERE table_name='{table_name}' ORDER BY eod_date").df()
    print(df)
