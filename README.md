"# query-manager" 
# Step 1: Run the loader
!python metadata_auto_loader.py


from smart_query_engine import SmartQueryEngine
engine = SmartQueryEngine()

# Example query: fetch sales >1000 for EMEA region in recent days
df = engine.query(
    table="table1",
    region="emea",
    date_range=("2025-10-24", "2025-10-26"),
    sql_filter="sales_amount > 1000"
)
print(df.head())


import duckdb
from metadata_utils import summarize_metadata
con = duckdb.connect("lake_metadata.duckdb")
print(summarize_metadata(con))
