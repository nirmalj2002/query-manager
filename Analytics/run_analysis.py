
from Analytics.duckdb_batch_variance import run_variance_analysis

results = run_variance_analysis(
    parquet_root="/data/batch_metrics/",
    region="APAC",
    baseline_date="2025-10-01",
    compare_date="2025-10-08",
    pct_threshold=0.20,
    delta_thresholds={"raw": 10, "cpu": 5, "sec": 10},
    out_dir="./output"
)
