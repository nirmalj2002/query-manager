"""
duckdb_batch_variance.py — FINAL VERSION (3 charts, weighted % aggregation)

Analyzes variance between two EOD snapshots for batch metrics stored in Parquet
using a Hive-style structure:

    /data/batch_metrics/
        region=REGION/
            eod-date=YYYY-MM-DD/
                part-0001.parquet

Outputs:
    - CSVs of flagged variance results & summaries
    - 3 PNG charts:
        1. weighted_param_variance.png
        2. top5_param_groups_weighted.png
        3. top5_models_cpu_variance.png
"""

import os
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


def run_variance_analysis(
    parquet_root: str,
    region: str,
    baseline_date: str,
    compare_date: str,
    pct_threshold: float = 0.20,
    delta_thresholds: dict = None,
    out_dir: str = "./out"
):
    """
    Compare two EOD snapshots for a given region and generate variance results.
    """
    if delta_thresholds is None:
        delta_thresholds = {"raw": 10.0, "cpu": 5.0, "sec": 10.0}

    os.makedirs(out_dir, exist_ok=True)
    con = duckdb.connect(database=':memory:')

    # 1️⃣ Load Parquet files with Hive-style partitions (region → eod-date)
    parquet_glob = os.path.join(parquet_root, "**", "*.parquet")
    con.execute(f"""
        CREATE VIEW batch_metrics_parquet AS
        SELECT
            *,
            regexp_extract(filename, 'region=([^/]+)', 1) AS region,
            regexp_extract(filename, 'eod-date=([0-9-]+)', 1)::DATE AS eod_date
        FROM parquet_scan('{parquet_glob}', recursive=true)
    """)

    region_filter = f"AND region = '{region}'" if region else ""

    # 2️⃣ Main SQL — join baseline and compare, compute deltas & % change
    sql = f"""
    WITH
    params AS (
      SELECT
        '{region}'::VARCHAR AS region,
        DATE '{baseline_date}' AS baseline_date,
        DATE '{compare_date}' AS compare_date,
        {pct_threshold} AS pct_threshold,
        {delta_thresholds['raw']} AS raw_delta_threshold,
        {delta_thresholds['cpu']} AS cpu_delta_threshold,
        {delta_thresholds['sec']} AS sec_delta_threshold
    ),
    agg_base AS (
      SELECT region, eod_date, parameter_group, instance_name, model_name,
             SUM(calc_node_raw_hours) AS base_raw_hours,
             SUM(model_cpu_hours) AS base_model_cpu_hours,
             SUM(security_count_thousands) AS base_security_thousands
      FROM batch_metrics_parquet bm, params p
      WHERE bm.eod_date = p.baseline_date {region_filter}
      GROUP BY region, eod_date, parameter_group, instance_name, model_name
    ),
    agg_comp AS (
      SELECT region, eod_date, parameter_group, instance_name, model_name,
             SUM(calc_node_raw_hours) AS comp_raw_hours,
             SUM(model_cpu_hours) AS comp_model_cpu_hours,
             SUM(security_count_thousands) AS comp_security_thousands
      FROM batch_metrics_parquet bm, params p
      WHERE bm.eod_date = p.compare_date {region_filter}
      GROUP BY region, eod_date, parameter_group, instance_name, model_name
    ),
    joined AS (
      SELECT COALESCE(b.region, c.region) AS region,
             COALESCE(b.parameter_group, c.parameter_group) AS parameter_group,
             COALESCE(b.instance_name, c.instance_name) AS instance_name,
             COALESCE(b.model_name, c.model_name) AS model_name,
             COALESCE(b.base_raw_hours, 0.0) AS base_raw_hours,
             COALESCE(c.comp_raw_hours, 0.0) AS comp_raw_hours,
             COALESCE(b.base_model_cpu_hours, 0.0) AS base_model_cpu_hours,
             COALESCE(c.comp_model_cpu_hours, 0.0) AS comp_model_cpu_hours,
             COALESCE(b.base_security_thousands, 0.0) AS base_security_thousands,
             COALESCE(c.comp_security_thousands, 0.0) AS comp_security_thousands
      FROM agg_base b FULL OUTER JOIN agg_comp c
        ON b.parameter_group=c.parameter_group AND b.instance_name=c.instance_name AND b.model_name=c.model_name
    )
    SELECT *,
           (comp_raw_hours - base_raw_hours) AS delta_raw_hours,
           (comp_model_cpu_hours - base_model_cpu_hours) AS delta_model_cpu_hours,
           (comp_security_thousands - base_security_thousands) AS delta_security_thousands,
           CASE WHEN base_raw_hours=0 THEN NULL ELSE (comp_raw_hours-base_raw_hours)/base_raw_hours END AS pct_raw_hours,
           CASE WHEN base_model_cpu_hours=0 THEN NULL ELSE (comp_model_cpu_hours-base_model_cpu_hours)/base_model_cpu_hours END AS pct_model_cpu_hours,
           CASE WHEN base_security_thousands=0 THEN NULL ELSE (comp_security_thousands-base_security_thousands)/base_security_thousands END AS pct_security_thousands
    FROM joined;
    """

    df = con.execute(sql).df()
    con.close()
    if df.empty:
        print("No records found.")
        return {}

    # 3️⃣ Flag significant variances
    df["raw_var_flag"] = (abs(df["pct_raw_hours"]) > pct_threshold) | (abs(df["delta_raw_hours"]) > delta_thresholds["raw"])
    df["cpu_var_flag"] = (abs(df["pct_model_cpu_hours"]) > pct_threshold) | (abs(df["delta_model_cpu_hours"]) > delta_thresholds["cpu"])
    df["sec_var_flag"] = (abs(df["pct_security_thousands"]) > pct_threshold) | (abs(df["delta_security_thousands"]) > delta_thresholds["sec"])
    df["any_metric_flag"] = df[["raw_var_flag", "cpu_var_flag", "sec_var_flag"]].any(axis=1)

    flagged = df[df["any_metric_flag"]].copy()
    flagged[["pct_raw_hours", "pct_model_cpu_hours", "pct_security_thousands"]] *= 100

    # 4️⃣ Weighted % change aggregation
    def weighted_pct(g, base_col, delta_col):
        base_sum = g[base_col].sum()
        return (g[delta_col].sum() / base_sum * 100) if base_sum != 0 else np.nan

    param_var = (
        flagged.groupby("parameter_group")
        .apply(lambda g: pd.Series({
            "weighted_pct_raw": weighted_pct(g, "base_raw_hours", "delta_raw_hours"),
            "weighted_pct_cpu": weighted_pct(g, "base_model_cpu_hours", "delta_model_cpu_hours"),
            "weighted_pct_sec": weighted_pct(g, "base_security_thousands", "delta_security_thousands")
        }))
        .reset_index()
    )
    param_var["total_weighted_abs"] = (
        param_var[["weighted_pct_raw", "weighted_pct_cpu", "weighted_pct_sec"]]
        .abs().sum(axis=1)
    )
    param_var = param_var.sort_values("total_weighted_abs", ascending=False).head(5)

    # 5️⃣ Weighted Variance Chart (green/red)
    labels = param_var["parameter_group"].tolist()
    x = np.arange(len(labels))
    width = 0.25
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, col in enumerate(["weighted_pct_raw", "weighted_pct_cpu", "weighted_pct_sec"]):
        bars = ax.bar(x + (i - 1) * width, param_var[col], width,
                      label=col.replace("weighted_pct_", "").upper())
        for bar in bars:
            bar.set_color("green" if bar.get_height() >= 0 else "red")
    ax.axhline(pct_threshold * 100, color="red", linestyle="--")
    ax.axhline(-pct_threshold * 100, color="red", linestyle="--")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45)
    ax.set_ylabel("Weighted % Change")
    ax.set_title("Weighted % Variance by Parameter Group (green=↑ red=↓)")
    ax.legend()
    plt.tight_layout()
    chart1_path = os.path.join(out_dir, "weighted_param_variance.png")
    plt.savefig(chart1_path, dpi=150)
    plt.close(fig)

    # 6️⃣ Top 5 Parameter Groups (stacked absolute weighted %)
    fig2, ax2 = plt.subplots(figsize=(8, 5))
    ax2.bar(param_var["parameter_group"], param_var["weighted_pct_raw"].abs(), label="Raw")
    ax2.bar(param_var["parameter_group"], param_var["weighted_pct_cpu"].abs(),
            bottom=param_var["weighted_pct_raw"].abs(), label="CPU")
    ax2.bar(param_var["parameter_group"], param_var["weighted_pct_sec"].abs(),
            bottom=param_var["weighted_pct_raw"].abs() + param_var["weighted_pct_cpu"].abs(), label="Security")
    ax2.set_ylabel("Absolute Weighted % Change")
    ax2.set_title("Top 5 Parameter Groups by Total Weighted Variance")
    ax2.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    chart2_path = os.path.join(out_dir, "top5_param_groups_weighted.png")
    plt.savefig(chart2_path, dpi=150)
    plt.close(fig2)

    # 7️⃣ Top Models by CPU Variance
    model_var = (
        flagged.groupby("model_name")[["pct_model_cpu_hours", "delta_model_cpu_hours"]]
        .agg({"pct_model_cpu_hours": "mean", "delta_model_cpu_hours": "sum"})
        .abs()
        .sort_values("pct_model_cpu_hours", ascending=False)
        .head(5)
    )
    if not model_var.empty:
        fig3, ax3 = plt.subplots(figsize=(8, 5))
        y = np.arange(len(model_var))
        ax3.barh(y, model_var["delta_model_cpu_hours"], color="#F37021", label="Δ CPU Hours")
        ax3.barh(y - 0.2, model_var["pct_model_cpu_hours"], height=0.4, color="#FFD166", label="% CPU Change")
        ax3.set_yticks(y)
        ax3.set_yticklabels(model_var.index)
        ax3.set_xlabel("Absolute Change / %")
        ax3.set_title("Top Models by CPU Variance")
        ax3.legend()
        plt.tight_layout()
        chart3_path = os.path.join(out_dir, "top5_models_cpu_variance.png")
        plt.savefig(chart3_path, dpi=150)
        plt.close(fig3)
    else:
        chart3_path = None

    # 8️⃣ Export CSVs
    flagged.to_csv(os.path.join(out_dir, "flagged_variances.csv"), index=False)
    param_var.to_csv(os.path.join(out_dir, "top5_param_groups_weighted.csv"), index=False)
    if not model_var.empty:
        model_var.to_csv(os.path.join(out_dir, "top5_models_cpu_variance.csv"))

    print(f"\n✅ Analysis complete! Charts saved to {out_dir}")
    print(f" - {chart1_path}\n - {chart2_path}\n - {chart3_path}")

    return {
        "flagged": flagged,
        "param_var": param_var,
        "model_var": model_var,
        "charts": {
            "weighted_param_variance": chart1_path,
            "top5_param_groups_weighted": chart2_path,
            "top5_models_cpu_variance": chart3_path,
        }
    }
