WITH base AS (
    SELECT parameter_group,
    SUM(calc_node_raw_hours) AS calc_node_raw_hours,
    SUM(security_count_thousands) AS security_count_thousands
    FROM batch_metrics
    WHERE eod_date = 2025-09-11
    AND region = 'nam'
    AND parameter_group IN ('group_a', 'group_b', 'group_c')
    GROUP BY parameter_group
),
compare AS (
    SELECT parameter_group,
    SUM(calc_node_raw_hours) AS calc_node_raw_hours,
    SUM(security_count_thousands) AS security_count_thousands
    FROM batch_metrics
    WHERE eod_date = 2025-10-14
    AND region = 'nam'
    AND parameter_group IN ('group_a', 'group_b', 'group_c')
    GROUP BY parameter_group
)
SELECT
    a.parameter_group,
    --Raw CPU Hours
    a.calc_node_raw_hours AS baseline_raw_hours,
    b.calc_node_raw_hours AS compare_raw_hours,
    b.calc_node_raw_hours - a.calc_node_raw_hours AS delta_raw_hours,
    100.0 * (b.calc_node_raw_hours - a.calc_node_raw_hours) / NULLIF(a.calc_node_raw_hours, 0) AS pct_change_raw_hours,

    --Security Count 
    a.security_count_thousands AS baseline_sec,
    b.security_count_thousands AS compare_sec,
    b.security_count_thousands - a.security_count_thousands AS delta_sec,
    100.0 * (b.security_count_thousands - a.security_count_thousands) / NULLIF(a.security_count_thousands, 0) AS pct_change_sec
FROM base a
JOIN compare b
AND a.parameter_group = b.parameter_group
ORDER BY a.parameter_group;