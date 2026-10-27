{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["mart"]
) }}

WITH clean_data AS (
    SELECT
        year::integer AS year,
        indicator,
        numerator,
        denominator
    FROM public.intermediate_outcomes
    WHERE year::text ~ '^[0-9]{4}$'
),

latest AS (
    SELECT MAX(year) AS latest_year
    FROM clean_data
),

targets (indicator, target) AS (
    VALUES
        ('ifa',            0.95::numeric),
        ('anc',            0.80::numeric),
        ('asi_eksklusif',  0.95::numeric),
        ('animal_protein', 0.80::numeric)
),

kpis AS (
    SELECT
        t.indicator,
        ROUND(SUM(c.numerator)::numeric / NULLIF(SUM(c.denominator), 0), 2) AS value,
        t.target
    FROM targets t
    LEFT JOIN clean_data c
        ON c.indicator = t.indicator
       AND c.year = (SELECT latest_year FROM latest)
    GROUP BY t.indicator, t.target
)

SELECT
    indicator,
    value,
    target,
    CASE
        WHEN value IS NULL THEN '⚪ Tidak ada data'
        WHEN value < target THEN
            CONCAT('🔴 ', ROUND((target - value) * 100, 1), '% di bawah target')
        ELSE '🟢 Target tercapai'
    END AS kpi_status
FROM kpis
ORDER BY indicator