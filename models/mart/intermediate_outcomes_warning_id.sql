{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["mart"]
) }}

WITH clean_data AS (
    SELECT *
    FROM public.intermediate_outcomes
    WHERE year ~ '^[0-9]{4}$'
),

latest AS (
    SELECT MAX(year::integer) AS latest_year
    FROM clean_data
),

kpis AS (

    SELECT
        'ifa' AS indicator,
        ROUND(AVG(ifa)::numeric, 2) AS value,
        0.95 AS target
    FROM clean_data
    WHERE year::integer = (SELECT latest_year FROM latest)

    UNION ALL

    SELECT
        'anc' AS indicator,
        ROUND(AVG(anc)::numeric, 2) AS value,
        0.80 AS target
    FROM clean_data
    WHERE year::integer = (SELECT latest_year FROM latest)

    UNION ALL

    SELECT
        'breastfeed' AS indicator,
        ROUND(AVG(breastfeed)::numeric, 2) AS value,
        0.95 AS target
    FROM clean_data
    WHERE year::integer = (SELECT latest_year FROM latest)

    UNION ALL

    SELECT
        'animal_protein' AS indicator,
        ROUND(AVG(animal_protein)::numeric, 2) AS value,
        0.80 AS target
    FROM clean_data
    WHERE year::integer = (SELECT latest_year FROM latest)

)

SELECT
    indicator,
    value,
    target,

    CASE
        WHEN value < target THEN
            CONCAT(
                '🔴 ',
                ROUND((target - value) * 100, 1),
                '% di bawah target'
            )
        ELSE
            '🟢 Target tercapai'
    END AS kpi_status

FROM kpis
ORDER BY indicator