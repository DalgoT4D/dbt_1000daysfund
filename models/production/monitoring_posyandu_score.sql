-- Model: Combines active and historical Posyandu monitoring scores.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["monitoring_posyandu_score", "staging", "monitoring_posyandu"]
) }}

select
    *
from {{ ref('monitoring_posyandu_score_stg') }}
