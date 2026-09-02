-- Model: Combines participant outcomes across all training cohorts.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["training_data_participants", "staging", "training"]
) }}

select
    *
from {{ ref('training_data_stg') }}