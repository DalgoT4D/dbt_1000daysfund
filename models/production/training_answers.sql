{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["training_answers", "staging", "training"]
) }}

select
    *
from {{ ref('training_answers_stg') }}