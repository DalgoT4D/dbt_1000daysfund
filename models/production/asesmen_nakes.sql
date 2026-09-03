{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["active_asesmen_nakes", "staging"]
) }}

select
    *
from {{ ref('active_asesmen_nakes_stg') }}
