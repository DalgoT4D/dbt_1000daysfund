{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["active_asesmen_nakes", "staging"]
) }}

select
    *
from {{ ref('asesmen_nakes_active_stg') }}
