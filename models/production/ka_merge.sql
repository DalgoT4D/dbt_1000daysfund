{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka_merge", "ka"]
) }}

select
    *
from {{ ref('ka_merge_int') }}
