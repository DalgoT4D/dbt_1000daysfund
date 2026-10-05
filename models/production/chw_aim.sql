{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["kobo", "chw_aim", "production"]
) }}

select
    *
from {{ ref('chw_aim_active_stg') }}
