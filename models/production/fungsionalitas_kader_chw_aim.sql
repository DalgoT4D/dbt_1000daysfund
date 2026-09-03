{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["kobo", "active_fungsionalitas_kader_chw_aim", "production"]
) }}

select
    *
from {{ ref('active_fungsionalitas_kader_chw_aim_stg') }}
