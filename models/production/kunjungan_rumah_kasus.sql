{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["kobo", "active_kunjungan_rumah_kasus_stg", "production"]
) }}

select
    *
from {{ ref('active_kunjungan_rumah_kasus_stg') }}
