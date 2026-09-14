-- Model: Combines participant outcomes across all old training cohorts.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["training_old_participants", "staging", "training"]
) }}

select
    nullif(btrim("training_type"), '')        as training_type,
    nullif(btrim("training_code"), '')        as training_code,
    nullif(btrim("year"), '')::int            as year,
    nullif(btrim("quarter"), '')              as quarter,
    nullif(btrim("date"), '')::date           as date,
    nullif(btrim("status"), '')               as status,
    nullif(btrim("nama"), '')                 as nama,
    nullif(btrim("peran_category"), '')       as peran_category,
    nullif(btrim("provinsi"), '')             as provinsi,
    nullif(btrim("kabupaten"), '')            as kota_kabupaten,
    nullif(btrim("kecamatan"), '')            as kecamatan,
    nullif(btrim("desa"), '')                 as desa_kelurahan,
    nullif(btrim("pre_score"), '')::numeric   as pre_score,
    nullif(btrim("post_score"), '')::numeric  as post_score,
    nullif(btrim("delta"), '')::numeric       as delta,
    nullif(btrim("outcome"), '')              as outcome

from {{ source('raw_sheets', 'training_old_participants') }}