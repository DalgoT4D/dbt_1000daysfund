-- Model: Stages monthly Satgas member performance and incentive records.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["satgas", "staging", "parent"]
) }}

-- Load raw Satgas supervisor records (all columns are text).
with raw as (

    select * from {{ source('raw_sheets', 'satgas_supervisor') }}

),

-- Normalise text once so the casts below stay readable.
norm as (

    select
        *,

        -- counts: strip filler words ('Bulan ke 4' → '4')
        regexp_replace(lower(coalesce(no, '')),       '(kunjungan|bulan|ke|-|\s)', '', 'g') as no_n,
        regexp_replace(lower(coalesce(bulan_ke, '')), '(kunjungan|bulan|ke|-|\s)', '', 'g') as bulan_ke_n,
        regexp_replace(coalesce(_source_row, ''),     '\s', '', 'g')                        as source_row_n,

        -- Ya / Tidak / Opsional flags (lowercased for matching)
        lower(btrim(coalesce(kriteria_supervisi_posyandu, ''))) as k_posyandu_n,
        lower(btrim(coalesce(kriteria_pembinaan_kader, '')))    as k_pembinaan_n,
        lower(btrim(coalesce(kriteria_supervisi_kr, '')))       as k_kr_n,
        lower(btrim(coalesce(kriteria_feedback_kr, '')))        as k_feedback_n,
        lower(btrim(coalesce(kriteria_meeting_bulanan, '')))    as k_meeting_n,
        lower(btrim(coalesce(kriteria_tindak_lanjut, '')))      as k_tindak_n,
        lower(btrim(coalesce(kriteria_aktif, '')))              as k_aktif_n,

        -- rupiah: drop 'Rp' and spaces ('Rp 150.000' → '150.000')
        regexp_replace(lower(coalesce(insentif_penuh, '')),    '(rp\.?|\s)', '', 'g')       as penuh_n,
        regexp_replace(lower(coalesce(insentif_diterima, '')), '(rp\.?|\s)', '', 'g')       as diterima_n,

        -- percent: comma → dot, drop % and spaces ('84,8 %' → '84.8')
        regexp_replace(replace(coalesce(insentif_persen, ''), ',', '.'), '[\s%]', '', 'g')  as persen_n

    from raw

),

-- Cast fields to analysis-ready types; malformed cells become NULL.
typed as (

    select
        -- origin
        nullif(btrim(source), '')                                   as source,
        nullif(btrim(kota_kabupaten), '')                           as kota_kabupaten,
        case when no_n ~ '^\d+$' then no_n::int end                 as no,

        -- period
        nullif(btrim(bulan), '')                                    as bulan,
        case when bulan_ke_n ~ '^\d+$' then bulan_ke_n::int end     as bulan_ke,

        -- member
        nullif(btrim(satgas_nama), '')                              as satgas_nama,
        nullif(btrim(satgas_profesi), '')                           as satgas_profesi,
        nullif(btrim(satgas_peran), '')                             as satgas_peran,

        -- geography / facility
        nullif(btrim(desa_kelurahan), '')                           as desa_kelurahan,
        nullif(btrim(puskesmas), '')                                as puskesmas,

        -- performance criteria: standardised to 'Ya' / 'Tidak' / 'Opsional';
        -- any other value is kept as trimmed text, blank → NULL
        case
            when k_posyandu_n in ('ya', 'y', 'iya', 'yes')        then 'Ya'
            when k_posyandu_n in ('tidak', 'tdk', 'no', 'n')      then 'Tidak'
            when k_posyandu_n in ('opsional', 'optional', 'ops') then 'Opsional'
            else nullif(btrim(kriteria_supervisi_posyandu), '')
        end                                                         as kriteria_supervisi_posyandu,
        case
            when k_pembinaan_n in ('ya', 'y', 'iya', 'yes')        then 'Ya'
            when k_pembinaan_n in ('tidak', 'tdk', 'no', 'n')      then 'Tidak'
            when k_pembinaan_n in ('opsional', 'optional', 'ops') then 'Opsional'
            else nullif(btrim(kriteria_pembinaan_kader), '')
        end                                                         as kriteria_pembinaan_kader,
        case
            when k_kr_n in ('ya', 'y', 'iya', 'yes')        then 'Ya'
            when k_kr_n in ('tidak', 'tdk', 'no', 'n')      then 'Tidak'
            when k_kr_n in ('opsional', 'optional', 'ops') then 'Opsional'
            else nullif(btrim(kriteria_supervisi_kr), '')
        end                                                         as kriteria_supervisi_kr,
        case
            when k_feedback_n in ('ya', 'y', 'iya', 'yes')        then 'Ya'
            when k_feedback_n in ('tidak', 'tdk', 'no', 'n')      then 'Tidak'
            when k_feedback_n in ('opsional', 'optional', 'ops') then 'Opsional'
            else nullif(btrim(kriteria_feedback_kr), '')
        end                                                         as kriteria_feedback_kr,
        case
            when k_meeting_n in ('ya', 'y', 'iya', 'yes')        then 'Ya'
            when k_meeting_n in ('tidak', 'tdk', 'no', 'n')      then 'Tidak'
            when k_meeting_n in ('opsional', 'optional', 'ops') then 'Opsional'
            else nullif(btrim(kriteria_meeting_bulanan), '')
        end                                                         as kriteria_meeting_bulanan,
        case
            when k_tindak_n in ('ya', 'y', 'iya', 'yes')        then 'Ya'
            when k_tindak_n in ('tidak', 'tdk', 'no', 'n')      then 'Tidak'
            when k_tindak_n in ('opsional', 'optional', 'ops') then 'Opsional'
            else nullif(btrim(kriteria_tindak_lanjut), '')
        end                                                         as kriteria_tindak_lanjut,
        case
            when k_aktif_n in ('ya', 'y', 'iya', 'yes')        then 'Ya'
            when k_aktif_n in ('tidak', 'tdk', 'no', 'n')      then 'Tidak'
            when k_aktif_n in ('opsional', 'optional', 'ops') then 'Opsional'
            else nullif(btrim(kriteria_aktif), '')
        end                                                         as kriteria_aktif,

        -- incentive: '150000', '150,000', '150.000' accepted; blank stays NULL (distinct from 0)
        case when penuh_n    ~ '^(\d{1,3}([.,]\d{3})+|\d+)$' then regexp_replace(penuh_n,    '[.,]', '', 'g')::numeric end as insentif_penuh,
        case when persen_n   ~ '^(\d+(\.\d*)?|\.\d+)$'       then persen_n::numeric end                                     as insentif_persen,
        case when diterima_n ~ '^(\d{1,3}([.,]\d{3})+|\d+)$' then regexp_replace(diterima_n, '[.,]', '', 'g')::numeric end as insentif_diterima,

        nullif(btrim(catatan), '')                                  as catatan,

        -- provenance from the ingestion layer
        nullif(btrim(_source_kabupaten), '')                        as source_file,
        case when source_row_n ~ '^\d+$' then source_row_n::int end as source_row,
        nullif(btrim(_unmapped), '')::jsonb                         as unmapped,
        nullif(btrim(_ingested_at), '')::timestamptz                as loaded_at

    from norm

)

select * from typed