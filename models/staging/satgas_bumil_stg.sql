-- Model: Stages Satgas maternal (Ibu Hamil) home-visit records from all kabupaten.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["satgas", "staging", "parent"]
) }}

-- Plausibility bounds for blood pressure (mmHg). Parsed readings outside these,
-- or with systolic <= diastolic, are nulled and flagged 'implausible'.
{% set td_sys_min, td_sys_max = 60, 260 %}
{% set td_dia_min, td_dia_max = 30, 160 %}

-- Load raw maternal Satgas records (all columns are text).
with raw as (

    select * from {{ source('raw_sheets', 'satgas_bumil') }}

),

-- Normalise text once so the casts below stay readable.
norm as (

    select
        *,

        -- counts: strip filler words ('Kunjungan ke-2' → '2')
        regexp_replace(lower(coalesce(no, '')),           '(kunjungan|bulan|ke|-|\s)', '', 'g') as no_n,
        regexp_replace(lower(coalesce(kunjungan_ke, '')), '(kunjungan|bulan|ke|-|\s)', '', 'g') as kunjungan_ke_n,
        regexp_replace(coalesce(_source_row, ''),         '\s', '', 'g')                        as source_row_n,

        -- date
        btrim(coalesce(kunjungan_tanggal, ''))                                                  as tanggal_n,

        -- decimals: comma → dot, drop inner spaces ('10. 2' → '10.2')
        regexp_replace(replace(coalesce(ibu_berat_badan, ''), ',', '.'), '\s+', '', 'g')        as bb_n,
        regexp_replace(replace(coalesce(ibu_lila, ''),        ',', '.'), '\s+', '', 'g')        as lila_n,

        -- blood pressure: lowercase → drop 'mmHg' in any spacing → backticks/quotes/
        -- newlines/tabs to space → letter 'o' to zero ('120/7o') → trim
        btrim(
            translate(
                regexp_replace(
                    regexp_replace(lower(coalesce(ibu_tekanan_darah_raw, '')), 'm\s*m\s*/?\s*h\s*g', ' ', 'g'),
                    '[`"\r\n\t]', ' ', 'g'
                ),
                'o', '0'
            )
        )                                                                                       as td_n

    from raw

),

-- Split blood pressure. Accepts 2–3 digit pairs separated by / ' \ | , ; : -
-- (optional spaces) or whitespace alone: '100/60', '100'60', '86,56', '110 - 70', '120 80'.
td_parsed as (

    select
        *,
        regexp_match(td_n, '^(\d{2,3})(?:\s*[/''\\|,;:-]+\s*|\s+)(\d{2,3})$') as td_match

    from norm

),

-- Cast fields to analysis-ready types; malformed cells become NULL.
typed as (

    select
        -- origin
        nullif(btrim(source), '')                                   as source,
        nullif(btrim(kota_kabupaten), '')                           as kota_kabupaten,
        case when no_n ~ '^\d+$' then no_n::int end                 as no,

        -- visit
        nullif(btrim(kunjungan_bulan), '')                          as kunjungan_bulan,
        -- ISO date only; nested CASE so impossible days (2026-02-30) return NULL
        case
            when tanggal_n ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'
            then case
                when substr(tanggal_n, 9, 2)::int
                     <= extract(day from (date_trunc('month', (substr(tanggal_n, 1, 7) || '-01')::date)
                                          + interval '1 month - 1 day'))
                then tanggal_n::date
            end
        end                                                         as kunjungan_tanggal,
        case when kunjungan_ke_n ~ '^\d+$' then kunjungan_ke_n::int end as kunjungan_ke,

        -- mother & case
        nullif(btrim(ibu_nama), '')                                 as ibu_nama,
        nullif(btrim(ibu_suami_nama), '')                           as ibu_suami_nama,
        nullif(btrim(kasus_raw), '')                                as kasus_raw,
        nullif(btrim(kasus_status), '')                             as kasus_status,

        -- cadre / satgas (raw lists, split later)
        nullif(btrim(chw_nama_raw), '')                             as chw_nama_raw,
        nullif(btrim(satgas_nama_raw), '')                          as satgas_nama_raw,

        -- geography / facility
        nullif(btrim(desa_kelurahan), '')                           as desa_kelurahan,
        nullif(btrim(puskesmas), '')                                as puskesmas,
        nullif(btrim(posyandu), '')                                 as posyandu,

        -- measures ('8..0', '8.4 (N)', '34.8/148' → NULL)
        nullif(btrim(masalah_utama), '')                            as masalah_utama,
        case when bb_n   ~ '^(\d+(\.\d*)?|\.\d+)$' then bb_n::numeric   end as ibu_berat_badan,
        case when lila_n ~ '^(\d+(\.\d*)?|\.\d+)$' then lila_n::numeric end as ibu_lila,

        -- blood pressure
        nullif(btrim(ibu_tekanan_darah_raw), '')                    as ibu_tekanan_darah_raw,
        case
            when td_match[1]::int between {{ td_sys_min }} and {{ td_sys_max }}
             and td_match[2]::int between {{ td_dia_min }} and {{ td_dia_max }}
             and td_match[1]::int > td_match[2]::int
            then td_match[1]::int
        end                                                         as ibu_tekanan_darah_sistolik,
        case
            when td_match[1]::int between {{ td_sys_min }} and {{ td_sys_max }}
             and td_match[2]::int between {{ td_dia_min }} and {{ td_dia_max }}
             and td_match[1]::int > td_match[2]::int
            then td_match[2]::int
        end                                                         as ibu_tekanan_darah_diastolik,
        case
            when td_n = ''             then 'missing'
            when td_match is null      then 'unparseable'
            when td_match[1]::int between {{ td_sys_min }} and {{ td_sys_max }}
             and td_match[2]::int between {{ td_dia_min }} and {{ td_dia_max }}
             and td_match[1]::int > td_match[2]::int
                                       then 'ok'
            else 'implausible'
        end                                                         as ibu_tekanan_darah_flag,

        -- visit content
        nullif(btrim(penyuluhan), '')                               as penyuluhan,
        nullif(btrim(bukti_supervisi_link), '')                     as bukti_supervisi_link,
        nullif(btrim(foto_kunjungan_link), '')                      as foto_kunjungan_link,
        nullif(btrim(ibu_usia_kehamilan_raw), '')                   as ibu_usia_kehamilan_raw,
        nullif(btrim(catatan_1), '')                                as catatan_1,
        nullif(btrim(catatan_2), '')                                as catatan_2,

        -- sheet QC notes
        nullif(btrim(qc_note_date), '')                             as qc_note_date,
        nullif(btrim(qc_note_visit), '')                            as qc_note_visit,

        -- provenance from the ingestion layer
        nullif(btrim(_source_kabupaten), '')                        as source_file,
        case when source_row_n ~ '^\d+$' then source_row_n::int end as source_row,
        nullif(btrim(_unmapped), '')::jsonb                         as unmapped,
        nullif(btrim(_ingested_at), '')::timestamptz                as loaded_at

    from td_parsed

)

select * from typed