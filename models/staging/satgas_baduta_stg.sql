-- Model: Stages Satgas child (Baduta) home-visit records from all kabupaten.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["satgas", "staging", "parent"]
) }}

-- Load raw child Satgas records (all columns are text).
with raw as (

    select * from {{ source('raw_sheets', 'satgas_baduta') }}

),

-- Normalise text once so the casts below stay readable.
norm as (

    select
        *,

        -- counts: strip filler words ('3 Kunjungan' → '3'; 'Belum kunjungan rumah' won't parse)
        regexp_replace(lower(coalesce(no, '')),               '(kunjungan|bulan|ke|-|\s)', '', 'g') as no_n,
        regexp_replace(lower(coalesce(kunjungan_ke, '')),     '(kunjungan|bulan|ke|-|\s)', '', 'g') as kunjungan_ke_n,
        regexp_replace(lower(coalesce(kunjungan_jumlah, '')), '(kunjungan|bulan|ke|-|\s)', '', 'g') as kunjungan_jumlah_n,
        regexp_replace(coalesce(_source_row, ''),             '\s', '', 'g')                        as source_row_n,

        -- date
        btrim(coalesce(kunjungan_tanggal, ''))                                                      as tanggal_n,

        -- decimals: comma → dot, drop inner spaces ('10. 2' → '10.2')
        regexp_replace(replace(coalesce(baduta_berat_badan, ''),  ',', '.'), '\s+', '', 'g')        as bb_n,
        regexp_replace(replace(coalesce(baduta_tinggi_badan, ''), ',', '.'), '\s+', '', 'g')        as tb_n

    from raw

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
        case when kunjungan_ke_n ~ '^\d+$' then kunjungan_ke_n::int end         as kunjungan_ke,
        case when kunjungan_jumlah_n ~ '^\d+$' then kunjungan_jumlah_n::int end as kunjungan_jumlah,

        -- child & case
        nullif(btrim(baduta_nama), '')                              as baduta_nama,
        nullif(btrim(baduta_pengasuh_nama), '')                     as baduta_pengasuh_nama,
        nullif(btrim(baduta_usia_raw), '')                          as baduta_usia_raw,
        nullif(btrim(kasus_raw), '')                                as kasus_raw,
        nullif(btrim(kasus_status), '')                             as kasus_status,

        -- cadre / satgas (raw lists, split later)
        nullif(btrim(chw_nama_raw), '')                             as chw_nama_raw,
        nullif(btrim(satgas_nama_raw), '')                          as satgas_nama_raw,

        -- geography / facility
        nullif(btrim(desa_kelurahan), '')                           as desa_kelurahan,
        nullif(btrim(puskesmas), '')                                as puskesmas,

        -- measures ('8..0', '8.4 (N)', 'Tidak hadir posyandu' → NULL)
        nullif(btrim(masalah_utama), '')                            as masalah_utama,
        case when bb_n ~ '^(\d+(\.\d*)?|\.\d+)$' then bb_n::numeric end as baduta_berat_badan,
        case when tb_n ~ '^(\d+(\.\d*)?|\.\d+)$' then tb_n::numeric end as baduta_tinggi_badan,

        -- visit content
        nullif(btrim(penyuluhan), '')                               as penyuluhan,
        nullif(btrim(progres), '')                                  as progres,
        nullif(btrim(bukti_supervisi_link), '')                     as bukti_supervisi_link,
        nullif(btrim(foto_kunjungan_link), '')                      as foto_kunjungan_link,
        nullif(btrim(catatan), '')                                  as catatan,

        -- sheet QC notes
        nullif(btrim(qc_note_date), '')                             as qc_note_date,
        nullif(btrim(qc_note_visit), '')                            as qc_note_visit,

        -- provenance from the ingestion layer
        nullif(btrim(_source_kabupaten), '')                        as source_file,
        case when source_row_n ~ '^\d+$' then source_row_n::int end as source_row,
        nullif(btrim(_unmapped), '')::jsonb                         as unmapped,
        nullif(btrim(_ingested_at), '')::timestamptz                as loaded_at

    from norm

)

select * from typed