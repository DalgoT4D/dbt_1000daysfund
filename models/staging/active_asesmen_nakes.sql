{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["active_asesmen_nakes", "staging"]
) }}

-- Form Asesmen Nakes (Kobo aGxRpg2GCsqoVqGW6eYphS).
-- Two assessment blocks, both scored on opsi_skor 1-3:
--   A. Rencana Pembelajaran        10 items over 5 subgroups
--   B. Sikap dan Perilaku          11 items over 3 subgroups
-- Three form versions exist and they do NOT carry the same blocks:
--   vDoLk6ERwpsUJsZbpbmKSR  A + B
--   vLah3dNY4vLyqPaqw6YELw  A only
--   vwYmrs2KNC29MG3mzuwFr9  B only
-- So a null group score is expected, not a defect. Blocks are answered all or
-- nothing -- across 147 submissions the only shapes seen are (10 A, 11 B),
-- (10 A, 0 B) and (0 A, 11 B), never a partial block -- so a null group_*_perc
-- means the version did not ask that block. Caveat: if an item is ever renamed
-- upstream, the denominators below shrink silently rather than erroring.

with source_data as (

    select
        case
            when data is null or trim(data::text) = '' then null::jsonb
            else data::jsonb
        end as json_payload
    from {{ source('raw_kobo', 'ACTIVEForm_Asesmen_Nakes') }}

),

typed_data as (

    select
        nullif(json_payload ->> '_id', '')::bigint                       as submission_id,
        nullif(json_payload ->> '_uuid', '')                             as submission_uuid,
        nullif(json_payload ->> 'meta/rootUuid', '')                     as meta_root_uuid,
        nullif(json_payload ->> '__version__', '')                       as form_version,
        nullif(json_payload ->> '_submission_time', '')::timestamptz     as submission_time,
        nullif(json_payload ->> 'start', '')::timestamptz                as submission_start_at,
        nullif(json_payload ->> 'end', '')::timestamptz                  as submission_end_at,

        -- pembukaan
        nullif(btrim(json_payload ->> 'pembukaan/peserta_nama'), '')     as peserta_nama,
        nullif(json_payload ->> 'pembukaan/tanggal', '')::date           as asesmen_tanggal,
        nullif(btrim(json_payload ->> 'pembukaan/penilai_nama'), '')     as penilai_nama_kode,
        nullif(btrim(json_payload ->> 'pembukaan/penilai_nama_lain'), '') as penilai_nama_lain,
        nullif(btrim(json_payload ->> 'pembukaan/provinsi'), '')         as provinsi_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kota_kabupaten'), '')   as kota_kabupaten_kode,
        nullif(btrim(json_payload ->> 'pembukaan/puskesmas'), '')        as puskesmas_kode,
        nullif(btrim(json_payload ->> 'pembukaan/puskesmas_lain'), '')   as puskesmas_lain,

        -- penutup
        nullif(btrim(json_payload ->> 'penutup/catatan'), '')            as catatan,

        -- A. Rencana Pembelajaran
        nullif(json_payload ->> 'group_a/group_a_1/group_a_1_1', '')::smallint as a_1_1,
        nullif(json_payload ->> 'group_a/group_a_2/group_a_2_1', '')::smallint as a_2_1,
        nullif(json_payload ->> 'group_a/group_a_2/group_a_2_2', '')::smallint as a_2_2,
        nullif(json_payload ->> 'group_a/group_a_3/group_a_3_1', '')::smallint as a_3_1,
        nullif(json_payload ->> 'group_a/group_a_4/group_a_4_1', '')::smallint as a_4_1,
        nullif(json_payload ->> 'group_a/group_a_4/group_a_4_2', '')::smallint as a_4_2,
        nullif(json_payload ->> 'group_a/group_a_4/group_a_4_3', '')::smallint as a_4_3,
        nullif(json_payload ->> 'group_a/group_a_4/group_a_4_4', '')::smallint as a_4_4,
        nullif(json_payload ->> 'group_a/group_a_4/group_a_4_5', '')::smallint as a_4_5,
        nullif(json_payload ->> 'group_a/group_a_5/group_a_5_1', '')::smallint as a_5_1,

        -- B. Sikap dan Perilaku dalam Pelatihan
        nullif(json_payload ->> 'group_b/group_b_1/group_b_1_1', '')::smallint as b_1_1,
        nullif(json_payload ->> 'group_b/group_b_1/group_b_1_2', '')::smallint as b_1_2,
        nullif(json_payload ->> 'group_b/group_b_1/group_b_1_3', '')::smallint as b_1_3,
        nullif(json_payload ->> 'group_b/group_b_1/group_b_1_4', '')::smallint as b_1_4,
        nullif(json_payload ->> 'group_b/group_b_2/group_b_2_1', '')::smallint as b_2_1,
        nullif(json_payload ->> 'group_b/group_b_2/group_b_2_2', '')::smallint as b_2_2,
        nullif(json_payload ->> 'group_b/group_b_2/group_b_2_3', '')::smallint as b_2_3,
        nullif(json_payload ->> 'group_b/group_b_2/group_b_2_4', '')::smallint as b_2_4,
        nullif(json_payload ->> 'group_b/group_b_3/group_b_3_1', '')::smallint as b_3_1,
        nullif(json_payload ->> 'group_b/group_b_3/group_b_3_2', '')::smallint as b_3_2,
        nullif(json_payload ->> 'group_b/group_b_3/group_b_3_3', '')::smallint as b_3_3

    from source_data
    where json_payload is not null

),

deduped as (

    -- Kobo edits reuse meta/rootUuid and mint a new _uuid; keep the last write.
    select distinct on (meta_root_uuid) *
    from typed_data
    order by meta_root_uuid, submission_time desc

),

items_long as (

    -- One row per answered item. Unanswered items drop out here, which is what
    -- makes every denominator below a live count rather than a hardcoded one.
    select
        d.submission_id,
        i.grup,
        i.subgrup,
        i.skor
    from deduped d
    cross join lateral (
        values
            ('a', 'a_1', d.a_1_1),
            ('a', 'a_2', d.a_2_1),
            ('a', 'a_2', d.a_2_2),
            ('a', 'a_3', d.a_3_1),
            ('a', 'a_4', d.a_4_1),
            ('a', 'a_4', d.a_4_2),
            ('a', 'a_4', d.a_4_3),
            ('a', 'a_4', d.a_4_4),
            ('a', 'a_4', d.a_4_5),
            ('a', 'a_5', d.a_5_1),
            ('b', 'b_1', d.b_1_1),
            ('b', 'b_1', d.b_1_2),
            ('b', 'b_1', d.b_1_3),
            ('b', 'b_1', d.b_1_4),
            ('b', 'b_2', d.b_2_1),
            ('b', 'b_2', d.b_2_2),
            ('b', 'b_2', d.b_2_3),
            ('b', 'b_2', d.b_2_4),
            ('b', 'b_3', d.b_3_1),
            ('b', 'b_3', d.b_3_2),
            ('b', 'b_3', d.b_3_3)
    ) as i (grup, subgrup, skor)
    where i.skor is not null

),

scored as (

    -- opsi_skor is ordinal 1-3, so rescale (x - 1) / 2 to a 0-1 fraction:
    -- 1 Kurang dari Harapan = 0.0, 2 Sesuai Harapan = 0.5, 3 Lebih dari Harapan = 1.0.
    -- Group-level scores are ITEM-weighted, not the mean of the subgroup
    -- percentages, matching monitoring_posyandu. It matters here: group_a_4
    -- carries 5 of the 10 A items while group_a_1 carries 1.
    select
        submission_id,

        avg((skor - 1) / 2.0) filter (where subgrup = 'a_1')    as group_a_1_perc,
        avg((skor - 1) / 2.0) filter (where subgrup = 'a_2')    as group_a_2_perc,
        avg((skor - 1) / 2.0) filter (where subgrup = 'a_3')    as group_a_3_perc,
        avg((skor - 1) / 2.0) filter (where subgrup = 'a_4')    as group_a_4_perc,
        avg((skor - 1) / 2.0) filter (where subgrup = 'a_5')    as group_a_5_perc,
        avg((skor - 1) / 2.0) filter (where grup = 'a')         as group_a_perc,

        -- Raw total on the 1-3 scale: 10 items, so 10-30.
        sum(skor) filter (where grup = 'a')                     as group_a_score,
        case
            when sum(skor) filter (where grup = 'a') between 10 and 16 then 'KH - Kurang dari Harapan'
            when sum(skor) filter (where grup = 'a') between 17 and 23 then 'SH - Sesuai Harapan'
            when sum(skor) filter (where grup = 'a') between 24 and 30 then 'LH - Lebih dari Harapan'
        end                                                     as group_a_score_category,

        avg((skor - 1) / 2.0) filter (where subgrup = 'b_1')    as group_b_1_perc,
        avg((skor - 1) / 2.0) filter (where subgrup = 'b_2')    as group_b_2_perc,
        avg((skor - 1) / 2.0) filter (where subgrup = 'b_3')    as group_b_3_perc,
        avg((skor - 1) / 2.0) filter (where grup = 'b')         as group_b_perc,

        -- Raw total on the 1-3 scale: 11 items, so 11-33.
        sum(skor) filter (where grup = 'b')                     as group_b_score,
        case
            when sum(skor) filter (where grup = 'b') between 11 and 18 then 'KH - Kurang dari Harapan'
            when sum(skor) filter (where grup = 'b') between 19 and 26 then 'SH - Sesuai Harapan'
            when sum(skor) filter (where grup = 'b') between 27 and 33 then 'LH - Lebih dari Harapan'
        end                                                     as group_b_score_category,

        -- Item-weighted over all 21 items, but only where BOTH blocks were
        -- asked. The A-only and B-only versions cannot produce a comparable
        -- overall, so they get null rather than a one-block score that would
        -- silently skew any downstream average. To score whatever was asked
        -- instead, replace the whole expression with: avg((skor - 1) / 2.0)
        case
            when count(*) filter (where grup = 'a') > 0
             and count(*) filter (where grup = 'b') > 0
            then avg((skor - 1) / 2.0)
        end                                                     as overall_perc

    from items_long
    group by submission_id

),

final as (

    select
        d.submission_id,
        d.submission_uuid,
        d.form_version,
        d.submission_time,
        d.submission_start_at,
        d.submission_end_at,

        d.peserta_nama,
        d.asesmen_tanggal,
        coalesce(d.penilai_nama_lain, d.penilai_nama_kode)      as penilai_nama,

        ref_prov.label                                          as provinsi,
        ref_kab.label                                           as kota_kabupaten,
        case
            when d.puskesmas_kode = 'lainnya' then d.puskesmas_lain
            else ref_pkm.label
        end                                                     as puskesmas,

        -- A. Rencana Pembelajaran
        s.group_a_1_perc,   -- 1. Identitas Dokumen
        s.group_a_2_perc,   -- 2. Tujuan Pembelajaran
        s.group_a_3_perc,   -- 3. Pokok Bahasan
        s.group_a_4_perc,   -- 4. Kegiatan Pembelajaran
        s.group_a_5_perc,   -- 5. Evaluasi
        s.group_a_perc,
        s.group_a_score,
        s.group_a_score_category,

        -- B. Sikap dan Perilaku dalam Pelatihan
        s.group_b_1_perc,   -- 1. Kehadiran dan Kedisiplinan
        s.group_b_2_perc,   -- 2. Partisipasi dan Keaktifan
        s.group_b_3_perc,   -- 3. Komunikasi di Dalam Kelas
        s.group_b_perc,
        s.group_b_score,
        s.group_b_score_category,

        s.overall_perc,

        d.catatan

    from deduped d
    left join scored s
        on s.submission_id = d.submission_id
    left join reference.kobo_list_provinsi_active  ref_prov on ref_prov.name = d.provinsi_kode
    left join reference.kobo_list_kabupaten_active ref_kab  on ref_kab.name  = d.kota_kabupaten_kode
    left join reference.kobo_list_puskesmas_active ref_pkm  on ref_pkm.name  = d.puskesmas_kode

)

select * from final