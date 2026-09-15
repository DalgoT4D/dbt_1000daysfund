-- Model: one unified table of every KA participation record, current and
-- historical.
--
-- This combines two very different sources into one consistent shape:
--   - modules 1-3 (current, Q2 2026 onward): people can submit under slightly
--     different names each time (typos, nicknames, adding a role suffix),
--     so before we can dedupe or count participation we first need to figure
--     out which submitted names actually belong to the same person.
--   - ka_past_quarter (historical, before Q2 2026): already one row per
--     person, but its kota_kabupaten values were never run through district
--     correction, unlike the current modules.
--
-- The steps below, in order:
--   1. current_modules_raw        - stack modules 1, 2, 3 into one set of rows.
--   2. distinct_names -> normalized_names -> name_pairs -> name_groups ->
--      name_groups_final -> with_unified
--                                  - identity resolution: decide which
--                                    submitted names are the same person
--                                    (matched by shared kota_kabupaten/email/
--                                    whatsapp plus a close name match), and
--                                    pick one representative name per person
--                                    (`unified_nama`).
--   3. ranked                      - drop duplicate submissions, keeping the
--                                    most recent one per person per module
--                                    per quarter.
--   4. district_lookup -> past_quarter_raw -> past_quarter
--                                  - load the historical records and correct
--                                    their kota_kabupaten values using the same
--                                    lookup table modules 1-3 already use.
--   5. current_modules             - finalize the current-module records
--                                    (score rounding, is_certified/is_latest
--                                    flags) to match past_quarter's shape.
--   6. Final select                - union both sides into one output.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

-- Step 1: stack the three current modules into one set of rows, tagging each
-- with which module it came from.
with recursive current_modules_raw as (
    select
        email,
        nama,
        clean_nama,
        role,
        whatsapp,
        kota_kabupaten_original,
        kota_kabupaten,
        provinsi,
        puskesmas,
        desa_kelurahan,
        cast(year as integer) as year,
        quarter,
        date,
        timestamp_raw,
        score,
        'modul_1' as modul
    from {{ ref('ka_modul_1_clean') }}

    union all

    select
        email,
        nama,
        clean_nama,
        role,
        whatsapp,
        kota_kabupaten_original,
        kota_kabupaten,
        provinsi,
        puskesmas,
        desa_kelurahan,
        cast(year as integer) as year,
        quarter,
        date,
        timestamp_raw,
        score,
        'modul_2' as modul
    from {{ ref('ka_modul_2_clean') }}

    union all

    select
        email,
        nama,
        clean_nama,
        role,
        whatsapp,
        kota_kabupaten_original,
        kota_kabupaten,
        provinsi,
        puskesmas,
        desa_kelurahan,
        cast(year as integer) as year,
        quarter,
        date,
        timestamp_raw,
        score,
        'modul_3' as modul
    from {{ ref('ka_modul_3_clean') }}
),

-- Step 2 (identity resolution): every distinct nama+kota_kabupaten+contact
-- combination seen in the current modules - the raw material for figuring
-- out which of these are actually the same person.
distinct_names as (
    select distinct clean_nama, nama, kota_kabupaten, email, whatsapp
    from current_modules_raw
    where clean_nama is not null
),

-- Strip down to a plain lowercase-letters-only version of each name, so two
-- names that differ only by punctuation/numbers/extra whitespace can still
-- be compared for similarity.
normalized_names as (
    select
        clean_nama,
        nama,
        kota_kabupaten,
        email,
        whatsapp,
        trim(
            regexp_replace(
                regexp_replace(clean_nama, '[^a-z ]', '', 'g'),
                '\s+',
                ' ',
                'g'
            )
        ) as name_plain -- more cleaning than just the macro used for unicode clean up
    from distinct_names
),

-- Pair up two names as "likely the same person" only when they share hard
-- identity evidence (same kota_kabupaten, or same email, or same whatsapp number)
-- AND the names themselves are a close match - either textually similar, or
-- one is the other with something appended (e.g. "Adea" vs "Adea O").
-- Matching name alone, or identity evidence alone, isn't enough on its own.
name_pairs as (
    select
        a.clean_nama,
        least(a.clean_nama, b.clean_nama) as root
    from normalized_names a
    join normalized_names b
        on a.clean_nama <> b.clean_nama
        and a.name_plain <> ''
        and b.name_plain <> ''
        and (
            a.kota_kabupaten = b.kota_kabupaten
            or a.email = b.email
            or a.whatsapp = b.whatsapp
        )
        -- Catch close variants plus short-prefix forms like "Adea" vs "Adea O".
        and (
            similarity(a.name_plain, b.name_plain) >= 0.64
            or a.name_plain like b.name_plain || ' %'
            or b.name_plain like a.name_plain || ' %'
        )
),

-- name_pairs only links names directly to each other (A-B, B-C), not
-- transitively (A-C). This recursive CTE walks those chains to find, for
-- every linked name, the single earliest/alphabetically-first name in its
-- whole chain - that becomes the shared group key for everyone in the chain.
name_groups (clean_nama, root) as (
    select clean_nama, root from name_pairs
    union
    select np.clean_nama, ng.root
    from name_pairs np
    join name_groups ng on np.root = ng.clean_nama
),

-- Two passes of chain-following can still leave a name pointing at a root
-- that itself points further ("A" -> "B" -> "C"); this collapses that down
-- to one final group key per name. Names with no match at all become their
-- own group of one.
name_groups_final as (
    select clean_nama, min(root) as name_group_key
    from (
        select clean_nama, root from name_groups
        union all
        select ng.clean_nama, ng2.root
        from name_groups ng
        join name_groups ng2 on ng.root = ng2.clean_nama
    ) chained
    group by clean_nama

    union all
    select distinct dn.clean_nama, dn.clean_nama
    from distinct_names dn
    where not exists (
        select 1
        from name_groups ng
        where ng.clean_nama = dn.clean_nama
    )
),

-- Pick one representative name per identity group (the longest name in the
-- group, i.e. most complete/least likely to be a shortened nickname, earliest
-- submitted as the tiebreaker) and stamp it onto every row in that group as
-- `unified_nama`.
with_unified as (
    select
        cm.*,
        first_value(cm.nama) over (
            partition by coalesce(
                ngf.name_group_key,
                cm.clean_nama,
                lower(trim(cm.nama)),
                cm.email,
                cm.whatsapp
            )
            order by length(coalesce(cm.clean_nama, '')) desc, cm.timestamp_raw asc
            rows between unbounded preceding and unbounded following
        ) as unified_nama
    from current_modules_raw cm
    left join name_groups_final ngf on cm.clean_nama = ngf.clean_nama
),

-- Step 3: the same person can submit the same module more than once in a
-- quarter (retakes, corrections). Number each person's submissions per
-- module/quarter newest-first; only rn = 1 (kept below as is_latest) is the
-- one that counts as their current record.
ranked as (
    select
        *,
        row_number() over (
            partition by
                modul,
                quarter,
                coalesce(email, whatsapp),
                kota_kabupaten,
                coalesce(unified_nama, nama, clean_nama)
            order by timestamp_raw desc
        ) as rn
    from with_unified
),

-- Step 4: same typo -> kota_kabupaten correction table the current modules already
-- use in staging (ka_modul_1/2/3_clean) - needed here because ka_past_quarter
-- is read directly from source and has never been through it.
district_lookup as (
    select typo_key, kota_kabupaten
    from {{ ref('ka_district_lookup_int') }}
),

-- Parse and type-cast the historical sheet. kota_kabupaten here is still the
-- raw, uncorrected value (see kota_kabupaten_raw) - correction happens in the
-- next CTE.
past_quarter_raw as (
    select
        nullif(trim("email"), '') as email,
        nullif(trim("name"), '') as nama,
        null::varchar as unified_nama,
        nullif(trim("role"), '') as role,
        nullif(trim("whatsapp"), '') as whatsapp,
        nullif(trim("district"), '') as kota_kabupaten_raw,
        nullif(trim("province"), '') as provinsi,
        nullif(trim("puskesmas"), '') as puskesmas,
        nullif(trim("village"), '') as desa_kelurahan,
        nullif(trim("year"), '')::integer as year,
        nullif(trim("quarter"), '') as quarter,
        nullif(trim("date"), '')::date as date,
        round(nullif(trim("score"), '')::numeric)::integer as score,
        nullif(trim("is_certified"), '') as is_certified,
        nullif(trim("modul"), '') as modul,
        nullif(trim("program"), '') as program,
        nullif(trim("is_latest"), '')::boolean as is_latest
    from {{ source('raw_sheets', 'ka_past_quarter') }}
    where nullif(trim("date"), '')::date < date '2026-07-01'
),

-- Look up each raw kota_kabupaten value; use the corrected value when there's
-- an approved/suggested match, otherwise keep the raw value as typed.
past_quarter as (
    select
        pq.email,
        pq.nama,
        pq.unified_nama,
        pq.role,
        pq.whatsapp,
        coalesce(dl.kota_kabupaten, pq.kota_kabupaten_raw) as kota_kabupaten,
        pq.provinsi,
        pq.puskesmas,
        pq.desa_kelurahan,
        pq.year,
        pq.quarter,
        pq.date,
        pq.score,
        pq.is_certified,
        pq.modul,
        pq.program,
        pq.is_latest
    from past_quarter_raw pq
    left join district_lookup dl on lower(trim(pq.kota_kabupaten_raw)) = dl.typo_key
),

-- Step 5: finish the current-module records so their columns line up
-- one-for-one with past_quarter's - round scores to whole percentages, derive
-- is_certified from the score, and mark each person's most recent submission
-- (rn = 1 from `ranked`) as is_latest.
current_modules as (
    select
        email,
        nama,
        unified_nama,
        role,
        whatsapp,
        kota_kabupaten,
        provinsi,
        puskesmas,
        desa_kelurahan,
        year,
        quarter,
        date,
        round(score)::integer as score,
        case when score >= 80 then 'TRUE' when score is not null then 'FALSE' end as is_certified,
        modul,
        null::varchar as program,
        rn = 1 as is_latest
    from ranked
)

-- Step 6: the final output - historical records plus current records, in one
-- shared shape.
select * from past_quarter
union all
select * from current_modules