-- Model: the official district catalog, cleaned up for use as a matching
-- reference.
--
-- reference."Kabupatenkota" is the raw source list and labels every entry
-- with its administrative type, e.g. "Kabupaten Badung" or "Kota Denpasar".
-- "Kabupaten" just means "district" - it's redundant on every row that has
-- it, so we drop it. "Kota" ("city") is kept, because it's the only thing
-- distinguishing a city from a district that happens to share the same base
-- name (e.g. "Kota Denpasar" vs a hypothetical "Kabupaten Denpasar").
--
-- This is the canonical district list that ka_district_lookup_int fuzzy-
-- matches typed-in district values against.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka", "intermediate"]
) }}

with stripped as (
    -- Remove a leading "Kabupaten" or "Kab." (with any punctuation after it,
    -- e.g. "Kab."), case-insensitively. A label with no such prefix (e.g.
    -- anything starting with "Kota") passes through unchanged.
    select
        regexp_replace(trim(label), '^(kabupaten|kab[[:punct:]]?)[[:space:]]+', '', 'i') as district
    from reference."Kabupatenkota"
    where nullif(trim(label), '') is not null
)

-- Dedupe (stripping the prefix can make two source rows collide) and drop any
-- row that stripped down to nothing.
select distinct district
from stripped
where nullif(trim(district), '') is not null
