-- Model: the official district catalog, cleaned up for use as a matching
-- reference.
-- Takes the raw official district catalog (reference."Kabupatenkota") and cleans up its labels
-- for use as a matching reference elsewhere. Specifically, it strips the redundant "Kabupaten"/"Kab." 
-- prefix from every district name — "Kabupaten" just means "district," so it adds nothing 
-- — while deliberately keeping the "Kota" ("city") prefix, because that's the only thing 
-- distinguishing an actual city from a district that might share the same base name 
-- (e.g. "Kota Denpasar" vs. a hypothetical "Kabupaten Denpasar"). 
-- Output: one clean, de-duplicated district name per row — this is the canonical list 
-- everything else matches against.

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
