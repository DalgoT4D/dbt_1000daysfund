-- Model: One row per participant x quiz question, for per-item % correct dashboards.
-- Grain: training_code + record_id + question_no.
-- Questions a participant was never asked (is_correct null) are dropped, so
-- AVG(is_correct::int) gives % correct among those who were asked.
-- Add a cohort by adding it to `cohorts` below (combiner model + question count).
{% set cohorts = [
    {'code': 12, 'model': 'training_12_forms_stg', 'n': 20},
    {'code': 13, 'model': 'training_13_forms', 'n': 19},
    {'code': 14, 'model': 'training_14_forms', 'n': 26},
] %}
-- Columns not every combiner has (e.g. cohort 12 has no Sheets source, so no
-- response_source). Missing ones fall back to the value given here.
-- record_id, form_tag and q*_correct are required and will error if absent.
{% set optional_columns = {
    'response_source': "'forms'::text",
    'unified_name': 'null::text',
    'provinsi_raw': 'null::text',
    'kabupaten_raw': 'null::text',
    'kecamatan_raw': 'null::text',
    'desa_raw': 'null::text',
    'puskesmas_raw': 'null::text',
    'posyandu_raw': 'null::text',
    'peran_category': 'null::text',
    'jenis_kelamin': 'null::text',
    'education': 'null::text',
    'timestamp_raw': 'null::timestamp',
    'year': 'null::integer',
    'quarter': 'null::text',
} %}

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    tags=['training', 'mart', 'training_answers']
) }}

-- Map training codes to display types (same mapping as training_answers_stg).
with training_meta (training_code, training_type) as (
    values
        (12, 'Kelompok Kerja'),
        (13, 'Poster Pintar, GC, Manajemen Posyandu'),
        (14, 'ASI, MPASI, ICCM')
),

-- Unpivot the q*_correct columns of every cohort combiner.
unpivoted as (
    {% for c in cohorts %}
    {%- set rel = ref(c.model) -%}
    {%- set existing = [] -%}
    {%- if execute -%}
        {%- for col in adapter.get_columns_in_relation(rel) -%}
            {%- do existing.append(col.name | lower) -%}
        {%- endfor -%}
    {%- endif %}
    select
        {{ c.code }} as training_code,
        t.record_id,
        t.form_tag,
        {% for col, fallback in optional_columns.items() %}
        {% if col in existing %}t.{{ col }}{% else %}{{ fallback }}{% endif %} as {{ col }},
        {% endfor %}
        v.question_no,
        v.is_correct
    from {{ ref(c.model) }} t
    cross join lateral (values
        {% for i in range(1, c.n + 1) %}
        ({{ i }}, t.q{{ i }}_correct){% if not loop.last %},{% endif %}
        {% endfor %}
    ) as v(question_no, is_correct)
    where v.is_correct is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

-- Light district cleanup: drop Kab./Kabupaten prefixes, keep "Kota ", title-case.
-- Province is passed through as-is (it is sometimes wrong for the district).
geo as (
    select
        *,
        initcap(nullif(btrim(regexp_replace(
            regexp_replace(lower(kabupaten_raw), '^\s*(kabupaten|kab\.?)\s*', ''),
            '\s+', ' ', 'g')), '')) as kabupaten
    from unpivoted
),

-- One display label per question, preferring the forms wording.
labels as (
    select
        training_code,
        question_no,
        coalesce(max(question_label) filter (where source_type = 'forms'),
                 max(question_label)) as question_label
    from reference.training_answer
    group by 1, 2
)

select
    g.training_code::text as training_code,
    m.training_type,
    g.record_id,
    g.response_source,
    g.form_tag,
    g.unified_name,
    g.provinsi_raw as provinsi,
    g.kabupaten as kota_kabupaten,
    g.kecamatan_raw as kecamatan,
    g.desa_raw as desa_kelurahan,
    g.puskesmas_raw as puskesmas,
    g.posyandu_raw as posyandu,
    g.peran_category,
    g.jenis_kelamin,
    g.education,
    g.timestamp_raw::date as date,
    g.year,
    g.quarter,
    g.question_no,
    l.question_label,
    g.is_correct,
    g.is_correct::int as is_correct_int
from geo g
left join training_meta m using (training_code)
left join labels l using (training_code, question_no)
