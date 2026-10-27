-- Model: Production view of the linked Posyandu registers, one row per person per month.
-- Built from register_posyandu_combined_int by collapsing every pem_/tdf_ column pair
-- into a single column without the prefix:
--   * both present -> TDF value wins (coalesce(tdf_x, pem_x))
--   * only one present -> that one is kept
-- Columns without a prefix pass through unchanged, in the same order.
-- The pairs are read from the intermediate table at run time, so a new pem_/tdf_ column
-- added upstream is collapsed here automatically.
-- Note: the choice is made per column, so a row with a TDF visit can still carry a
-- pemerintah value in a column TDF left empty. in_pemerintah / in_tdf show which
-- sources contributed to the row.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    tags=['production', 'register_posyandu']
) }}

{%- set source_relation = ref('register_posyandu_combined_int') -%}
{%- set select_list = [] -%}

{%- if execute -%}
    {%- set cols = adapter.get_columns_in_relation(source_relation) | map(attribute='name') | list -%}
    {%- set seen = [] -%}
    {%- for c in cols -%}
        {%- if c[:4] in ['pem_', 'tdf_'] -%}
            {%- set base = c[4:] -%}
            {%- if base in cols -%}
                {{ exceptions.raise_compiler_error("register_posyandu_combined: '" ~ base ~ "' already exists unprefixed, cannot collapse " ~ c) }}
            {%- endif -%}
            {%- if base not in seen -%}
                {%- do seen.append(base) -%}
                {%- set parts = [] -%}
                {%- if ('tdf_' ~ base) in cols -%}{%- do parts.append('tdf_' ~ base) -%}{%- endif -%}
                {%- if ('pem_' ~ base) in cols -%}{%- do parts.append('pem_' ~ base) -%}{%- endif -%}
                {%- if parts | length > 1 -%}
                    {%- do select_list.append('coalesce(' ~ parts | join(', ') ~ ') as ' ~ base) -%}
                {%- else -%}
                    {%- do select_list.append(parts[0] ~ ' as ' ~ base) -%}
                {%- endif -%}
            {%- endif -%}
        {%- else -%}
            {%- do select_list.append(c) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

select
    {{ select_list | join(',\n    ') if select_list else '*' }}
from {{ source_relation }}