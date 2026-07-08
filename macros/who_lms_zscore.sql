{% macro who_lms_zscore(y, l, m, s) -%}
{#-
    WHO Child Growth Standards z-score (LMS method).

    z = ((y/M)^L - 1) / (S * L)

    Beyond |z| = 3 the WHO applies a linear tail correction so extreme
    values stay comparable across ages/indicators:
        z > 3  ->  3 + (y - SD3pos) / (SD3pos - SD2pos)
        z < -3 -> -3 + (y - SD3neg) / (SD2neg - SD3neg)
    where SDkpos/neg = M * (1 + L*S*k)^(1/L).

    Assumes L <> 0 (holds for WFA and LHFA). Returns NULL on any NULL input
    (e.g. age outside the 0-60m reference range -> no seed match -> NULL L/M/S).

    Args are SQL expressions/column refs: y = measurement, l/m/s = LMS params.
-#}
{%- set zraw   = "((power((" ~ y ~ ") / (" ~ m ~ "), (" ~ l ~ ")) - 1) / ((" ~ s ~ ") * (" ~ l ~ ")))" -%}
{%- set sd3pos = "((" ~ m ~ ") * power(1 + (" ~ l ~ ") * (" ~ s ~ ") * ( 3), 1.0 / (" ~ l ~ ")))" -%}
{%- set sd2pos = "((" ~ m ~ ") * power(1 + (" ~ l ~ ") * (" ~ s ~ ") * ( 2), 1.0 / (" ~ l ~ ")))" -%}
{%- set sd3neg = "((" ~ m ~ ") * power(1 + (" ~ l ~ ") * (" ~ s ~ ") * (-3), 1.0 / (" ~ l ~ ")))" -%}
{%- set sd2neg = "((" ~ m ~ ") * power(1 + (" ~ l ~ ") * (" ~ s ~ ") * (-2), 1.0 / (" ~ l ~ ")))" -%}
    case
        when ({{ y }}) is null or ({{ l }}) is null or ({{ m }}) is null or ({{ s }}) is null
            then null
        when {{ zraw }} >  3
            then  3 + (({{ y }}) - {{ sd3pos }}) / ({{ sd3pos }} - {{ sd2pos }})
        when {{ zraw }} < -3
            then -3 + (({{ y }}) - {{ sd3neg }}) / ({{ sd2neg }} - {{ sd3neg }})
        else {{ zraw }}
    end
{%- endmacro %}
