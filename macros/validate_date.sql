{% macro validate_date(field_name) %}
-- We have data coming from several google sheets and from commcare. below macro makes sure every data column, no matter
-- which table it is in or which source it is coming from is in the same appropriate format. 
-- Sometimes in google sheets, in one row someone enters 2024/10/05 and elsewhere someone enters 10-5-2024 it will standardize it
-- If the extracted year is before 1900 or after 2050, it is assumed to be incorrect and returns NULL.
CAST(
    CASE
        -- Ensure NULL and empty values are handled safely
        WHEN {{ field_name }} IS NULL OR TRIM({{ field_name }}::TEXT) = '' THEN NULL::DATE

        -- If already a DATE, return as is (but check valid range)
        WHEN LOWER(pg_typeof({{ field_name }})::TEXT) = 'date' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM {{ field_name }}::DATE) BETWEEN 1900 AND 2050 
                THEN {{ field_name }}::DATE
                ELSE NULL 
            END
            
        -- Handle strict DD/MM/YYYY format (e.g., 01/09/2024 or 9/9/2024)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^([0-2]?[0-9]|3[01])/([0]?[1-9]|1[0-2])/[1-2]\d{3}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD/FMMM/YYYY')) BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD/FMMM/YYYY') 
                ELSE NULL 
            END

        -- Handle DD/MM/YY (e.g., 05/08/24 → 2024-08-05) and DD/M/YY (e.g., 15/5/24 → 2024-05-15)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\d{2}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD/FMMM/YY') + INTERVAL '2000 years') BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD/FMMM/YY') + INTERVAL '2000 years' 
                ELSE NULL 
            END

        -- Handle DD-MM-YY (e.g., 05-08-24 → 2024-08-05) and DD-M-YY (e.g., 15-5-24 → 2024-05-15)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\d{2}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD-FMMM-YY') + INTERVAL '2000 years') BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD-FMMM-YY') + INTERVAL '2000 years' 
                ELSE NULL 
            END

        -- Handle ISO 8601 format (e.g., 2024-02-05T08:52:24.859000Z)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$' THEN
            CASE 
                WHEN EXTRACT(YEAR FROM TO_TIMESTAMP(TRIM({{ field_name }}::TEXT), 'YYYY-MM-DD"T"HH24:MI:SS.US')) BETWEEN 1900 AND 2050 
                THEN TO_TIMESTAMP(TRIM({{ field_name }}::TEXT), 'YYYY-MM-DD"T"HH24:MI:SS.US')::DATE
                ELSE NULL 
            END

        -- If TIMESTAMP, convert to DATE (but check valid range)
        WHEN LOWER(pg_typeof({{ field_name }})::TEXT) LIKE 'timestamp%' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM {{ field_name }}::DATE) BETWEEN 1900 AND 2050 
                THEN {{ field_name }}::DATE
                ELSE NULL 
            END
        

        -- Handle DD-M-YYYY (e.g., 15-5-2024 → 2024-05-15)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\d{4}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD-FMMM-YYYY')) BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD-FMMM-YYYY') 
                ELSE NULL 
            END

        -- Handle DD/M/YYYY (e.g., 15/5/2024 → 2024-05-15)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\d{4}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD/FMMM/YYYY')) BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMDD/FMMM/YYYY') 
                ELSE NULL 
            END

        -- Handle YYYY-MM-DD 
        WHEN TRIM({{ field_name }}::TEXT) ~ '^\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'YYYY-FMMM-FMDD')) BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'YYYY-FMMM-FMDD') 
                ELSE NULL 
            END

        -- Handle YYYY/MM/DD 
        WHEN TRIM({{ field_name }}::TEXT) ~ '^\d{4}/(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'YYYY/FMMM/FMDD')) BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'YYYY/FMMM/FMDD') 
                ELSE NULL 
            END

        -- Handle MM-DD-YYYY 
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-\d{4}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM-FMDD-YYYY')) BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM-FMDD-YYYY') 
                ELSE NULL 
            END

        -- Handle MM/DD/YYYY 
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM/FMDD/YYYY')) BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM/FMDD/YYYY') 
                ELSE NULL 
            END

        -- Handle MM/DD/YY (e.g., 08/05/24 → 2024-08-05)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\d{2}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM/FMDD/YY') + INTERVAL '2000 years') BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM/FMDD/YY') + INTERVAL '2000 years' 
                ELSE NULL 
            END

        -- Handle MM-DD-YY (e.g., 08-05-24 → 2024-08-05)
        WHEN TRIM({{ field_name }}::TEXT) ~ '^(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-\d{2}$' THEN 
            CASE 
                WHEN EXTRACT(YEAR FROM TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM-FMDD-YY') + INTERVAL '2000 years') BETWEEN 1900 AND 2050 
                THEN TO_DATE(TRIM({{ field_name }}::TEXT), 'FMMM-FMDD-YY') + INTERVAL '2000 years' 
                ELSE NULL 
            END

        -- If no match, return NULL
        ELSE NULL::DATE
    END
AS DATE)
{% endmacro %}