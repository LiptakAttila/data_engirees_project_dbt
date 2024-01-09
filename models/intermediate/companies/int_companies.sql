
/*
WITH stg_companies AS (
    SELECT
        *  -- Include necessary columns from the staging layer
    FROM
        {{ ref('stg_companies') }}
)
SELECT
   company_pk,
   organization,
   repository_account,
   repository_name,
   L1_type,
   L2_type,
   L3_type,
   tags,
   is_open_source_available,
   created_at_datetime_utc,
   'v1' as version  -- Add a version column indicating the version of the data,

FROM
   {{ ref('stg_companies') }}
ORDER BY 
   company_pk ASC 


*/
{% snapshot int_companies %}
{{
    config(
         target_schema='snapshots',
         strategy='timestamp',
         unique_key='company_sk',
         updated_at='created_at_datetime_utc',
    )
}}
SELECT
   company_pk,
   organization,
   repository_account,
   repository_name,
   L1_type,
   L2_type,
   L3_type,
   tags,
   is_open_source_available,
   created_at_datetime_utc,
FROM
   {{ ref('stg_companies') }}
ORDER BY
   company_pk ASC
{% endsnapshot %}