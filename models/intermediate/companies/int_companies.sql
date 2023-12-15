/*{{ config(
   materialized='snapshot',
   unique_key='_pk',
   strategy='check'
) }}

SELECT
   _pk,
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
   _pk ASC 
*/