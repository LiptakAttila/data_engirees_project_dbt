
with source_company as (

    select * from {{ source('company', 'table') }}

),

final as (

    select
 
        row_number() over(order by Organization DESC) as _pk,
        Organization as organization,
        Repository_account as repository_account,
        Repository_name as repository_name,
        L1_type,
        L2_type,
        L3_type,
        Tags as tags,
        case 
            when Open_source_available = 'Yes' then true
            when Open_source_available = 'No' then false
        end as is_open_source_available,
        current_datetime() as created_at_datetime_utc,
        DATETIME_ADD(current_timestamp(), INTERVAL -EXTRACT(SECOND FROM current_timestamp()) SECOND) as load_datetime

    from source_company

)

select * from final