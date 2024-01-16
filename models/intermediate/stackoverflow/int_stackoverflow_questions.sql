{{
    config(
        materialized='incremental'
    )
}}

with stackoverflow_answers_data as (

    select *
    except (body),
    (select(split(tags, '|'))) as tags_array

    from {{ ref("stg_stackoverflow_posts_questions") }}

    {% if is_incremental() %}
        where created_at_datetime_utc > (select max(created_at_datetime_utc) from {{ this }})
    {% endif %}

),

company_data as (

    select *,
    (select(split(tags, ','))) as tags_array
    from {{ ref("int_companies") }}

),

final as (

    select

        stackoverflow_answers_data.*,
        organization

    from stackoverflow_answers_data

    join company_data on
        company_data.organization in unnest(stackoverflow_answers_data.tags_array) or
        company_data.repository_account in unnest(stackoverflow_answers_data.tags_array) or
        company_data.repository_name in unnest(stackoverflow_answers_data.tags_array)

)

select * from final