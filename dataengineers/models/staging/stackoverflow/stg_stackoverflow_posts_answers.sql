with source_stackowerflow as (

    select * from {{ source('stackoverflow', 'answers') }}

    where extract(year from creation_date) = 2022

),

final as (

    select 

        id as _pk,
        title,
        body,
        accepted_answer_id,
        answer_count,
        comment_count,
        community_owned_date as community_owned_date_datetime_utc,
        creation_date as creation_date_datetime_utc,
        favorite_count,
        last_activity_date as last_activity_date_datetime_utc,
        last_edit_date as last_edit_date_datetime_utc,
        last_editor_display_name,
        last_editor_user_id,
        owner_display_name,
        owner_user_id,
        parent_id,
        post_type_id,
        score,
        tags,
        view_count,
        current_timestamp() as load_datetime

    from source_stackowerflow

)

select * from final