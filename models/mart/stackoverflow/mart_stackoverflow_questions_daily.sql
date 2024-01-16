with time_granularity as (
    select

        extract(day from creation_date_datetime_utc) as day,
        extract(month from creation_date_datetime_utc) as month,
        extract(quarter from creation_date_datetime_utc) as quarter,
        extract(year from creation_date_datetime_utc) as year,
        organization,
        count(_pk) as post_count,
        sum(answer_count) as answer_count,
        (sum(answer_count) / count(_pk)) as avg_answer_count,
        sum(comment_count) as comment_count,
        (sum(comment_count) / count(_pk)) as avg_comment_count,
        sum(favorite_count) as favorite_count,
        (sum(favorite_count) / count(_pk)) as avg_favorite_count,
        sum(view_count) as view_count,
        (sum(view_count) / count(_pk)) as avg_view_count,
        count(accepted_answer_id) as accepted_answer_count,
        count(answer_count is null) as no_answer_count,
        (count(answer_count is null) / count(_pk)) as avg_no_answer_count,
        max(last_activity_date_datetime_utc) as last_activity_datetime_utc,
        max(last_edit_date_datetime_utc) as last_edit_datetime_utc

    from {{ ref('int_stackoverflow_questions')}}
    group by 1, 2, 3, 4, 5
)

select

    {{ dbt_utils.generate_surrogate_key(
        ["organization", "day", "year"]
    ) }} as _pk,
    time_granularity.*

from time_granularity
