WITH time_granularity AS (
  SELECT
    date_trunc(DATE(creation_date_datetime_utc), QUARTER) as day,
    EXTRACT(QUARTER FROM creation_date_datetime_utc) AS quarter,
    EXTRACT(YEAR FROM creation_date_datetime_utc) AS year,
    organization,
    COUNT(DISTINCT _pk) AS post_count,
    SUM(answer_count) AS answer_count,
    (SUM(answer_count) / COUNT(_pk)) AS avg_answer_count,
    SUM(comment_count) AS comment_count,
    (SUM(comment_count) / COUNT(_pk)) AS avg_comment_count,
    SUM(favorite_count) AS favorite_count,
    (SUM(favorite_count) / COUNT(_pk)) AS avg_favorite_count,
    SUM(view_count) AS view_count,
    (SUM(view_count) / COUNT(_pk)) AS avg_view_count,
    COUNT(accepted_answer_id) AS accepted_answer,
    COUNT(answer_count IS NULL) AS no_answer_count,
    (COUNT(answer_count IS NULL) / COUNT(_pk)) AS avg_no_answer_count,
    MAX(last_activity_date_datetime_utc) AS last_activity_datetime_utc,
    MAX(last_edit_date_datetime_utc) AS last_edit_datetime_utc
  FROM {{ ref('int_stackoverflow_questions') }}
  GROUP BY 1, 2, 3, 4
)
SELECT
    {{ dbt_utils.generate_surrogate_key (
        ['organization', 'quarter', 'year']
    ) }} as _pk,
    time_granularity.*
FROM time_granularity