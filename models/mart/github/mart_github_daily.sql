WITH time_granularity AS (
  SELECT
    EXTRACT(DAY FROM created_at_datetime_utc) AS day,
    EXTRACT(MONTH FROM created_at_datetime_utc) AS month,
    EXTRACT(QUARTER FROM created_at_datetime_utc) AS quarter,
    EXTRACT(YEAR FROM created_at_datetime_utc) AS year,
    organization,
    repository_account_git,
    COALESCE(repository_name_git, repository_account_git) AS repository_name,
    COUNT(DISTINCT event_id) AS event_count,
    COUNT(DISTINCT user_id) AS user_count,
    COUNT(DISTINCT CASE WHEN type = 'IssuesEvent' THEN event_id END) AS issues_count,
    COUNT(DISTINCT CASE WHEN type = 'MemberEvent' THEN event_id END) AS member_count,
    COUNT(DISTINCT CASE WHEN type = 'CreateEvent' THEN event_id END) AS create_count,
    COUNT(DISTINCT CASE WHEN type = 'GollumEvent' THEN event_id END) AS gollum_count,
    COUNT(DISTINCT CASE WHEN type = 'PublicEvent' THEN event_id END) AS public_count,
    COUNT(DISTINCT CASE WHEN type = 'DeleteEvent' THEN event_id END) AS delete_count,
    COUNT(DISTINCT CASE WHEN type = 'PullRequestReviewEvent' THEN event_id END) AS pr_count,
    COUNT(DISTINCT CASE WHEN type = 'WatchEvent' THEN event_id END) AS watch_count,
    COUNT(DISTINCT CASE WHEN type = 'ForkEvent' THEN event_id END) AS fork_count,
    COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN event_id END) AS push_count,
    COUNT(DISTINCT CASE WHEN type = 'CommitCommentEvent' THEN event_id END) AS commit_comment_count,
    COUNT(DISTINCT event_id) AS total_event_count
  FROM {{ ref('int_github') }}
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ["organization"]
    ) }} as _pk,
  day,
  month,
  quarter,
  year,
  organization,
  repository_account_git,
  repository_name,
  event_count,
  user_count,
  issues_count,
  member_count,
  create_count,
  gollum_count,
  public_count,
  delete_count,
  pr_count,
  watch_count,
  fork_count,
  push_count,
  commit_comment_count
FROM time_granularity
