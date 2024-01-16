WITH time_granularity AS (
  SELECT

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
  FROM {{ ref('int_github')}}
  GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ["organization", "month", "year"]
    ) }} as _pk,

  month,
  quarter,
  year,
  organization,
  repository_account_git,
  repository_name,
  SUM(event_count) AS event_count,
  SUM(user_count) AS user_count,
  SUM(issues_count) AS issues_count,
  SUM(member_count) AS member_count,
  SUM(create_count) AS create_count,
  SUM(gollum_count) AS gollum_count,
  SUM(public_count) AS public_count,
  SUM(delete_count) AS delete_count,
  SUM(pr_count) AS pr_count,
  SUM(watch_count) AS watch_count,
  SUM(fork_count) AS fork_count,
  SUM(push_count) AS push_count,
  SUM(commit_comment_count) AS commit_comment_count,
  SUM(total_event_count) AS total_event_count

FROM time_granularity
GROUP BY 1, 2, 3, 4, 5, 6, 7