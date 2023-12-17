{{
    config(
        materialized='incremental'
    )

}}

WITH stg_github AS (
    SELECT
        *
    FROM 
        {{ ref('stg_github') }}
    
    {% if is_incremental() %}

    WHERE created_at_datetime_utc > (SELECT max(created_at_datetime_utc) FROM {{ this }})

    {% endif %}
),

github AS (

    SELECT 
        _pk,
        SPLIT(repo_name, '/')[0] AS repository_account_git,
        SPLIT(repo_name, '/')[1] AS repository_name_git,
        actor_id AS user_id,
        id as event_id,
        type,
        created_at_datetime_utc

    FROM stg_github

),

final AS (
    SELECT 
        git._pk,
        git.repository_account_git,
        git.repository_name_git,
        git.user_id,
        git.event_id,
        git.type,
        git.created_at_datetime_utc,
        com.repository_account
    FROM github AS git
    JOIN {{ ref('stg_companies')}} AS com
    ON com.repository_account = git.repository_account_git
)

SELECT * FROM final