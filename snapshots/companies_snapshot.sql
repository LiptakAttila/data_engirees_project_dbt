{% snapshot company_snapshot %}

    {{
        config(
            target_schema='snapshots',
            strategy='check',
            unique_key='company_sk',
            check_cols=['company_sk', 'repository_account', 'repository_name', 'tags'],
            invalidate_hard_deletes=True,
        )

    }}

select * from {{ ref("stg_companies")}}

{% endsnapshot %}

