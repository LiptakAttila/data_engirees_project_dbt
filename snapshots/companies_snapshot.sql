{% snapshot company_snapshot %}

    {{
        config(
            target_schema='snapshots',
            strategy='timestamp',
            unique_key='company_sk',
            updated_at='created_at_datetime_utc',
        )

    }}

select * from {{ ref("stg_companies")}}

{% endsnapshot %}