{% snapshot stock_snapshot %}

{{
    config(
      target_database='DEV',
      target_schema='SNAPSHOTS',
      unique_key="symbol||'-'||to_varchar(trade_date)",
      strategy='check',
      check_cols=['close', 'volume']
    )
}}

select * from {{ source('raw', 'stock_prices') }}

{% endsnapshot %}
