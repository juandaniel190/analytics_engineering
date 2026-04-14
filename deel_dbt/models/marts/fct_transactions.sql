with acceptance as (
    select * from {{ ref('stg_globepay__acceptance') }}
),

chargeback as (
    select * from {{ ref('stg_globepay__chargeback') }}
)

select
    a.external_ref,
    a.transaction_at,
    a.transaction_date,
    a.transaction_week,
    a.transaction_month,
    a.source,
    a.country,
    a.currency_local,
    a.amount_local,
    a.amount_usd,
    a.is_accepted,
    a.is_cvv_provided,
    a.is_active,
    c.has_chargeback,
    c.external_ref is null as is_missing_chargeback
from acceptance a
left join chargeback c
    on a.external_ref = c.external_ref
