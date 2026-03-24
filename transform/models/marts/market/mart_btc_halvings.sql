{{
    config(
        materialized='view'
    )
}}

select
    halving_number,
    halving_date,
    btc_from,
    btc_to,
    is_historical,
    is_projected
from {{ ref('stg_btc_halvings') }}
