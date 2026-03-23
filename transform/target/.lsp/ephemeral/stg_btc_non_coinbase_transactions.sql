__dbt__cte__stg_btc_non_coinbase_transactions as (


select *
from 
    CRYPTO.DBT_MICOWS.stg_btc_outputs
where
    is_coinbase = false
)