
  create or replace   view CRYPTO.DBT_MICOWS.btc_whale_alert_v2
  
   as (
    with  __dbt__cte__stg_btc_non_coinbase_transactions as (


select *
from 
    CRYPTO.DBT_MICOWS.stg_btc_outputs
where
    is_coinbase = false
), whales as (

    select
        output_address,
        sum(output_value) as total_sent,
        count(*) as transaction_count
    from __dbt__cte__stg_btc_non_coinbase_transactions
    where output_value > 10
    group by output_address

)

select
    output_address,
    total_sent,
    transaction_count
from whales 
order by total_sent desc
  );

