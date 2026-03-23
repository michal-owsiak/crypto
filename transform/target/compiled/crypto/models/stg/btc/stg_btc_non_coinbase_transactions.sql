

select *
from 
    CRYPTO.DBT_MICOWS.stg_btc_outputs
where
    is_coinbase = false