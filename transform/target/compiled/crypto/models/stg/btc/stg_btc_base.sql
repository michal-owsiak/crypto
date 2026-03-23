

select *
from crypto.btc.transactions



where block_timestamp > (
    select max(block_timestamp)
    from CRYPTO.DBT_MICOWS.stg_btc_base
)

