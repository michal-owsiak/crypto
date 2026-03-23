

select
    src.payload:hash::varchar as hash_key,
    src.payload:block_number::number as block_number,
    src.payload:block_timestamp::timestamp_ntz as block_timestamp,
    src.payload:to_address::varchar as to_address,
    src.payload:value::float as value_wei
from crypto.eth.transactions_raw src



where src.payload:block_timestamp::timestamp_ntz > (
    select max(block_timestamp)
    from CRYPTO.DBT_MICOWS.stg_eth_base
)

