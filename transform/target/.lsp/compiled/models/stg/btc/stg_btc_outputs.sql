

with flattened_outputs as (

    select
        stg.hash_key,
        stg.block_number,
        stg.block_timestamp,
        stg.is_coinbase,
        f.value:address::string as output_address,
        f.value:value::float as output_value
    from CRYPTO.DBT_MICOWS.stg_btc_base stg, 
        lateral flatten(input => stg.outputs) f
    where 
        f.value:address is not null

    

    and stg.block_timestamp > (
        select max(block_timestamp)
        from CRYPTO.DBT_MICOWS.stg_btc_outputs
    )

    
)

select 
    hash_key,
    block_number,
    block_timestamp,
    is_coinbase,
    output_address,
    output_value
from flattened_outputs