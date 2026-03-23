
  create or replace   view CRYPTO.DBT_MICOWS.stg_eth_value
  
   as (
    

select 
    hash_key,
    block_number,
    block_timestamp,
    to_address,
    value_wei / 1e18 as value_eth
from CRYPTO.DBT_MICOWS.stg_eth_base
  );

