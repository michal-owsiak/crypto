select
    hash_key,
    block_number,
    block_timestamp,
    is_coinbase
from
    {{ ref('stg_btc') }}