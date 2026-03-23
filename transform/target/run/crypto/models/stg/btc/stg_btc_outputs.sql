begin;
    insert into CRYPTO.DBT_MICOWS.stg_btc_outputs ("HASH_KEY", "BLOCK_NUMBER", "BLOCK_TIMESTAMP", "IS_COINBASE", "OUTPUT_ADDRESS", "OUTPUT_VALUE")
    (
        select "HASH_KEY", "BLOCK_NUMBER", "BLOCK_TIMESTAMP", "IS_COINBASE", "OUTPUT_ADDRESS", "OUTPUT_VALUE"
        from CRYPTO.DBT_MICOWS.stg_btc_outputs__dbt_tmp
    )

;
    commit;