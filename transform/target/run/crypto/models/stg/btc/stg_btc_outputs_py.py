-- back compat for old kwarg name
  
  begin;
    

        insert into CRYPTO.DBT_MICOWS.stg_btc_outputs_py ("HASH_KEY", "BLOCK_HASH", "BLOCK_NUMBER", "BLOCK_TIMESTAMP", "FEE", "INPUT_VALUE", "OUTPUT_VALUE", "FEE_PER_BYTE", "IS_COINBASE", "ADDRESS", "VALUE")
        (
            select "HASH_KEY", "BLOCK_HASH", "BLOCK_NUMBER", "BLOCK_TIMESTAMP", "FEE", "INPUT_VALUE", "OUTPUT_VALUE", "FEE_PER_BYTE", "IS_COINBASE", "ADDRESS", "VALUE"
            from CRYPTO.DBT_MICOWS.stg_btc_outputs_py__dbt_tmp
        );
    commit;