-- back compat for old kwarg name
  
  begin;
    
        
            
	    
	    
            
        
    

    

    merge into CRYPTO.DBT_MICOWS.stg_eth_base as DBT_INTERNAL_DEST
        using CRYPTO.DBT_MICOWS.stg_eth_base__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.hash_key = DBT_INTERNAL_DEST.hash_key))

    
    when matched then update set
        "HASH_KEY" = DBT_INTERNAL_SOURCE."HASH_KEY","BLOCK_NUMBER" = DBT_INTERNAL_SOURCE."BLOCK_NUMBER","BLOCK_TIMESTAMP" = DBT_INTERNAL_SOURCE."BLOCK_TIMESTAMP","TO_ADDRESS" = DBT_INTERNAL_SOURCE."TO_ADDRESS","VALUE_WEI" = DBT_INTERNAL_SOURCE."VALUE_WEI"
    

    when not matched then insert
        ("HASH_KEY", "BLOCK_NUMBER", "BLOCK_TIMESTAMP", "TO_ADDRESS", "VALUE_WEI")
    values
        ("HASH_KEY", "BLOCK_NUMBER", "BLOCK_TIMESTAMP", "TO_ADDRESS", "VALUE_WEI")

;
    commit;