{{
    config(
        materialized='table'
    )
}}

{{ 
    build_supertrend(
        ref('int_btcusdt_1w'),
        '1w',
        10,
        3
    ) 
}}
