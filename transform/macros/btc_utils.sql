{% macro convert_to_usd(col_name) %}

    {{ col_name }} * (
        select
            price
        from {{ ref('btc_usd_max') }}
        where to_date(replace(snapped_at, 'UTC', '')) = current_date()
    )

{% endmacro %}