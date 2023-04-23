{{ config(materialized='view') }}

with source as (

    select * from {{ source('data_ai_glaucus', 'dim_m_audience_group_rta_group_config_daily_jwm_pdi') }} 

),
renamed as ( 
    select
        config_id,
        bucket,
        input_date,
        date_format(input_date,'yyyyMMdd')input_pday,
        first(strategys_name) strategys_name
    from
        (
            select strategys_name, config_id, input_date, bucket
            from source LATERAL VIEW EXPLODE(split(buckets, ",")) as bucket
        )
    group by 1,2,3,4
)

select * from renamed



