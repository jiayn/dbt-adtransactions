{{ config(materialized='view') }}

with rta_bucket_config_dim as ( 
    select
        config_id,
        bucket,
        input_date,
        date_format(input_date,'yyyyMMdd')input_pday,
        first(strategys_name) as strategys_name
    from
        (
            select 
                strategys_name
                , config_id
                , input_date
                , LATERAL VIEW EXPLODE(split(buckets, ",")) as bucket
            from (
                select 
                    strategys_name
                    , config_id
                    , input_date
                    , buckets
                from {{ source('fin_dim', 'dim_m_audience_group_rta_group_config_daily_jwm_pdi') }} 
                where input_date >=date_sub(date('{{ var("pday") }}') ,8)
                ) temp_table_1
        ) temp_table_2
    group by 1,2,3,4
)

select * from rta_bucket_config_dim



