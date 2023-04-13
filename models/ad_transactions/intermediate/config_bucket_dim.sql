{{ config(materialized='view') }}

with config_bucket_dim as ( -- 策略中文名 排老黑2.0/PreA4.0低分高分差异化出价
    select
        config_id,
        bucket,
        input_date,
        date_format(input_date,'yyyyMMdd')input_pday,
        first(strategys_name) strategys_name
    from
        (
            select strategys_name, config_id, input_date, bucket
            from fin_dim.dim_m_audience_group_rta_group_config_daily_jwm_pdi LATERAL VIEW EXPLODE(split(buckets, ",")) as bucket
            where input_date >=date_sub(date('${pDate}') ,8)
        )
    group by 1,2,3,4
)

select * from config_bucket_dim



