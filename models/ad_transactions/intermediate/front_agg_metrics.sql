{{ config(materialized='view') }}

with front as (
    select  p_day, 
            p_resource_code, 
            win_config_id, 
            req_bucket,
            sum(imp_pv) as imp_pv,
            count(distinct imp_device_md5) as imp_uv,
            sum(clk_pv) as clk_pv,
            count(distinct clk_device_md5) as clk_uv,
            sum(imp_cpm_cost) as imp_cpm_cost
    from (
    select 
        p_day, 
        p_resource_code, 
        win_config_id, 
        req_bucket, 
        case when length(imp_device_id)=32 then imp_device_id
           when length(imp_idfa_md5)=32 then imp_idfa_md5
           when length(imp_imei_md5)=32 then imp_imei_md5
           when length(imp_oaid_md5)=32 then imp_oaid_md5
           when length(imp_oaid) between 10 and 90 then md5(imp_oaid)
           when length(imp_android_id)=32 then imp_android_id
           else null end as imp_device_md5,
        case when length(clk_device_id)=32 then clk_device_id
           when length(clk_idfa_md5)=32 then clk_idfa_md5
           when length(clk_imei_md5)=32 then clk_imei_md5
           when length(clk_oaid_md5)=32 then clk_oaid_md5
           when length(clk_oaid) between 10 and 90 then md5(clk_oaid)
           when length(clk_android_id)=32 then clk_android_id
           else null end as clk_device_md5,
           sum(imp_agg_ct) as imp_pv, 
           sum(clk_agg_ct) as clk_pv, 
           sum(imp_cpm_cost) as imp_cpm_cost
    from {{ source('ad_transactions_sources', 'dm_eco_dataeco_ad_transations_0_4_1') }} as agg
    where p_day >=date_format(DATE_SUB(STR_TO_DATE('{{ var("pday") }}', "%Y-%m-%d"), INTERVAL 7 DAY),'%Y-%m-%d')
    and  p_day < date_format(DATE_ADD(STR_TO_DATE('{{ var("pday") }}', "%Y-%m-%d"), INTERVAL 1 DAY),'%Y-%m-%d')
    group by 1,2,3,4,5,6
    ) as agg_ext
    group by 1,2,3,4
)

select * from front
