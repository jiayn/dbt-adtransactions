{{ config(materialized='view') }}

with ad_front as (

select    p_day
        , p_resource_code
        , win_config_id
        , req_bucket
        , sum(imp_pv) as imp_pv
        , count(distinct imp_device_md5) as imp_uv
        , sum(clk_pv) as clk_pv
        , count(distinct clk_device_md5) as clk_uv
    from (
      select 
            p_day
          , p_resource_code
          , win_config_id
          , req_bucket
          , case  when length(imp_device_id)=32 then imp_device_id
                  when length(imp_idfa_md5)=32 then imp_idfa_md5
                  when length(imp_imei_md5)=32 then imp_imei_md5
                  when length(imp_oaid_md5)=32 then imp_oaid_md5
                  when length(imp_oaid) between 10 and 90 then md5(imp_oaid)
                  when length(imp_android_id)=32 then imp_android_id
                  else null end as imp_device_md5--集中设备号到一个字段,计算UV
          , case  when length(clk_device_id)=32 then clk_device_id
                  when length(clk_idfa_md5)=32 then clk_idfa_md5
                  when length(clk_imei_md5)=32 then clk_imei_md5
                  when length(clk_oaid_md5)=32 then clk_oaid_md5
                  when length(clk_oaid) between 10 and 90 then md5(clk_oaid)
                  when length(clk_android_id)=32 then clk_android_id
                  else null end as clk_device_md5--集中设备号到一个字段,计算UV
          , sum(imp_agg_ct) as imp_pv
          , sum(clk_agg_ct) as clk_pv
    from {{ ref('stg_ad_transactions__details') }}
    group by 1,2,3,4,5,6
    ) ad_f
    group by 1,2,3,4
)
select * from ad_front
