{{ config(materialized='view') }}

with ad_transactions_details as (

   select * from {{ ref('stg_ad_transactions__details') }}

),
ad_transactions_aggregated_to_configid_bucket_front as (

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
          , imp_android_id
          , clk_device_md5
          , sum(imp_agg_ct) as imp_pv
          , sum(clk_agg_ct) as clk_pv
      from ad_transactions_details
      group by 1,2,3,4,5,6
    ) ad_f
    group by 1,2,3,4
)

select * from ad_transactions_aggregated_to_configid_bucket_front
