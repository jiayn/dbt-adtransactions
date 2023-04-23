{{ config(materialized='ephemeral') }}

with ad_transactions_details as (

   select * from {{ ref('stg_ad_transactions__details') }}

),
ephemeral_data as (
  select  p_day
        , p_resource_code
        , win_config_id
        , req_bucket
        , sum(imp_pv) as imp_pv
        , count(distinct imp_device_md5) as imp_uv
        , sum(clk_pv) as clk_pv
        , count(distinct clk_device_md5) as clk_uv
        , sum(imp_cpm_cost) as imp_cpm_cost
    from (
      select 
            p_day
          , p_resource_code
          , win_config_id
          , req_bucket
          , imp_device_md5
          , clk_device_md5
          , sum(imp_agg_ct) as imp_pv
          , sum(clk_agg_ct) as clk_pv
          , sum(imp_cpm_cost) as imp_cpm_cost
      from ad_transactions_details
      where p_day >= date_format(date_sub(date('{{ var("pday") }}') ,7),'yyyyMMdd')
      and   p_day < date_format(date_add(date('{{ var("pday") }}') ,1),'yyyyMMdd')
      group by 1,2,3,4,5,6
    ) ad_f
    group by 1,2,3,4
)

select * from ephemeral_data
