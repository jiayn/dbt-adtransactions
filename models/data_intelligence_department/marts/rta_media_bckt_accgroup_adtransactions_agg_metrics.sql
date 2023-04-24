{{ config(
  materialized = 'incremental',
  incremental_strategy = 'insert_overwrite',
  partition_by = 'p_day'
)}}

with incremental_table_data as (
select
    a.p_day as pday
    , a.media
    , a.bucket
    , a.config_id
    , case when a.media in ('穿山甲','头条') then concat('stgy_TT_',a.config_id)
        when a.media in ('快手') then concat('stgy_KS_',a.config_id)
        when a.media in ('广点通','微信') then concat('stgy_TX_',a.config_id)
        when a.media in ( '百度开屏', '百度百青藤', '百度') then concat('stgy_BD_',a.config_id)--V7
        else a.config_id end as config_id_name
    ,  a.strategys_name
    , r.request_pv
    , r.release_pv
    , r.request_uv
    , r.release_uv
    , s.costs
    , coalesce(imp_pv,0)imp_pv
    , coalesce(imp_uv,0)imp_uv
    , coalesce(clk_pv,0)clk_pv
    , coalesce(clk_uv,0)clk_uv
    , coalesce(lz_uv,0)lz_uv
    , coalesce(wj_uv,0)wj_uv
    , coalesce(first_wj_uv,0)first_wj_uv
    , coalesce(wj_t0_uv,0)wj_t0_uv
    , coalesce(sx_uv,0)sx_uv
    , coalesce(first_sx_uv,0)first_sx_uv
    , coalesce(sx_t0_uv,0)sx_t0_uv
    , coalesce(sx_p18_pp_uv,0)sx_p18_pp_uv
    , coalesce(sx_p18_sc_uv,0)sx_p18_sc_uv
    , coalesce(big_sx_uv,0)big_sx_uv
    , coalesce(sum_sx_amt,0)sum_sx_amt
    , coalesce(first_sx_amt,0)first_sx_amt
    , coalesce(sx_p18_pp_amt,0)sx_p18_pp_amt
    , coalesce(sx_p18_sc_amt,0)sx_p18_sc_amt
    , coalesce(sx_uv_nonff,0)sx_uv_nonff
    , coalesce(first_sx_uv_nonff,0)first_sx_uv_nonff
    , coalesce(sum_sx_amt_nonff,0)sum_sx_amt_nonff
    , coalesce(first_sx_amt_nonff,0)first_sx_amt_nonff
    , coalesce(dz_uv,0)dz_uv
    , coalesce(dz_uv_nonff,0)dz_uv_nonff
    , coalesce(sum_first_dz_amt,0)sum_first_dz_amt
    , coalesce(sum_first_dz_amt_nonff,0)sum_first_dz_amt_nonff
    , coalesce(dz_t0_uv,0)dz_t0_uv
    , coalesce(dz_t0_uv_nonff,0)dz_t0_uv_nonff
    , coalesce(sum_t0_dz_amt,0)sum_t0_dz_amt
    , coalesce(sum_t0_dz_amt_nonff,0)sum_t0_dz_amt_nonff
    , coalesce(dz_wjt0_uv,0) dz_wjt0_uv
    , coalesce(dz_wjt0_uv_nonff,0) dz_wjt0_uv_nonff
    , coalesce(sum_wjt0_dz_amt,0) sum_wjt0_dz_amt
    , coalesce(sum_wjt0_dz_amt_nonff,0) sum_wjt0_dz_amt_nonff
    , coalesce(dz_wjt1_uv,0) dz_wjt1_uv
    , coalesce(dz_wjt1_uv_nonff,0) dz_wjt1_uv_nonff
    , coalesce(sum_wjt1_dz_amt,0) sum_wjt1_dz_amt
    , coalesce(sum_wjt1_dz_amt_nonff,0) sum_wjt1_dz_amt_nonff
    , coalesce(dz_wjt3_uv,0) dz_wjt3_uv
    , coalesce(dz_wjt3_uv_nonff,0) dz_wjt3_uv_nonff
    , coalesce(sum_wjt3_dz_amt,0) sum_wjt3_dz_amt
    , coalesce(sum_wjt3_dz_amt_nonff,0) sum_wjt3_dz_amt_nonff
    , coalesce(imp_cpm_cost,0) imp_cpm_cost
from
(
  select 
    p_day
  , media
  , bucket
  , config_id
  , strategys_name 
  from {{ ref('int_adtransactions_aggregated_to_media_bckt_accgroup_full_stg') }} funnel  group by 1,2,3,4,5

  union

  select 
    p_day 
    , media 
    , bucket
    , config_id
    , strategys_name 
  from {{ ref('int_rtarequest_media_bckt_accgroup_costs') }} spend group by 1,2,3,4,5

  union

  select 
    p_day 
    , media
    , bucket
    , config_id
    , strategys_name 
  from {{ ref('int_rtarequest_media_bckt_accgroup_pvuv') }} request where config_id not in ('','无') and config_id is not null group by 1,2,3,4,5
) a
left join funnel f  on a.p_day=f.p_day and a.media=f.media and a.bucket=f.bucket and a.config_id=f.config_id and a.strategys_name=f.strategys_name
left join spend s on a.p_day=s.p_day and a.media=s.media and a.bucket=s.bucket and a.config_id=s.config_id and a.strategys_name=s.strategys_name
left join request r on a.p_day=r.p_day and a.media=r.media and a.bucket=r.bucket and a.config_id=r.config_id and a.strategys_name=r.strategys_name

)

select * from incremental_table_data

