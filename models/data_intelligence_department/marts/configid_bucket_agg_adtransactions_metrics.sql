{{ config(
  materialized='table',
  incremental_strategy='insert_overwrite',
  partition_by=['p_day'], 
  file_format='orc'
)}}

with configid_bucket_agg_adtransactions_metrics as (
  select
    nvl(front.p_day,back.p_day) as p_day,
    nvl(front.p_resource_code,back.p_resource_code) as p_resource_code,
    nvl(front.win_config_id,back.win_config_id) as config_id,
    nvl(front.req_bucket,back.req_bucket) as bucket,
    strategys_name,
    imp_pv,
    imp_uv,
    clk_pv,
    clk_uv,
    --后端
    lz_uv, wj_uv, first_wj_uv, wj_t0_uv, sx_uv, first_sx_uv, sx_t0_uv, sx_p18_pp_uv, sx_p18_sc_uv, big_sx_uv, sum_sx_amt, first_sx_amt, sx_p18_pp_amt, sx_p18_sc_amt, sx_uv_nonff, first_sx_uv_nonff, sum_sx_amt_nonff, first_sx_amt_nonff, dz_uv, dz_uv_nonff, sum_first_dz_amt, sum_first_dz_amt_nonff, dz_t0_uv, dz_t0_uv_nonff, sum_t0_dz_amt, sum_t0_dz_amt_nonff,
    --新增
    dz_wjt0_uv, dz_wjt0_uv_nonff, sum_wjt0_dz_amt, sum_wjt0_dz_amt_nonff, dz_wjt1_uv, dz_wjt1_uv_nonff, sum_wjt1_dz_amt, sum_wjt1_dz_amt_nonff, dz_wjt3_uv, dz_wjt3_uv_nonff, sum_wjt3_dz_amt, sum_wjt3_dz_amt_nonff,imp_cpm_cost
from {{ ref('int_adtransactions_aggregated_to_configid_bucket_front') }} as front
full join {{ ref('int_adtransactions_aggregated_to_configid_bucket_back') }} as back 
on
    front.p_resource_code = back.p_resource_code and
    front.win_config_id = back.win_config_id and
    front.req_bucket = back.req_bucket and
    front.p_day = back.p_day
left join {{ ref('stg_glaucus__rta_bucket_config') }} as stgy 
on  front.win_config_id =  stgy.config_id 
    and front.req_bucket =  stgy.bucket --and front.p_day =  stgy.input_pday
    and from_unixtime(unix_timestamp(stgy.input_pday,'yyyyMMdd'),'yyyy-MM-dd')=date_sub(from_unixtime(unix_timestamp(front.p_day,'yyyyMMdd'),'yyyy-MM-dd'),1)
)

select * from configid_bucket_agg_adtransactions_metrics
