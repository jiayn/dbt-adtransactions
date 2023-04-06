{{ config(materialized='table') }}

with stgy as ( 
    select
        config_id,
        bucket,
        input_date,
        date_format(input_date,'yyyyMMdd')input_pday,
        strategys_name strategys_name
    from
        (
            select strategys_name, config_id, input_date, bucket
            from fin_dim.dim_m_audience_group_rta_group_config_daily_jwm_pdi LATERAL VIEW EXPLODE(split(buckets, ",")) as bucket
            where input_date >=date_sub(date('${pDate}') ,8)
        )
    group by 1,2,3,4
)

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
    lz_uv, wj_uv, first_wj_uv, wj_t0_uv, sx_uv, first_sx_uv, sx_t0_uv, sx_p18_pp_uv, sx_p18_sc_uv, big_sx_uv, sum_sx_amt, first_sx_amt, sx_p18_pp_amt, sx_p18_sc_amt, sx_uv_nonff, first_sx_uv_nonff, sum_sx_amt_nonff, first_sx_amt_nonff, dz_uv, dz_uv_nonff, sum_first_dz_amt, sum_first_dz_amt_nonff, dz_t0_uv, dz_t0_uv_nonff, sum_t0_dz_amt, sum_t0_dz_amt_nonff,
    dz_wjt0_uv, dz_wjt0_uv_nonff, sum_wjt0_dz_amt, sum_wjt0_dz_amt_nonff, dz_wjt1_uv, dz_wjt1_uv_nonff, sum_wjt1_dz_amt, sum_wjt1_dz_amt_nonff, dz_wjt3_uv, dz_wjt3_uv_nonff, sum_wjt3_dz_amt, sum_wjt3_dz_amt_nonff
,imp_cpm_cost
from {{ ref('front_agg_metrics') }} as front
full join {{ ref('back_agg_metrics') }} as back on
    front.p_resource_code = back.p_resource_code and
    front.win_config_id = back.win_config_id and
    front.req_bucket = back.req_bucket and
    front.p_day = back.p_day
left join stgy on front.win_config_id =  stgy.config_id and front.req_bucket =  stgy.bucket --and front.p_day =  stgy.input_pday
  and from_unixtime(unix_timestamp(stgy.input_pday,'yyyyMMdd'),'yyyy-MM-dd')=date_sub(from_unixtime(unix_timestamp(front.p_day,'yyyyMMdd'),'yyyy-MM-dd'),1)
;