{{ config(materialized = 'view' )}}

with renamed as (
  select
    a.p_day as pday
    , case when p_resource_code='tt_rta' and config_id in ('42','13') then '穿山甲'----'10785' 20220624去掉 ,对应configid 63
           when p_resource_code='tt_rta' and(config_id not in ('42','13')or config_id is null) then '头条' ----'10785' 20220624去掉 ,对应configid 63
           when p_resource_code='ks_rta' then '快手'
           when p_resource_code='tencent_gdt_rta' then '广点通'
           when p_resource_code='baidu_rta' then '百度'
           when p_resource_code='BAIDUKP_rta' then '百度开屏'
           else p_resource_code end as media
    , coalesce(a.bucket,'无')bucket
    , coalesce(a.config_id,'无')config_id
    , coalesce(a.strategys_name,'无')strategys_name
    , imp_pv
    , imp_uv
    , clk_pv
    , clk_uv
    , lz_uv
    , wj_uv
    , first_wj_uv
    , wj_t0_uv
    , sx_uv
    , first_sx_uv
    , sx_t0_uv
    , sx_p18_pp_uv
    , sx_p18_sc_uv
    , big_sx_uv
    , sum_sx_amt
    , first_sx_amt
    , sx_p18_pp_amt
    , sx_p18_sc_amt
    , sx_uv_nonff
    , first_sx_uv_nonff
    , sum_sx_amt_nonff
    , first_sx_amt_nonff
    , dz_uv
    , dz_uv_nonff
    , sum_first_dz_amt
    , sum_first_dz_amt_nonff
    , dz_t0_uv
    , dz_t0_uv_nonff
    , sum_t0_dz_amt
    , sum_t0_dz_amt_nonff
    , dz_wjt0_uv
    , dz_wjt0_uv_nonff
    , sum_wjt0_dz_amt
    , sum_wjt0_dz_amt_nonff
    , dz_wjt1_uv
    , dz_wjt1_uv_nonff
    , sum_wjt1_dz_amt
    , sum_wjt1_dz_amt_nonff
    , dz_wjt3_uv
    , dz_wjt3_uv_nonff
    , sum_wjt3_dz_amt
    , sum_wjt3_dz_amt_nonff
    , imp_cpm_cost
from {{ ref('int_adtransactions_aggregated_to_media_bckt_accgroup_full') }} 
where      p_day >= date_format(date_sub(date('{{ var("pday") }}') ,7),'yyyyMMdd')
      and  p_day < date_format(date_add(date('{{ var("pday") }}') ,1),'yyyyMMdd') 
      and  p_resource_code in ('ks_rta','tt_rta','tencent_gdt_rta','tencent_wx_rta','baidu_rta','BAIDUKP_rta')--V7
)

select * from renamed

