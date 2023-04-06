{{ config(materialized='view') }}

with back as (
    select
        p_day, 
        p_resource_code, 
        win_config_id, 
        req_bucket, 
        count(distinct ldp_userno) lz_uv,
        count(distinct IF(is_wj and if_jt=1,ldp_userno,null)) wj_uv
      , count(distinct IF(is_first_wj and if_jt=1,ldp_userno,null)) first_wj_uv
      , count(distinct IF(is_wj_t0 and if_jt=1,ldp_userno,null)) wj_t0_uv
      , count(distinct IF(is_sx and if_jt=1,ldp_userno,null)) sx_uv
      , count(distinct IF(is_first_sx and if_jt=1,ldp_userno,null)) first_sx_uv
      , count(distinct IF(is_sx_t0 and if_jt=1,ldp_userno,null)) sx_t0_uv
      , count(distinct IF(is_sx_p18_pp and if_jt=1,ldp_userno,null)) sx_p18_pp_uv
      , count(distinct IF(is_sx_p18_sc and if_jt=1,ldp_userno,null)) sx_p18_sc_uv
      , count(distinct IF(is_big_sx and if_jt=1,ldp_userno,null)) big_sx_uv
      , count(distinct IF(is_sx and has_fenfa=0 and if_jt=1,ldp_userno,null)) sx_uv_nonff
      , count(distinct IF(is_first_sx and has_fenfa=0 and if_jt=1,ldp_userno,null)) first_sx_uv_nonff
      , sum(IF(if_jt=1,sx_amt,0)) sum_sx_amt
      , sum(IF(is_first_sx and if_jt=1,sx_amt,0)) first_sx_amt
      , sum(IF(is_sx_p18_pp and if_jt=1,sx_amt,0)) sx_p18_pp_amt
      , sum(IF(is_sx_p18_sc and if_jt=1,sx_amt,0)) sx_p18_sc_amt
      , sum(case when has_fenfa=0 and if_jt=1 then sx_amt else 0 end) sum_sx_amt_nonff
      , sum(case when is_first_sx=1 and has_fenfa=0 and if_jt=1 then sx_amt else 0 end) first_sx_amt_nonff
      , count(distinct IF(is_dz and if_jt=1,ldp_userno,null)) dz_uv
      , count(distinct IF(is_dz and has_fenfa=0 and if_jt=1,ldp_userno,null)) dz_uv_nonff
      , sum(IF(is_first_sx=1 and if_jt=1,first_dz_amt,0)) sum_first_dz_amt
      , sum(case when is_first_sx=1 and has_fenfa=0 and if_jt=1 then first_dz_amt else 0 end) sum_first_dz_amt_nonff
      , count(distinct IF(is_dz_t0 and if_jt=1,ldp_userno,null)) dz_t0_uv
      , count(distinct IF(is_dz_t0 and if_jt=1 and has_fenfa=0,ldp_userno,null)) dz_t0_uv_nonff
      , sum(IF(is_dz_t0 and if_jt=1,first_dz_amt,0)) sum_t0_dz_amt
      , sum(case when is_dz_t0=1 and has_fenfa=0 and if_jt=1 then first_dz_amt else 0 end) sum_t0_dz_amt_nonff

      , count(distinct IF(is_wj_dz_t0 and if_jt=1,ldp_userno,null)) dz_wjt0_uv
      , count(distinct IF(is_wj_dz_t0 and if_jt=1 and has_fenfa=0,ldp_userno,null)) dz_wjt0_uv_nonff
      , sum(IF(is_wj_dz_t0 and if_jt=1,wj_dz_t0_amt,0)) sum_wjt0_dz_amt
      , sum(case when is_wj_dz_t0=1 and has_fenfa=0 and if_jt=1 then wj_dz_t0_amt else 0 end) sum_wjt0_dz_amt_nonff

      , count(distinct IF(is_wj_dz_t1 and if_jt=1,ldp_userno,null)) dz_wjt1_uv
      , count(distinct IF(is_wj_dz_t1 and if_jt=1 and has_fenfa=0,ldp_userno,null)) dz_wjt1_uv_nonff
      , sum(IF(is_wj_dz_t1 and if_jt=1,wj_dz_t1_amt,0)) sum_wjt1_dz_amt
      , sum(case when is_wj_dz_t1=1 and has_fenfa=0 and if_jt=1 then wj_dz_t1_amt else 0 end) sum_wjt1_dz_amt_nonff

      , count(distinct IF(is_wj_dz_t3 and if_jt=1,ldp_userno,null)) dz_wjt3_uv
      , count(distinct IF(is_wj_dz_t3 and if_jt=1 and has_fenfa=0,ldp_userno,null)) dz_wjt3_uv_nonff
      , sum(IF(is_wj_dz_t3 and if_jt=1,wj_dz_t3_amt,0)) sum_wjt3_dz_amt
      , sum(case when is_wj_dz_t3=1 and has_fenfa=0 and if_jt=1 then wj_dz_t3_amt else 0 end) sum_wjt3_dz_amt_nonff

    from(
        select *,
        case when sx_product_names like '%RJ%'
         or sx_product_names like '%上海大额%'
         or sx_product_names like '%上海小额%'
         or sx_product_names like '%北京小额%'
         or sx_product_names is null
         or sx_product_names='NULL'
        then 1 else 0 end as if_jt
        from {{ source('ad_transactions_sources', 'dm_eco_dataeco_ad_transations_0_4_1') }} 
        where p_day >=date_format(DATE_SUB(STR_TO_DATE('{{ var("pday") }}', "%Y-%m-%d"), INTERVAL 7 DAY),'%Y-%m-%d')
        and  p_day < date_format(DATE_ADD(STR_TO_DATE('{{ var("pday") }}', "%Y-%m-%d"), INTERVAL 1 DAY),'%Y-%m-%d')
        and ldp_userno is not null
    ) a
    group by 1,2,3,4
)

select * from back
