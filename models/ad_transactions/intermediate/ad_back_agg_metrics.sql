{{ config(materialized='view') }}

with ad_back as (
    select
        p_day
      , p_resource_code
      , win_config_id
      , req_bucket
      , count(distinct ldp_userno) lz_uv
      , count(distinct IF(is_wj and if_jt=1,ldp_userno,null)) wj_uv
      , count(distinct IF(is_first_wj and if_jt=1,ldp_userno,null)) first_wj_uv
      , count(distinct IF(is_wj_t0 and if_jt=1,ldp_userno,null)) wj_t0_uv
      , count(distinct IF(is_sx and if_jt=1,ldp_userno,null)) sx_uv
      , count(distinct IF(is_first_sx and if_jt=1,ldp_userno,null)) first_sx_uv
      , count(distinct IF(is_sx_t0 and if_jt=1,ldp_userno,null)) sx_t0_uv
      , count(distinct IF(is_sx_p18_pp and if_jt=1,ldp_userno,null)) sx_p18_pp_uv
      , count(distinct IF(is_sx_p18_sc and if_jt=1,ldp_userno,null)) sx_p18_sc_uv
      , count(distinct IF(is_big_sx and if_jt=1,ldp_userno,null)) big_sx_uv
      , count(distinct IF(is_sx and has_fenfa='false' and if_jt=1,ldp_userno,null)) sx_uv_nonff
      , count(distinct IF(is_first_sx and has_fenfa='false' and if_jt=1,ldp_userno,null)) first_sx_uv_nonff
      , sum(IF(if_jt=1,sx_amt,0)) sum_sx_amt
      , sum(IF(is_first_sx and if_jt=1,sx_amt,0)) first_sx_amt
      , sum(IF(is_sx_p18_pp and if_jt=1,sx_amt,0)) sx_p18_pp_amt
      , sum(IF(is_sx_p18_sc and if_jt=1,sx_amt,0)) sx_p18_sc_amt
      , sum(case when has_fenfa='false' and if_jt=1 then sx_amt else 0 end) sum_sx_amt_nonff
      , sum(case when is_first_sx='true' and has_fenfa='false' and if_jt=1 then sx_amt else 0 end) first_sx_amt_nonff
      , count(distinct IF(is_dz and if_jt=1,ldp_userno,null)) dz_uv
      , count(distinct IF(is_dz and has_fenfa='false' and if_jt=1,ldp_userno,null)) dz_uv_nonff
      , sum(IF(is_first_sx='true' and if_jt=1,first_dz_amt,0)) sum_first_dz_amt
      , sum(case when is_first_sx='true' and has_fenfa='false' and if_jt=1 then first_dz_amt else 0 end) sum_first_dz_amt_nonff
      , count(distinct IF(is_dz_t0 and if_jt=1,ldp_userno,null)) dz_t0_uv
      , count(distinct IF(is_dz_t0 and if_jt=1 and has_fenfa='false',ldp_userno,null)) dz_t0_uv_nonff
      , sum(IF(is_dz_t0 and if_jt=1,first_dz_amt,0)) sum_t0_dz_amt
      , sum(case when is_dz_t0='true' and has_fenfa='false' and if_jt=1 then first_dz_amt else 0 end) sum_t0_dz_amt_nonff

      , count(distinct IF(is_wj_dz_t0 and if_jt=1,ldp_userno,null)) dz_wjt0_uv
      , count(distinct IF(is_wj_dz_t0 and if_jt=1 and has_fenfa='false',ldp_userno,null)) dz_wjt0_uv_nonff
      , sum(IF(is_wj_dz_t0 and if_jt=1,wj_dz_t0_amt,0)) sum_wjt0_dz_amt
      , sum(case when is_wj_dz_t0='true' and has_fenfa='false' and if_jt=1 then wj_dz_t0_amt else 0 end) sum_wjt0_dz_amt_nonff

      , count(distinct IF(is_wj_dz_t1 and if_jt=1,ldp_userno,null)) dz_wjt1_uv
      , count(distinct IF(is_wj_dz_t1 and if_jt=1 and has_fenfa='false',ldp_userno,null)) dz_wjt1_uv_nonff
      , sum(IF(is_wj_dz_t1 and if_jt=1,wj_dz_t1_amt,0)) sum_wjt1_dz_amt
      , sum(case when is_wj_dz_t1='true' and has_fenfa='false' and if_jt=1 then wj_dz_t1_amt else 0 end) sum_wjt1_dz_amt_nonff

      , count(distinct IF(is_wj_dz_t3 and if_jt=1,ldp_userno,null)) dz_wjt3_uv
      , count(distinct IF(is_wj_dz_t3 and if_jt=1 and has_fenfa='false',ldp_userno,null)) dz_wjt3_uv_nonff
      , sum(IF(is_wj_dz_t3 and if_jt=1,wj_dz_t3_amt,0)) sum_wjt3_dz_amt
      , sum(case when is_wj_dz_t3='true' and has_fenfa='false' and if_jt=1 then wj_dz_t3_amt else 0 end) sum_wjt3_dz_amt_nonff

    from (

        select * from (
            select *,
                case when sx_product_names like '%RJ%'
                 or sx_product_names like '%上海大额%'
                 or sx_product_names like '%上海小额%'
                 or sx_product_names like '%北京小额%'
                 or sx_product_names is null
                 or sx_product_names='NULL'
                then 1 else 0 end as if_jt
                ,row_number() over (partition by p_day, p_resource_code, win_config_id, req_bucket, ldp_userno order by rand(123)) ct
                from {{ source('ad_transactions_sources', 'ad_trans_baidu_feb_03071') }} 
                where p_day >= date_format(date_sub(date('{{ var("pday") }}') ,7),'yyyyMMdd')
                and   p_day < date_format(date_add(date('{{ var("pday") }}') ,1),'yyyyMMdd')
                and ldp_userno is not null
            )
        where ct = 1 
    ) temp_table
    group by 1,2,3,4
)

select * from ad_back



