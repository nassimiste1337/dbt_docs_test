
  create or replace   view PRFR_00_DEV_20_SILVER_DB.PRFR.project_inventory
  
    
    
(
  
    "SITE_CD" COMMENT $$$$, 
  
    "INV_QTY_QUA_D_BT" COMMENT $$$$, 
  
    "INV_QTY_D_BT" COMMENT $$$$, 
  
    "ITEM_ID" COMMENT $$$$, 
  
    "PROJECTED_TIME_ID" COMMENT $$$$, 
  
    "PO_QTY_D_BT" COMMENT $$$$, 
  
    "PO_QTY_TD_BT" COMMENT $$$$, 
  
    "ORDER_QTY_D_BT" COMMENT $$$$, 
  
    "ORDER_QTY_TD_BT" COMMENT $$$$, 
  
    "OT_QTY_D_BT" COMMENT $$$$, 
  
    "OT_QTY_TD_BT" COMMENT $$$$, 
  
    "WORK_ORDER_QTY_D_BT" COMMENT $$$$, 
  
    "WORK_ORDER_QTY_TD_BT" COMMENT $$$$, 
  
    "WORK_ORDER_COPACK_QTY_D_BT" COMMENT $$$$, 
  
    "WORK_ORDER_COPACK_QTY_TD_BT" COMMENT $$$$, 
  
    "ST_QTY_D_BT" COMMENT $$$$, 
  
    "ST_QTY_TD_BT" COMMENT $$$$
  
)

   as (
    /*with
    calendar as (
        select dt_ddmmyyyy
        from
            prfr_00_DEV_20_silver_db.prfr_parameters_sch.dwh_calendar_sat
    ),

    item as (
        select item_id
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_ref_item_sch.dwh_item_hub item_hub
    --    qualify
     --       row_number() over (
      --          partition by item_hub.item_key order by item_hub.ldts desc) = 1
            ),
    
            
    stock_ledger_inventory_qua as (
        select site_cd, item_id, site_location_cd, mvmt_qty_puom,mvmt_dt
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_ref_stock_sch.dwh_stock_ledger_inventory_sat stock_l_i where site_location_cd ='QUA' and mvmt_dt > current_date()
     --   qualify
        --    row_number() over (
        --        partition by stock_l_i.item_ledger_inv_key order by stock_l_i.ldts desc)
         --   = 1) , 
         ),
    stock_ledger_inventory_ex as (
        select site_cd, item_id, site_location_cd, mvmt_qty_puom,mvmt_dt
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_ref_stock_sch.dwh_stock_ledger_inventory_sat stock_l_i where site_location_cd not in ('QUA', 'PNC', 'DES')  and mvmt_dt > current_date()
     --   qualify
        --    row_number() over (
        --        partition by stock_l_i.item_ledger_inv_key order by stock_l_i.ldts desc)
         --   = 1) , 
         ),
   

    orders_line_ind as (
        select
            orders_line_hub.order_line_key as hub_id,
            ind_qty_order_transaction,
            ind_delivered_qty,
            item_id
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_sales_sch.dwh_orders_line_hub orders_line_hub
        inner join
            prfr_00_DEV_20_silver_db.prfr_dwh_sales_sch.dwh_orders_line_ind_sat orders_line_sat
            on orders_line_hub.order_line_key = orders_line_sat.order_line_key
     --   qualify
      --      row_number() over (
        --        partition by orders_line_sat.order_line_key
        --        order by orders_line_sat.ldts desc)
            
       --     = 1
    ),
    work_order as (
        select item_id, WRK_ORDER_QTY, PRODUCED_QTY
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_ref_stock_sch.PRFR_DWH_REF_PRODUCTION_SCH.DWH_WORK_ORDER_SAT
    ),
    work_order_cpck as (
        select item_id, WRK_ORDER_QTY, PRODUCED_QTY
        from
            prfr_00_DEV_20_silver_db.PRFR_DWH_REF_PRODUCTION_SCH.DWH_WORK_ORDER_SAT where COPACK_FLG = 1
    ),
    purchase_order_detail as (
        select
            order_qty_review_puom,
            delivered_qty_puom,
            puom_cd,
            item_id,
            order_dt,
            order_type_cd,
            qty_ordered,
            delevery_qty
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_ref_purchase_sch.dwh_purchase_order_detail_sat p_o_d
     --   qualify
       --     row_number() over (
        --        partition by p_o_d.po_detail_key order by p_o_d.ldts desc
        --    )
        --    = 1
    ),

    item_coeff_measure_unit as (
        select cnv_coeff_bt, item_id
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_ref_item_sch.dwh_item_coeff_measure_unit_sat item_c_m_u
       -- qualify
       --     row_number() over (
        --        partition by item_c_m_u.item_key order by item_c_m_u.ldts desc
        --    )
        --    = 1
    ),

    t as (
        select
            pod.item_id as item_id,
            current_date() as time_id,
            -- site_cd,
            dt_ddmmyyyy as projected_time_id,
                         case
                when puom_cd <> 'BT' then (sli_ex.mvmt_qty_puom * cnv_coeff_bt)::float
                else sli_ex.mvmt_qty_puom
            end as INV_QTY_D_BT,
            case
                when puom_cd <> 'BT' then (sli_qua.mvmt_qty_puom * cnv_coeff_bt)::float
                else sli_qua.mvmt_qty_puom
            end as inv_qty_qua_d_bt,
            order_dt,
            puom_cd,
            delivered_qty_puom,
            order_qty_review_puom,
            case
                when puom_cd <> 'BT'
                then
                    (ind_qty_order_transaction - ind_delivered_qty)
                    * cnv_coeff_bt::float
                else (ind_qty_order_transaction - ind_delivered_qty)::float
            end as order_qty_d_bt,
            case
                when puom_cd <> 'BT'
                then (order_qty_review_puom - delivered_qty_puom) * cnv_coeff_bt::float
                else (order_qty_review_puom - delivered_qty_puom)::float
            end as po_qty_d_bt,
            case
                when puom_cd <> 'BT'
                then (WRK_ORDER_QTY - PRODUCED_QTY) * cnv_coeff_bt::float
                else (WRK_ORDER_QTY - PRODUCED_QTY)::float
            end as WORK_ORDER_QTY_D_BT,
            case
                when puom_cd <> 'BT'
                then (work_order_cpck.WRK_ORDER_QTY - work_order_cpck.PRODUCED_QTY) * cnv_coeff_bt::float
                else (work_order_cpck.WRK_ORDER_QTY - work_order_cpck.PRODUCED_QTY)::float
            end as WORK_ORDER_COPACK_QTY_TD_BT,
            case
                when order_type_cd = 'OT' and puom_cd <> 'BT'
                then (qty_ordered - delevery_qty) * cnv_coeff_bt::float
                when order_type_cd = 'OT' and puom_cd = 'BT'
                then (qty_ordered - delevery_qty)::float
                else null
            end as ot_qty_d_bt,
            case
                when order_type_cd = 'ST' and puom_cd <> 'BT'
                then (qty_ordered - delevery_qty) * cnv_coeff_bt::float
                when order_type_cd = 'ST' and puom_cd = 'BT'
                then (qty_ordered - delevery_qty)::float
                else null
            end as st_qty_d_bt,


        from purchase_order_detail pod
        inner join item_coeff_measure_unit icm on pod.item_id = icm.item_id
        -- inner join stock_ledger_inventory sli on sli.item_id = pod.item_id
        inner join calendar cal on cal.dt_ddmmyyyy = pod.order_dt
            inner join work_order wo on wo.item_id=pod.item_id
            inner join stock_ledger_inventory_qua sli_qua on sli_qua.item_id = pod.item_id
            inner join stock_ledger_inventory_ex sli_ex on sli_ex.item_id = pod.item_id
        )

    select
        item_id,
        time_id,
        projected_time_id,
    -- inv_qty_d_bt,
    -- inv_qty_qua_d_bt,
    order_qty_d_bt,
    -- INV_QTY_D_BT  - ORDER_QTY_TD_BT  + PO_QTY_TD_BT  + WORK_ORDER_QTY_TD_BT  -
    -- WORK_ORDER_COPACK_QTY_TD_BT end as PROJECTED_INVENTORY_QTY_D_BT,
    (
        select sum(t1.order_qty_d_bt)
        from t t1
        where
            t1.order_dt between t.projected_time_id and dateadd('DAY', 1, t.time_id)
            and t.item_id = t1.item_id
    ) as order_qty_td_bt,
    po_qty_d_bt,
    (
        select sum(t1.po_qty_d_bt)
        from t t1
        where
            t1.order_dt between t.projected_time_id and dateadd('DAY', 1, t.time_id)
            and t.item_id = t1.item_id
    ) as po_qty_td_bt,
    st_qty_d_bt,
    (
        select sum(t1.st_qty_d_bt)
        from t t1
        where
            t1.order_dt between t.projected_time_id and dateadd('DAY', 1, t.time_id)
            and t.item_id = t1.item_id
    ) as st_qty_td_bt,
    ot_qty_d_bt,
    (
        select sum(t1.ot_qty_d_bt)
        from t t1
        where
            t1.order_dt between t.projected_time_id and dateadd('DAY', 1, t.time_id)
            and t.item_id = t1.item_id
    ) as ot_qty_td_bt

from t
*/











with
    calendar as (
        select distinct dt_ddmmyyyy
        from
            prfr_00_DEV_20_silver_db.prfr_parameters_sch.dwh_calendar_sat
    ),

    item as (
        select item_id
        from
            PRFR_00_DEV_30_GOLD_DB.PRFR_DWH_3NF_REF_ITEM_SCH.ITEM
       -- qualify
        --    row_number() over (
        --        partition by item_hub.item_key order by item_hub.ldts desc) = 1
            ),

    work_order as (
        select item_id, shipped_qty,site_cd, wrk_order_qty from PRFR_00_DEV_20_SILVER_DB.PRFR_DWH_REF_PRODUCTION_SCH.DWH_WORK_ORDER_SAT
    )
    
    
            ,

    stock_ledger_inventory as (
        select  site_cd, item_id, item_cd, site_location_cd, mvmt_qty_puom,mvmt_dt
        from
            PRFR_00_DEV_30_GOLD_DB.PRFR_DWH_3NF_STOCK_SCH.STOCK_LEDGER_INVENTORY
    ),
    orders_line_ind as (
        select
            orders_line_hub.order_line_key as hub_id,
            ind_qty_order_transaction,
            ind_delivered_qty,
            item_id
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_sales_sch.dwh_orders_line_hub orders_line_hub
        inner join
            prfr_00_DEV_20_silver_db.prfr_dwh_sales_sch.dwh_orders_line_ind_sat orders_line_sat
            on orders_line_hub.order_line_key = orders_line_sat.order_line_key
     ),

    purchase_order_detail as (
        select
        
            order_qty_review_puom,
            delivered_qty_puom,
            puom_cd,
            item_id,
            order_dt,
            order_type_cd,
            qty_ordered,
            delevery_qty,
            time_of_the_day
        from
            PRFR_00_DEV_30_GOLD_DB.PRFR_DWH_3NF_PURCHASE_SCH.PURCHASE_ORDER_DETAIL p_o_d

    ),

    item_coeff_measure_unit as (
        select distinct cnv_coeff_bt, item_id,item_cd
        from
            prfr_00_DEV_20_silver_db.prfr_dwh_ref_item_sch.dwh_item_coeff_measure_unit_sat item_c_m_u

    ),

    item_bottling as (
    select item_id,item_copack_FLG from PRFR_00_DEV_30_GOLD_DB.PRFR_DWH_3NF_REF_ITEM_SCH.ITEM_BOTTLING
    ),

    t as (
        select
            pod.item_id as item_id,
            current_date() as time_id,
            wo.site_cd,
            time_of_the_day as projected_time_id,
            
            case
                when puom_cd <> 'BT' then (sli_qua.mvmt_qty_puom * cnv_coeff_bt)::float
                else sli_qua.mvmt_qty_puom
            end as inv_qty_qua_d_bt,
             case
                when puom_cd <> 'BT' then (sli_ex.mvmt_qty_puom * cnv_coeff_bt)::float
                else sli_ex.mvmt_qty_puom
            end as INV_QTY_D_BT,
            order_dt,
            puom_cd,
            delivered_qty_puom,
            order_qty_review_puom,
            case
                when puom_cd <> 'BT'
                then (order_qty_review_puom - delivered_qty_puom) * cnv_coeff_bt::float
                else (order_qty_review_puom - delivered_qty_puom)::float
            end as po_qty_d_bt,
            case
                when puom_cd <> 'BT'
                then
                    (ind_qty_order_transaction - ind_delivered_qty)
                    * cnv_coeff_bt::float
                else (ind_qty_order_transaction - ind_delivered_qty)::float
            end as order_qty_d_bt,
            case
                when puom_cd <> 'BT'
                then
                    (WRK_ORDER_QTY-SHIPPED_QTY)
                    * cnv_coeff_bt::float
                else (WRK_ORDER_QTY-SHIPPED_QTY)::float
            end as WORK_ORDER_QTY_D_BT,
            case
                when puom_cd <> 'BT' and ITEM_COPACK_FLG=1
                then
                    (WRK_ORDER_QTY-SHIPPED_QTY)
                    * cnv_coeff_bt::float
                when puom_cd = 'BT' and ITEM_COPACK_FLG=1 then (WRK_ORDER_QTY-SHIPPED_QTY)::float
                else null
            end as WORK_ORDER_COPACK_QTY_D_BT,
            case
                when order_type_cd = 'ST' and puom_cd <> 'BT'
                then (qty_ordered - delevery_qty) * cnv_coeff_bt::float
                when order_type_cd = 'ST' and puom_cd = 'BT'
                then (qty_ordered - delevery_qty)::float
                else null
            end as st_qty_d_bt,
            case
                when order_type_cd = 'OT' and puom_cd <> 'BT'
                then (qty_ordered - delevery_qty) * cnv_coeff_bt::float
                when order_type_cd = 'OT' and puom_cd = 'BT'
                then (qty_ordered - delevery_qty)::float
                else null
            end as ot_qty_d_bt

        from purchase_order_detail pod
        inner join item_coeff_measure_unit icm on pod.item_id = icm.item_id
        left join stock_ledger_inventory sli_qua on sli_qua.item_id = pod.item_id and sli_qua.site_location_cd ='QUA' and sli_qua.mvmt_dt>current_date()
        left join stock_ledger_inventory sli_ex on sli_ex.item_id = pod.item_id and sli_ex.site_location_cd not in ('QUA', 'PNC', 'DES') and sli_ex.mvmt_dt>current_date()
        inner join calendar cal on cal.dt_ddmmyyyy = pod.time_of_the_day::DATE
        inner join orders_line_ind oli on oli.item_id=icm.item_cd
        inner join work_order wo on wo.item_id=pod.item_id
        inner join item_bottling ib on ib.item_id=pod.item_id
    ),
    p_i as (
    select distinct * from t
    ),
    --PO --ORDER  + PO + OT + WO + WOCPCK + ST
po_td_cte as (
select  distinct ITEM_ID, projected_time_id, PO_QTY_D_BT from p_i ),
po_td as (
select ITEM_ID,projected_time_id, PO_QTY_D_BT, SUM(PO_QTY_D_BT) OVER (PARTITION BY ITEM_ID ORDER BY projected_time_id desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) PO_QTY_TD_BT from po_td_cte),

order_td_cte as (
select  distinct ITEM_ID, projected_time_id, ORDER_QTY_D_BT from p_i ),
order_td as (
select ITEM_ID,projected_time_id, ORDER_QTY_D_BT, SUM(ORDER_QTY_D_BT) OVER (PARTITION BY ITEM_ID ORDER BY projected_time_id desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ORDER_QTY_TD_BT from order_td_cte  ),
ot_td_cte as (
select  distinct ITEM_ID, projected_time_id, OT_QTY_D_BT from p_i ),
ot_td as (
select ITEM_ID,projected_time_id, OT_QTY_D_BT, SUM(OT_QTY_D_BT) OVER (PARTITION BY ITEM_ID ORDER BY projected_time_id desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) OT_QTY_TD_BT from ot_td_cte ),
work_order_td_cte as (
select  distinct ITEM_ID, projected_time_id, work_order_QTY_D_BT from p_i ),
wo_td as (select ITEM_ID,projected_time_id, work_order_QTY_D_BT, SUM(work_order_QTY_D_BT) OVER (PARTITION BY ITEM_ID ORDER BY projected_time_id desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) work_order_QTY_TD_BT from work_order_td_cte), 
WORK_ORDER_COPACK_td_cte as (
select  distinct ITEM_ID, projected_time_id, WORK_ORDER_COPACK_QTY_D_BT from p_i ),
wo_cpck_td as (
select ITEM_ID,projected_time_id, WORK_ORDER_COPACK_QTY_D_BT, SUM(WORK_ORDER_COPACK_QTY_D_BT) OVER (PARTITION BY ITEM_ID ORDER BY projected_time_id desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) WORK_ORDER_COPACK_QTY_TD_BT from WORK_ORDER_COPACK_td_cte),
st_td_cte as (
select  distinct ITEM_ID, projected_time_id, ST_QTY_D_BT from p_i ),
st_td as (select ITEM_ID,projected_time_id, ST_QTY_D_BT, SUM(ST_QTY_D_BT) OVER (PARTITION BY ITEM_ID ORDER BY projected_time_id desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ST_QTY_TD_BT from st_td_cte )


select site_cd,INV_QTY_QUA_D_BT,INV_QTY_D_BT,po_td.ITEM_ID, po_td.projected_time_id, po_td.PO_QTY_D_BT, po_td.PO_QTY_TD_BT, otd.ORDER_QTY_D_BT, otd.ORDER_QTY_TD_BT,ot_td.OT_QTY_D_BT,ot_td.OT_QTY_TD_BT,wo_td.WORK_ORDER_QTY_D_BT,wo_td.WORK_ORDER_QTY_TD_BT,wo_cpck_td.WORK_ORDER_COPACK_QTY_D_BT,wo_cpck_td.WORK_ORDER_COPACK_QTY_TD_BT,st_td.ST_QTY_D_BT,st_td.ST_QTY_TD_BT from po_td 
inner join order_td otd on po_td.item_id=otd.item_id and po_td.projected_time_id=otd.projected_time_id
inner join ot_td on po_td.item_id=ot_td.item_id and po_td.projected_time_id=ot_td.projected_time_id
inner join wo_td on po_td.item_id=wo_td.item_id and po_td.projected_time_id=wo_td.projected_time_id
inner join wo_cpck_td on po_td.item_id=wo_cpck_td.item_id and po_td.projected_time_id=wo_cpck_td.projected_time_id
inner join st_td on po_td.item_id=st_td.item_id and po_td.projected_time_id=st_td.projected_time_id
inner join p_i pi on pi.item_id=po_td.item_id and pi.projected_time_id=po_td.projected_time_id
  );

