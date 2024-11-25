CREATE OR REPLACE VIEW COMP_SALES_CHANNEL_WEEK_LEVEL (
BRAND_ID,
OWNERSHIP_TYPE,
OWNER_ID,
REST_ID,
CHANNEL_ID,
ORDER_CHANNEL,
FULFILLMENT_CHANNEL,
COMP_TYPE,
FISC_YR_NBR,
FISC_PERIOD_NBR,
FISC_WK_NBR,
WK_START_DATE,
WK_START_DATE_PREV_YR,
IS_COMP_WK_IND,
IS_COMP_PERIOD_IND,
IS_COMPLETE_WK_IND,
DERIVED_NET_SALES_AMT,
DERIVED_NET_TRANS_QTY,
COMP_SALES_THIS_YR_CURR_YR,
COMP_SALES_LAST_YR_CURR_YR,
COMP_TRANS_THIS_YR_CURR_YR,
COMP_TRANS_LAST_YR_CURR_YR,
COMP_SALES_THIS_YR_PREV_YR,
COMP_SALES_LAST_YR_PREV_YR,
COMP_TRANS_THIS_YR_PREV_YR,
COMP_TRANS_LAST_YR_PREV_YR,
COMP_SALES_THIS_YR_PREV_PREV_YR,
COMP_SALES_LAST_YR_PREV_PREV_YR,
COMP_TRANS_THIS_YR_PREV_PREV_YR,
COMP_TRANS_LAST_YR_PREV_PREV_YR
) as
with eligibility_week as (
    select
        wk.brand_id,
        wk.rest_id,
        wk.comp_sales_type,
        wk.yr_nbr,
        wk.period_nbr,
        wk.wk_nbr,
        wk.comp_sales_weekly_eligible_ind,
        period.comp_sales_period_eligible_ind,
        wk.complete_wk_ind,
        last_wk.yr_nbr as last_yr,
        last_wk.comp_sales_weekly_eligible_ind as last_yr_eleg,
        prev_wk.yr_nbr as prev_yr,
        prev_wk.comp_sales_weekly_eligible_ind as prev_yr_eleg,
        prev_prev_wk.yr_nbr as prev_prev_yr,
        prev_prev_wk.comp_sales_weekly_eligible_ind as prev_prev_yr_eleg
    from
        idm_%%envname%%.comp_sales.comp_sales_rest_eligibility_weekly wk
        left join idm_%%envname%%.comp_sales.comp_sales_rest_eligibility_weekly last_wk on wk.comp_yr_nbr = last_wk.yr_nbr
        and wk.wk_nbr = last_wk.wk_nbr
        and wk.brand_id = last_wk.brand_id
        and wk.rest_id = last_wk.rest_id
        and iff(wk.comp_sales_type ='FLEX',
            last_wk.comp_sales_type='CAL',
            wk.comp_sales_type = last_wk.comp_sales_type)
        left join idm_%%envname%%.comp_sales.comp_sales_rest_eligibility_weekly prev_wk on last_wk.comp_yr_nbr = prev_wk.yr_nbr
        and last_wk.wk_nbr = prev_wk.wk_nbr
        and last_wk.brand_id = prev_wk.brand_id
        and last_wk.rest_id = prev_wk.rest_id
        and last_wk.comp_sales_type = prev_wk.comp_sales_type
        left join idm_%%envname%%.comp_sales.comp_sales_rest_eligibility_weekly prev_prev_wk on prev_wk.comp_yr_nbr = prev_prev_wk.yr_nbr
        and prev_wk.wk_nbr = prev_prev_wk.wk_nbr
        and prev_wk.brand_id = prev_prev_wk.brand_id
        and prev_wk.rest_id = prev_prev_wk.rest_id
        and prev_wk.comp_sales_type = prev_prev_wk.comp_sales_type
        left join idm_%%envname%%.comp_sales.comp_sales_rest_eligibility_period period on wk.yr_nbr = period.yr_nbr
        and wk.period_nbr = period.period_nbr
        and wk.brand_id = period.brand_id
        and wk.rest_id = period.rest_id
        and iff(wk.comp_sales_type ='FLEX',
            period.comp_sales_type='CAL',
            wk.comp_sales_type = period.comp_sales_type)
),
elegibility_week_with_date as (
    select
        DISTINCT eligibility_week.*,
        curr_date.fiscal_week_start_dt as WK_START_DATE,
        curr_date.fiscal_week_end_dt as WK_END_DATE,
        last_date.fiscal_week_start_dt as LAST_WK_START_DATE,
        last_date.fiscal_week_end_dt as LAST_WK_END_DATE,
        prev_date.fiscal_week_start_dt as PREV_WK_START_DATE,
        prev_date.fiscal_week_end_dt as PREV_WK_END_DATE,
        prev_prev_date.fiscal_week_start_dt as PREV_PREV_WK_START_DATE,
        prev_prev_date.fiscal_week_end_dt as PREV_PREV_WK_END_DATE
    from
        eligibility_week
        left join ids_%%envname%%.int_ref.date_dim curr_date ON curr_date.fiscal_year_nbr = eligibility_week.yr_nbr
        and curr_date.fiscal_week_nbr = eligibility_week.wk_nbr
        left join ids_%%envname%%.int_ref.date_dim last_date ON last_date.fiscal_year_nbr = eligibility_week.last_yr
        and last_date.fiscal_week_nbr = eligibility_week.wk_nbr
        left join ids_%%envname%%.int_ref.date_dim prev_date ON prev_date.fiscal_year_nbr = eligibility_week.prev_yr
        and prev_date.fiscal_week_nbr = eligibility_week.wk_nbr
        left join ids_%%envname%%.int_ref.date_dim prev_prev_date ON prev_prev_date.fiscal_year_nbr = eligibility_week.prev_prev_yr
        and prev_prev_date.fiscal_week_nbr = eligibility_week.wk_nbr
),
max_owner_by_store_in_week as (
    select
        eleg.brand_id,
        eleg.rest_id,
        eleg.WK_START_DATE,
        eleg.WK_END_DATE,
        rest.owner_id,
        rest.ownership_typ,
        row_number() over (
            partition by rest.brand_id,
            rest.store_id
            order by
                effective_end_dt desc NULLS FIRST
        ) as newest_dup
    from
        elegibility_week_with_date eleg
        LEFT JOIN IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest ON rest.store_id = eleg.rest_id
        and rest.brand_id = eleg.brand_id
        and greatest(rest.effective_begin_dt, eleg.WK_START_DATE) >= eleg.WK_START_DATE
        and LEAST(rest.effective_end_dt, eleg.WK_END_DATE) <= eleg.WK_END_DATE
        QUALIFY newest_dup <= 1
),
daypart_by_channel as (
    select
        daypart.*,
        channel.order_channel_hierarchy_l3_nm as ORDER_CHANNEL,
        channel.FULFILLMENT_CHANNEL_HIERARCHY_L3_NM as FULFILLMENT_CHANNEL
    from
   (
        select
            dp.brand_id,
            dp.rest_id,
            date_dim.fiscal_year_nbr yr_nbr,
            date_dim.fiscal_week_nbr wk_nbr,
            dp.order_channel_id,
            SUM(derived_net_sales_usd_amount) as DERIVED_NET_SALES_AMT,
            SUM(trans_qty) as DERIVED_NET_TRANS_QTY
        from
            idm_%%envname%%.comp_sales.comp_sales_rest_daypart_sales dp
            inner join ids_%%envname%%.int_ref.date_dim date_dim on business_date = calendar_dt
        group by
            1,
            2,
            3,
            4,
            5
    ) as daypart
    inner join IDM_%%envname%%.coredim_bv.channel_dim_bv channel on channel.channel_id = daypart.order_channel_id
),

preagg as (
    select
        elegibility_week_with_date.*,
        max_owner_by_store_in_week.owner_id,
        max_owner_by_store_in_week.ownership_typ,
        daypart_by_channel.ORDER_CHANNEL,
        daypart_by_channel.FULFILLMENT_CHANNEL,
        daypart_by_channel.ORDER_CHANNEL_ID,
        daypart_by_channel.DERIVED_NET_SALES_AMT,
        daypart_by_channel.DERIVED_NET_TRANS_QTY,
        last.DERIVED_NET_SALES_AMT LAST_DERIVED_NET_SALES_AMT,
        last.DERIVED_NET_TRANS_QTY LAST_DERIVED_NET_TRANS_QTY,
        prev.DERIVED_NET_SALES_AMT PREV_DERIVED_NET_SALES_AMT,
        prev.DERIVED_NET_TRANS_QTY PREV_DERIVED_NET_TRANS_QTY,
        prev_prev.DERIVED_NET_SALES_AMT PREV_PREV_DERIVED_NET_SALES_AMT,
        prev_prev.DERIVED_NET_TRANS_QTY PREV_PREV_DERIVED_NET_TRANS_QTY
    from
        elegibility_week_with_date
        INNER JOIN max_owner_by_store_in_week USING(brand_id, rest_id, WK_START_DATE, WK_END_DATE)
        LEFT JOIN daypart_by_channel ON daypart_by_channel.brand_id = elegibility_week_with_date.brand_id
        AND daypart_by_channel.rest_id = elegibility_week_with_date.rest_id
        AND daypart_by_channel.yr_nbr = elegibility_week_with_date.yr_nbr
        AND daypart_by_channel.wk_nbr = elegibility_week_with_date.wk_nbr
        LEFT JOIN daypart_by_channel last ON last.brand_id = elegibility_week_with_date.brand_id
        AND last.rest_id = elegibility_week_with_date.rest_id
        AND last.yr_nbr = elegibility_week_with_date.last_yr
        AND last.wk_nbr = elegibility_week_with_date.wk_nbr
        LEFT JOIN daypart_by_channel prev ON prev.brand_id = elegibility_week_with_date.brand_id
        AND prev.rest_id = elegibility_week_with_date.rest_id
        AND prev.yr_nbr = elegibility_week_with_date.prev_yr
        AND prev.wk_nbr = elegibility_week_with_date.wk_nbr
        LEFT JOIN daypart_by_channel prev_prev ON prev_prev.brand_id = elegibility_week_with_date.brand_id
        AND prev_prev.rest_id = elegibility_week_with_date.rest_id
        AND prev_prev.yr_nbr = elegibility_week_with_date.prev_prev_yr
        AND prev_prev.wk_nbr = elegibility_week_with_date.wk_nbr
)
select
    preagg.BRAND_ID,
    CASE
        WHEN lower(preagg.OWNERSHIP_TYP) IN (
            'franchised',
            'territory franchise agreement',
            'direct-non-franchise tenant',
            'direct customer'
        ) THEN 'FRAN'
        WHEN lower(preagg.OWNERSHIP_TYP) IN ('company owned', 'company operated') THEN 'ICR'
        ELSE 'N/A'
    END AS OWNERSHIP_TYPE,
    preagg.OWNER_ID,
    preagg.REST_ID,
    preagg.order_channel_id,
    coalesce(preagg.ORDER_CHANNEL, 'N/A') as ORDER_CHANNEL,
    coalesce(preagg.FULFILLMENT_CHANNEL, 'N/A') as FULFILLMENT_CHANNEL,
    COMP_SALES_TYPE as COMP_TYPE,
    YR_NBR as FISC_YR_NBR,
    PERIOD_NBR as FISC_PERIOD_NBR,
    WK_NBR as FISC_WK_NBR,
    WK_START_DATE,
    LAST_WK_START_DATE as WK_START_DATE_PREV_YR,
    COMP_SALES_WEEKLY_ELIGIBLE_IND as IS_COMP_WK_IND,
    comp_sales_period_eligible_ind as IS_COMP_PERIOD_IND,
    complete_wk_ind as IS_COMPLETE_WK_IND,
    DERIVED_NET_SALES_AMT,
    DERIVED_NET_TRANS_QTY,
    IFF(
        COMP_SALES_WEEKLY_ELIGIBLE_IND,
        DERIVED_NET_SALES_AMT,
        0
    ) as COMP_SALES_THIS_YR_CURR_YR,
    IFF(
        LAST_YR_ELEG
        and COMP_SALES_WEEKLY_ELIGIBLE_IND,
        LAST_DERIVED_NET_SALES_AMT,
        0
    ) as COMP_SALES_LAST_YR_CURR_YR,
    IFF(
        COMP_SALES_WEEKLY_ELIGIBLE_IND,
        DERIVED_NET_TRANS_QTY,
        0
    ) as COMP_TRANS_THIS_YR_CURR_YR,
    IFF(
        LAST_YR_ELEG
        and COMP_SALES_WEEKLY_ELIGIBLE_IND,
        LAST_DERIVED_NET_TRANS_QTY,
        0
    ) as COMP_TRANS_LAST_YR_CURR_YR,
    IFF(
        LAST_YR_ELEG,
        LAST_DERIVED_NET_SALES_AMT,
        0
    ) as COMP_SALES_THIS_YR_PREV_YR,
    IFF(
        LAST_YR_ELEG
        and PREV_YR_ELEG,
        PREV_DERIVED_NET_SALES_AMT,
        0
    ) as COMP_SALES_LAST_YR_PREV_YR,
    IFF(
        LAST_YR_ELEG,
        LAST_DERIVED_NET_TRANS_QTY,
        0
    ) as COMP_TRANS_THIS_YR_PREV_YR,
    IFF(
        LAST_YR_ELEG
        and PREV_YR_ELEG,
        PREV_DERIVED_NET_TRANS_QTY,
        0
    ) as COMP_TRANS_LAST_YR_PREV_YR,
    IFF(
        PREV_YR_ELEG,
        PREV_DERIVED_NET_SALES_AMT,
        0
    ) as COMP_SALES_THIS_YR_PREV_PREV_YR,
    IFF(
        PREV_YR_ELEG
        and PREV_PREV_YR_ELEG,
        PREV_PREV_DERIVED_NET_SALES_AMT,
        0
    ) as COMP_SALES_LAST_YR_PREV_PREV_YR,
    IFF(
        PREV_YR_ELEG,
        PREV_DERIVED_NET_TRANS_QTY,
        0
    ) as COMP_TRANS_THIS_YR_PREV_PREV_YR,
    IFF(
        PREV_YR_ELEG
        and PREV_PREV_YR_ELEG,
        PREV_PREV_DERIVED_NET_TRANS_QTY,
        0
    ) as COMP_TRANS_LAST_YR_PREV_PREV_YR
from
    preagg;