create or replace view COMP_SALES_DAILY_LEVEL(
	BRAND_ID,
	OWNERSHIP_TYPE,
	OWNER_ID,
	REST_ID,
	COMP_TYPE,
	FISC_YR_NBR,
	FISC_PERIOD_NBR,
	FISC_WK_NBR,
	BUSINESS_DATE,
	BUSINESS_DATE_PREV_YR,
	DAY_OF_WK_NAME,
	IS_COMP_DAY_IND,
	IS_COMP_WK_IND,
	IS_COMP_PERIOD_IND,
	DERIVED_NET_SALES_USD_AMOUNT,
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
with eligibility as (
    select
        *
    from
        IDM_%%envname%%.comp_sales.comp_sales_elegibility_comp_date
)
,max_rest_by_business_date as (
    select DISTINCT
            daily.brand_id,
            daily.rest_id,
            daily.business_date,
            (select max(rest.effective_end_dt)
             from IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
             where rest.store_id = daily.rest_id
             and rest.brand_id = daily.brand_id
             and rest.effective_begin_dt <= daily.business_date and daily.business_date < rest.effective_end_dt) effective_end_dt
    from eligibility daily
)
,daily_with_rest as (
    select DISTINCT
        rest.brand_id,
        daily.rest_id,
        daily.business_date,
        rest.OWNERSHIP_TYP,
        rest.owner_id,
        daily.yr_nbr,
        daily.period_nbr,
        daily.wk_nbr
    from IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_DAILY_BV daily
    left join IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
    on rest.store_id = daily.rest_id
    and rest.brand_id = daily.brand_id
    inner join max_rest_by_business_date max_edt
    on rest.store_id = max_edt.rest_id
    and rest.brand_id = max_edt.brand_id
    and daily.business_date = max_edt.business_date
    and rest.effective_end_dt = max_edt.effective_end_dt
),
daypart_sum as (
    SELECT
            BRAND_ID,
            REST_ID,
            BUSINESS_DATE,
            SUM(DERIVED_NET_SALES_USD_AMOUNT) DERIVED_NET_SALES_USD_AMOUNT,
            SUM(TRANS_QTY) DERIVED_NET_TRANS_QTY
        FROM IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_DAYPART_SALES_BV
        GROUP BY 1, 2, 3
),
elegibility_day_part as (
    select
        eligibility.brand_id,
        eligibility.rest_id,
        eligibility.COMP_SALES_TYPE,
        eligibility.BUSINESS_DATE,
        eligibility.LAST_BT,
        eligibility.COMP_SALES_WEEKLY_ELIGIBLE_IND,
        eligibility.COMP_SALES_PERIOD_ELIGIBLE_IND,
        eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
        daypart_sum.DERIVED_NET_SALES_USD_AMOUNT,
        daypart_sum.DERIVED_NET_TRANS_QTY,
        IFF(
            eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            daypart_sum.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_CURR_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            lst.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_CURR_YR,
        IFF(
            eligibility.last_eleg,
            lst.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_PREV_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.prev_eleg,
            prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_PREV_YR,
        IFF(
            eligibility.prev_eleg,
            prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_PREV_PREV_YR,
        IFF(
            eligibility.prev_eleg
            and eligibility.prev_prev_eleg,
            prev_prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_PREV_PREV_YR,
        IFF(
            eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            daypart_sum.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_CURR_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            lst.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_CURR_YR,
        IFF(
            eligibility.last_eleg,
            lst.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_PREV_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.prev_eleg,
            prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_PREV_YR,
        IFF(
            eligibility.prev_eleg,
            prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_PREV_PREV_YR,
        IFF(
            eligibility.prev_eleg
            and eligibility.prev_prev_eleg,
            prev_prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_PREV_PREV_YR
    from
        eligibility
        LEFT JOIN daypart_sum ON eligibility.brand_id = daypart_sum.brand_id
        and eligibility.rest_id = daypart_sum.rest_id
        and eligibility.business_date = daypart_sum.business_date
        LEFT JOIN daypart_sum lst on eligibility.brand_id = lst.brand_id
        AND eligibility.rest_id = lst.rest_id
        AND eligibility.last_bt = lst.business_date
        LEFT JOIN daypart_sum prev on eligibility.brand_id = prev.brand_id
        AND eligibility.rest_id = prev.rest_id
        AND eligibility.prev_bt = prev.business_date
        LEFT JOIN daypart_sum prev_prev on eligibility.brand_id = prev_prev.brand_id
        AND eligibility.rest_id = prev_prev.rest_id
        AND eligibility.prev_prev_bt = prev_prev.business_date
)
select distinct
    elegibility_day_part.BRAND_ID,
    CASE
        WHEN lower(daily.OWNERSHIP_TYP) IN ('franchised', 'territory franchise agreement', 'direct-non-franchise tenant','direct customer' ) THEN 'FRAN'
        WHEN lower(daily.OWNERSHIP_TYP) IN ('company owned', 'company operated') THEN 'ICR'
        ELSE 'N/A'
    END AS OWNERSHIP_TYPE,
    daily.OWNER_ID,
    elegibility_day_part.REST_ID,
    elegibility_day_part.COMP_SALES_TYPE as COMP_TYPE,
    daily.yr_nbr as FISC_YR_NBR,
    daily.period_nbr as FISC_PERIOD_NBR,
    daily.wk_nbr as FISC_WK_NBR,
    elegibility_day_part.BUSINESS_DATE,
    elegibility_day_part.LAST_BT as BUSINESS_DATE_PREV_YR,
    DAYNAME(elegibility_day_part.BUSINESS_DATE) as DAY_OF_WK_NAME,
    elegibility_day_part.COMP_SALES_DAILY_ELIGIBLE_IND as IS_COMP_DAY_IND,
    elegibility_day_part.COMP_SALES_WEEKLY_ELIGIBLE_IND as IS_COMP_WK_IND,
    elegibility_day_part.COMP_SALES_PERIOD_ELIGIBLE_IND as IS_COMP_PERIOD_IND,
    elegibility_day_part.DERIVED_NET_SALES_USD_AMOUNT,
    elegibility_day_part.DERIVED_NET_TRANS_QTY,
    elegibility_day_part.COMP_SALES_THIS_YR_CURR_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_CURR_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_CURR_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_CURR_YR,
    elegibility_day_part.COMP_SALES_THIS_YR_PREV_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_PREV_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_PREV_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_PREV_YR,
    elegibility_day_part.COMP_SALES_THIS_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_PREV_PREV_YR
from
    elegibility_day_part
    left join daily_with_rest daily
        on elegibility_day_part.brand_id = daily.brand_id
        AND elegibility_day_part.rest_id = daily.rest_id
        AND elegibility_day_part.business_date = daily.business_date;

create or replace view COMP_SALES_CHANNEL_DAILY_LEVEL(
	BRAND_ID,
	OWNERSHIP_TYPE,
	OWNER_ID,
	REST_ID,
	ORDER_CHANNEL_ID,
	ORDER_CHANNEL,
	FULFILLMENT_CHANNEL,
	COMP_TYPE,
	FISC_YR_NBR,
	FISC_PERIOD_NBR,
	FISC_WK_NBR,
	BUSINESS_DATE,
	COMP_BUSINESS_DATE,
	DAY_OF_WK_NAME,
	IS_COMP_DAY_IND,
	IS_COMP_WK_IND,
	IS_COMP_PERIOD_IND,
	DERIVED_NET_SALES_USD_AMOUNT,
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
with eligibility as (
    select *
    from idm_%%envname%%.comp_sales.comp_sales_elegibility_comp_date
)
,max_rest_by_business_date as (
    select DISTINCT
            daily.brand_id,
            daily.rest_id,
            daily.business_date,
            (select max(rest.effective_end_dt)
             from IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
             where rest.store_id = daily.rest_id
             and rest.brand_id = daily.brand_id
             and rest.effective_begin_dt <= daily.business_date and daily.business_date < rest.effective_end_dt) effective_end_dt
    from eligibility daily
)
,daily_with_rest as (
    select DISTINCT
        rest.brand_id,
        daily.rest_id,
        daily.business_date,
        rest.OWNERSHIP_TYP,
        rest.owner_id,
        daily.yr_nbr,
        daily.period_nbr,
        daily.wk_nbr
    from IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_DAILY_BV daily
    left join IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
    on rest.store_id = daily.rest_id
    and rest.brand_id = daily.brand_id
    inner join max_rest_by_business_date max_edt
    on rest.store_id = max_edt.rest_id
    and rest.brand_id = max_edt.brand_id
    and daily.business_date = max_edt.business_date
    and rest.effective_end_dt = max_edt.effective_end_dt
),
daypart_by_channel as (
    select
        daypart.*,
        channel.order_channel_hierarchy_l3_nm as ORDER_CHANNEL,
        channel.FULFILLMENT_CHANNEL_HIERARCHY_L3_NM as FULFILLMENT_CHANNEL
    from
        (
            select
                brand_id,
                rest_id,
                business_date,
                order_channel_id,
                SUM(DERIVED_NET_SALES_USD_AMOUNT) as DERIVED_NET_SALES_USD_AMOUNT,
                sum(TRANS_QTY) as DERIVED_NET_TRANS_QTY
            from
                IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_DAYPART_SALES_BV
            group by
                1,
                2,
                3,
                4
        ) as daypart
        inner join IDM_%%envname%%.coredim_bv.channel_dim_bv channel on channel.channel_id = daypart.order_channel_id
),
elegibility_day_part as (
    select
        eligibility.brand_id,
        eligibility.rest_id,
        eligibility.COMP_SALES_TYPE,
        eligibility.BUSINESS_DATE,
        eligibility.LAST_BT,
        eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
        eligibility.COMP_SALES_WEEKLY_ELIGIBLE_IND,
        eligibility.COMP_SALES_PERIOD_ELIGIBLE_IND,
        daypart_by_channel.DERIVED_NET_SALES_USD_AMOUNT,
        daypart_by_channel.DERIVED_NET_TRANS_QTY,
        daypart_by_channel.ORDER_CHANNEL,
        daypart_by_channel.FULFILLMENT_CHANNEL,
        daypart_by_channel.order_channel_id,
        IFF(
            eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            daypart_by_channel.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_CURR_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            lst.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_CURR_YR,
        IFF(
            eligibility.last_eleg,
            lst.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_PREV_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.prev_eleg,
            prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_PREV_YR,
        IFF(
            eligibility.prev_eleg,
            prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_PREV_PREV_YR,
        IFF(
            eligibility.prev_eleg
            and eligibility.prev_prev_eleg,
            prev_prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_PREV_PREV_YR,
        IFF(
            eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            daypart_by_channel.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_CURR_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            lst.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_CURR_YR,
        IFF(
            eligibility.last_eleg,
            lst.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_PREV_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.prev_eleg,
            prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_PREV_YR,
        IFF(
            eligibility.prev_eleg,
            prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_PREV_PREV_YR,
        IFF(
            eligibility.prev_eleg
            and eligibility.prev_prev_eleg,
            prev_prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_PREV_PREV_YR
    from
        eligibility
        LEFT JOIN daypart_by_channel ON eligibility.brand_id = daypart_by_channel.brand_id
        and eligibility.rest_id = daypart_by_channel.rest_id
        and eligibility.business_date = daypart_by_channel.business_date
        LEFT JOIN daypart_by_channel lst on eligibility.brand_id = lst.brand_id
        AND eligibility.rest_id = lst.rest_id
        AND eligibility.last_bt = lst.business_date
        AND daypart_by_channel.order_channel_id = lst.order_channel_id
        LEFT JOIN daypart_by_channel prev on eligibility.brand_id = prev.brand_id
        AND eligibility.rest_id = prev.rest_id
        AND eligibility.prev_bt = prev.business_date
        AND daypart_by_channel.order_channel_id = prev.order_channel_id
        LEFT JOIN daypart_by_channel prev_prev on eligibility.brand_id = prev_prev.brand_id
        AND eligibility.rest_id = prev_prev.rest_id
        AND eligibility.prev_prev_bt = prev_prev.business_date
        AND daypart_by_channel.order_channel_id = prev_prev.order_channel_id
)
select
    elegibility_day_part.BRAND_ID,
    CASE
        WHEN lower(daily.OWNERSHIP_TYP) IN ('franchised', 'territory franchise agreement', 'direct-non-franchise tenant', 'direct customer') THEN 'FRAN'
        WHEN lower(daily.OWNERSHIP_TYP) IN ('company owned', 'company operated') THEN 'ICR'
        ELSE 'N/A'
    END AS OWNERSHIP_TYPE,
    daily.owner_id,
    elegibility_day_part.rest_id,
    elegibility_day_part.order_channel_id as ORDER_CHANNEL_ID,
    coalesce(elegibility_day_part.ORDER_CHANNEL, 'N/A') as ORDER_CHANNEL,
    coalesce(elegibility_day_part.FULFILLMENT_CHANNEL, 'N/A') as FULFILLMENT_CHANNEL,
    elegibility_day_part.COMP_SALES_TYPE,
    daily.yr_nbr as FISC_YR_NBR,
    daily.period_nbr as FISC_PERIOD_NBR,
    daily.wk_nbr as FISC_WK_NBR,
    elegibility_day_part.BUSINESS_DATE,
    elegibility_day_part.last_bt as comp_business_date,
    DAYNAME(elegibility_day_part.BUSINESS_DATE) as DAY_OF_WK_NAME,
    elegibility_day_part.COMP_SALES_DAILY_ELIGIBLE_IND as IS_COMP_DAY_IND,
    elegibility_day_part.COMP_SALES_WEEKLY_ELIGIBLE_IND as IS_COMP_WK_IND,
    elegibility_day_part.COMP_SALES_PERIOD_ELIGIBLE_IND as IS_COMP_PERIOD_IND,
    elegibility_day_part.DERIVED_NET_SALES_USD_AMOUNT,
    elegibility_day_part.DERIVED_NET_TRANS_QTY,
    elegibility_day_part.COMP_SALES_THIS_YR_CURR_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_CURR_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_CURR_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_CURR_YR,
    elegibility_day_part.COMP_SALES_THIS_YR_PREV_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_PREV_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_PREV_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_PREV_YR,
    elegibility_day_part.COMP_SALES_THIS_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_PREV_PREV_YR
from
    elegibility_day_part 
    left join daily_with_rest daily
        on elegibility_day_part.brand_id = daily.brand_id
        AND elegibility_day_part.rest_id = daily.rest_id
        AND elegibility_day_part.business_date = daily.business_date;
        
        
create or replace view COMP_SALES_DAYPART_DAILY_LEVEL(
	BRAND_ID,
	OWNERSHIP_TYPE,
	OWNER_ID,
	REST_ID,
	DAYPART_NAME,
	COMP_TYPE,
	FISC_YR_NBR,
	FISC_PERIOD_NBR,
	FISC_WK_NBR,
	BUSINESS_DATE,
	COMP_BUSINESS_DATE,
	DAY_OF_WK_NAME,
	IS_COMP_DAY_IND,
	IS_COMP_WK_IND,
	IS_COMP_PERIOD_IND,
	DERIVED_NET_SALES_USD_AMOUNT,
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
with eligibility as (
    select
        *
    from
        IDM_%%envname%%.comp_sales.comp_sales_elegibility_comp_date
)
,max_rest_by_business_date as (
    select DISTINCT
            daily.brand_id,
            daily.rest_id,
            daily.business_date,
            (select max(rest.effective_end_dt)
             from IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
             where rest.store_id = daily.rest_id
             and rest.brand_id = daily.brand_id
             and rest.effective_begin_dt <= daily.business_date and daily.business_date < rest.effective_end_dt) effective_end_dt
    from eligibility daily
)
,daily_with_rest as (
    select DISTINCT
        rest.brand_id,
        daily.rest_id,
        daily.business_date,
        rest.OWNERSHIP_TYP,
        rest.owner_id,
        daily.yr_nbr,
        daily.period_nbr,
        daily.wk_nbr
    from IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_DAILY_BV daily
    left join IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
    on rest.store_id = daily.rest_id
    and rest.brand_id = daily.brand_id
    inner join max_rest_by_business_date max_edt
    on rest.store_id = max_edt.rest_id
    and rest.brand_id = max_edt.brand_id
    and daily.business_date = max_edt.business_date
    and rest.effective_end_dt = max_edt.effective_end_dt
),
daypart_by_daypart_name as (
    select
        brand_id,
        rest_id,
        business_date,
        daypart_name,
        SUM(DERIVED_NET_SALES_USD_AMOUNT) as DERIVED_NET_SALES_USD_AMOUNT,
        SUM(TRANS_QTY) as DERIVED_NET_TRANS_QTY
    from
        IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_DAYPART_SALES_BV
    group by
        1,
        2,
        3,
        4
),
elegibility_day_part as (
    select
        eligibility.brand_id,
        eligibility.rest_id,
        eligibility.COMP_SALES_TYPE,
        eligibility.BUSINESS_DATE,
        eligibility.LAST_BT,
        eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
        eligibility.COMP_SALES_WEEKLY_ELIGIBLE_IND,
        eligibility.COMP_SALES_PERIOD_ELIGIBLE_IND,
        daypart_by_daypart_name.DERIVED_NET_SALES_USD_AMOUNT,
        daypart_by_daypart_name.DERIVED_NET_TRANS_QTY,
        daypart_by_daypart_name.daypart_name,
        IFF(
            eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            daypart_by_daypart_name.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_CURR_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            lst.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_CURR_YR,
        IFF(
            eligibility.last_eleg,
            lst.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_PREV_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.prev_eleg,
            prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_PREV_YR,
        IFF(
            eligibility.prev_eleg,
            prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_THIS_YR_PREV_PREV_YR,
        IFF(
            eligibility.prev_eleg
            and eligibility.prev_prev_eleg,
            prev_prev.DERIVED_NET_SALES_USD_AMOUNT,
            0
        ) as COMP_SALES_LAST_YR_PREV_PREV_YR,
        IFF(
            eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            daypart_by_daypart_name.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_CURR_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.COMP_SALES_DAILY_ELIGIBLE_IND,
            lst.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_CURR_YR,
        IFF(
            eligibility.last_eleg,
            lst.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_PREV_YR,
        IFF(
            eligibility.last_eleg
            and eligibility.prev_eleg,
            prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_PREV_YR,
        IFF(
            eligibility.prev_eleg,
            prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_THIS_YR_PREV_PREV_YR,
        IFF(
            eligibility.prev_eleg
            and eligibility.prev_prev_eleg,
            prev_prev.DERIVED_NET_TRANS_QTY,
            0
        ) as COMP_TRANS_LAST_YR_PREV_PREV_YR
    from
        eligibility
        LEFT JOIN daypart_by_daypart_name ON eligibility.brand_id = daypart_by_daypart_name.brand_id
        and eligibility.rest_id = daypart_by_daypart_name.rest_id
        and eligibility.business_date = daypart_by_daypart_name.business_date
        
        LEFT JOIN daypart_by_daypart_name lst on eligibility.brand_id = lst.brand_id
        AND eligibility.rest_id = lst.rest_id
        AND eligibility.last_bt = lst.business_date
        AND daypart_by_daypart_name.daypart_name = lst.daypart_name
        
        LEFT JOIN daypart_by_daypart_name prev on eligibility.brand_id = prev.brand_id
        AND eligibility.rest_id = prev.rest_id
        AND eligibility.prev_bt = prev.business_date
        AND daypart_by_daypart_name.daypart_name = prev.daypart_name
        
        LEFT JOIN daypart_by_daypart_name prev_prev on eligibility.brand_id = prev_prev.brand_id
        AND eligibility.rest_id = prev_prev.rest_id
        AND eligibility.prev_prev_bt = prev_prev.business_date
        AND daypart_by_daypart_name.daypart_name = prev_prev.daypart_name
)
select
    elegibility_day_part.BRAND_ID,
    CASE
        WHEN lower(daily.OWNERSHIP_TYP) IN ('franchised', 'territory franchise agreement') THEN 'FRAN'
        WHEN lower(daily.OWNERSHIP_TYP) IN ('company owned', 'company operated') THEN 'ICR'
        ELSE 'N/A'
    END AS OWNERSHIP_TYPE,
    daily.owner_id,
    elegibility_day_part.rest_id,
    elegibility_day_part.daypart_name,
    elegibility_day_part.COMP_SALES_TYPE,
    daily.yr_nbr as FISC_YR_NBR,
    daily.period_nbr as FISC_PERIOD_NBR,
    daily.wk_nbr as FISC_WK_NBR,
    elegibility_day_part.BUSINESS_DATE,
    elegibility_day_part.LAST_BT as comp_business_date,
    DAYNAME(elegibility_day_part.BUSINESS_DATE) as DAY_OF_WK_NAME,
    elegibility_day_part.COMP_SALES_DAILY_ELIGIBLE_IND as IS_COMP_DAY_IND,
    elegibility_day_part.COMP_SALES_WEEKLY_ELIGIBLE_IND as IS_COMP_WK_IND,
    elegibility_day_part.COMP_SALES_PERIOD_ELIGIBLE_IND as IS_COMP_PERIOD_IND,
    elegibility_day_part.DERIVED_NET_SALES_USD_AMOUNT,
    elegibility_day_part.DERIVED_NET_TRANS_QTY,
    elegibility_day_part.COMP_SALES_THIS_YR_CURR_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_CURR_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_CURR_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_CURR_YR,
    elegibility_day_part.COMP_SALES_THIS_YR_PREV_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_PREV_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_PREV_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_PREV_YR,
    elegibility_day_part.COMP_SALES_THIS_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_SALES_LAST_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_TRANS_THIS_YR_PREV_PREV_YR,
    elegibility_day_part.COMP_TRANS_LAST_YR_PREV_PREV_YR
from
    elegibility_day_part
    left join daily_with_rest daily
        on elegibility_day_part.brand_id = daily.brand_id
        AND elegibility_day_part.rest_id = daily.rest_id
        AND elegibility_day_part.business_date = daily.business_date;