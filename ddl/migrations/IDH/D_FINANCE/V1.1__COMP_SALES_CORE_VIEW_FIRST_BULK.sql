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
),
daily_with_rest as (
    select DISTINCT 
        rest.brand_id,
        daily.rest_id,
        daily.business_date,
        rest.OWNERSHIP_TYP,
        rest.owner_id,
        daily.yr_nbr,
        daily.period_nbr,
        daily.wk_nbr,
        rank() over (partition by rest.brand_id, rest.store_id order by effective_end_dt desc) as newest_dup
    from IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_DAILY_BV daily
    left join IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
    on rest.store_id = daily.rest_id
    and rest.brand_id = daily.brand_id
    and daily.business_date between rest.effective_begin_dt and coalesce(rest.effective_end_dt, daily.business_date)
    where daily.comp_sales_type = 'FISC' --IS FOR AVOID DUPLICATED ROW for FISC/CAL
    QUALIFY newest_dup = 1
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
        WHEN lower(daily.OWNERSHIP_TYP) IN ('franchised', 'territory franchise agreement') THEN 'FRAN'
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
    daily_with_rest daily
    join elegibility_day_part
        on elegibility_day_part.brand_id = daily.brand_id
        AND elegibility_day_part.rest_id = daily.rest_id
        AND elegibility_day_part.business_date = daily.business_date;

create or replace view COMP_SALES_CHANNEL_PERIOD_LEVEL(
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
	PERIOD_START_DATE,
	PERIOD_START_DATE_PREV_YR,
	IS_COMP_PERIOD_IND,
	IS_COMPLETE_PERIOD_IND,
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
with eligibility_date_dim as (
       SELECT distinct
           e.BRAND_ID,
           e.REST_ID,
           dd2.FISCAL_YEAR_NBR YR_NBR,
           dd2.FISCAL_PERIOD_NBR PERIOD_NBR,
           e.COMP_SALES_TYPE,
           CASE
               when e.COMP_SALES_TYPE = 'CAL' then dd2.CALENDAR_COMPARABLE_PERIOD_START_DT
               when e.COMP_SALES_TYPE = 'FLEX' then dd1.FISCAL_PERIOD_START_DT
               when e.COMP_SALES_TYPE = 'FISC' then dd1.FISCAL_PERIOD_START_DT
               END AS PERIOD_START_DATE_PREV_YR,
           dd2.FISCAL_PERIOD_START_DT FISCAL_PERIOD_START_DATE
       FROM IDM_%%envname%%.COMP_SALES.COMP_SALES_ELEGIBILITY_COMP_DATE e
           INNER JOIN IDS_%%envname%%.INT_REF_BV.DATE_DIM_bv dd1 ON e.LAST_BT = dd1.CALENDAR_DT
           INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd2 ON e.BUSINESS_DATE = dd2.CALENDAR_DT
   ),
daypart_by_channel as (
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
),

daily_restaurant as (
    SELECT
        p.BRAND_ID,
        p.REST_ID,
        p.YR_NBR,
        p.PERIOD_NBR,
        ((p.PERIOD_NBR - dd_begin.FISCAL_PERIOD_NBR)+12*(p.YR_NBR - dd_begin.FISCAL_YEAR_NBR)) as begin_business,
        ((dd_end.FISCAL_PERIOD_NBR-dd_begin.FISCAL_PERIOD_NBR)+12*(dd_end.FISCAL_YEAR_NBR-dd_begin.FISCAL_YEAR_NBR)) as begin_end,
        r.OWNER_ID,
        CASE WHEN lower(r.OWNERSHIP_TYP)
            IN (
                'franchised',
                'territory franchise agreement',
                'direct-non-franchise tenant',
                'direct customer') THEN 'FRAN'
            WHEN lower(r.OWNERSHIP_TYP)
            IN ('company owned','company operated') THEN 'ICR'
            ELSE 'N/A' END AS OWNERSHIP_TYPE,
        rank() over (partition by r.BRAND_ID, r.STORE_ID, p.YR_NBR, p.PERIOD_NBR order by r.EFFECTIVE_BEGIN_DT desc) as newest_dup
    FROM
        IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV r
        INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd_begin ON r.EFFECTIVE_BEGIN_DT = dd_begin.CALENDAR_DT
        INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd_end ON r.EFFECTIVE_END_DT = dd_end.CALENDAR_DT
        INNER JOIN IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_PERIOD_BV p
        ON r.BRAND_ID = p.BRAND_ID
            AND r.STORE_ID = p.REST_ID
            AND  begin_business >= 0
            AND  begin_end >= 0
            AND begin_business <= begin_end
    WHERE p.COMP_SALES_TYPE = 'FISC'
    QUALIFY newest_dup = 1
),
comp_sales_rest_eligibility as (
    SELECT
        e_dd.BRAND_ID,
        d_r.OWNERSHIP_TYPE,
        d_r.OWNER_ID,
        e_dd.REST_ID,
        e_dd.COMP_SALES_TYPE COMP_TYPE,
        p.YR_NBR FISC_YR_NBR,
        p.PERIOD_NBR FISC_PERIOD_NBR,
        e_dd.FISCAL_PERIOD_START_DATE,
        e_dd.PERIOD_START_DATE_PREV_YR,
        p.COMP_SALES_PERIOD_ELIGIBLE_IND IS_COMP_PERIOD_IND,
        p.COMPLETE_PERIOD_IND IS_COMPLETE_PERIOD_IND
    FROM
        IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_PERIOD_BV p
        INNER JOIN eligibility_date_dim e_dd
            ON
                e_dd.BRAND_ID = p.BRAND_ID AND
                e_dd.REST_ID = p.REST_ID AND
                e_dd.YR_NBR = p.YR_NBR AND
                e_dd.PERIOD_NBR = p.PERIOD_NBR AND
                e_dd.COMP_SALES_TYPE = p.COMP_SALES_TYPE
        INNER JOIN daily_restaurant d_r
            ON
                d_r.BRAND_ID = p.BRAND_ID AND
                d_r.REST_ID = p.REST_ID AND
                d_r.YR_NBR = p.YR_NBR AND
                d_r.PERIOD_NBR = p.PERIOD_NBR
),
eligibility_daypart as (
    SELECT
        e.BRAND_ID,
        e.REST_ID,
        e.COMP_SALES_TYPE COMP_TYPE,
        dd.FISCAL_YEAR_NBR FISC_YR_NBR,
        dd.FISCAL_PERIOD_NBR FISC_PERIOD_NBR,
        d_curr_yr.order_channel_id,
        d_curr_yr.DERIVED_NET_SALES_USD_AMOUNT DERIVED_NET_SALES_AMT,
        d_curr_yr.DERIVED_NET_TRANS_QTY,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND ,
            d_curr_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_CURR_YR,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND ,
            d_curr_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_THIS_YR_CURR_YR,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND and
            e.last_period_eleg,
            d_last_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_CURR_YR,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND and
            e.last_period_eleg,
            d_last_yr.DERIVED_NET_TRANS_QTY , 0) COMP_TRANS_LAST_YR_CURR_YR,
        IFF(
            e.last_period_eleg ,
            d_last_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_PREV_YR,
        IFF(
            e.last_period_eleg ,
            d_last_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_THIS_YR_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.last_period_eleg,
            d_prev_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.last_period_eleg,
            d_prev_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_LAST_YR_PREV_YR,
        IFF(
            e.prev_period_eleg ,
            d_prev_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg ,
            d_prev_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_THIS_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.prev_prev_period_eleg,
            d_prev_prev_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.prev_prev_period_eleg,
            d_prev_prev_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_LAST_YR_PREV_PREV_YR
    FROM
        IDM_%%envname%%.COMP_SALES.COMP_SALES_ELEGIBILITY_COMP_DATE e
        INNER JOIN daypart_by_channel d_curr_yr
            ON
                e.BRAND_ID = d_curr_yr.BRAND_ID AND
                e.REST_ID = d_curr_yr.REST_ID AND
                e.BUSINESS_DATE = d_curr_yr.BUSINESS_DATE

        INNER JOIN daypart_by_channel d_last_yr
            ON
                e.BRAND_ID = d_last_yr.BRAND_ID AND
                e.REST_ID = d_last_yr.REST_ID AND
                e.last_bt = d_last_yr.BUSINESS_DATE

        INNER JOIN daypart_by_channel d_prev_yr
            ON
                e.BRAND_ID = d_prev_yr.BRAND_ID AND
                e.REST_ID = d_prev_yr.REST_ID AND
                e.prev_bt = d_prev_yr.BUSINESS_DATE

        INNER JOIN daypart_by_channel d_prev_prev_yr
            ON
                e.BRAND_ID = d_prev_prev_yr.BRAND_ID AND
                e.REST_ID = d_prev_prev_yr.REST_ID AND
                e.prev_prev_bt = d_prev_prev_yr.BUSINESS_DATE

        INNER JOIN IDS_%%envname%%.INT_REF_BV.DATE_DIM_BV dd
            ON e.BUSINESS_DATE = dd.CALENDAR_DT
),
period_agg_daypart AS (
    SELECT
        BRAND_ID,
        REST_ID,
        COMP_TYPE,
        FISC_YR_NBR,
        FISC_PERIOD_NBR,
        ORDER_CHANNEL_ID,
        SUM(DERIVED_NET_SALES_AMT) DERIVED_NET_SALES_AMT,
        SUM(DERIVED_NET_TRANS_QTY) DERIVED_NET_TRANS_QTY,
        SUM(COMP_SALES_THIS_YR_CURR_YR) COMP_SALES_THIS_YR_CURR_YR,
        SUM(COMP_TRANS_THIS_YR_CURR_YR) COMP_TRANS_THIS_YR_CURR_YR,
        SUM(COMP_SALES_LAST_YR_CURR_YR) COMP_SALES_LAST_YR_CURR_YR,
        SUM(COMP_TRANS_LAST_YR_CURR_YR) COMP_TRANS_LAST_YR_CURR_YR,
        SUM(COMP_SALES_THIS_YR_PREV_YR) COMP_SALES_THIS_YR_PREV_YR,
        SUM(COMP_TRANS_THIS_YR_PREV_YR) COMP_TRANS_THIS_YR_PREV_YR,
        SUM(COMP_SALES_LAST_YR_PREV_YR) COMP_SALES_LAST_YR_PREV_YR,
        SUM(COMP_TRANS_LAST_YR_PREV_YR) COMP_TRANS_LAST_YR_PREV_YR,
        SUM(COMP_SALES_THIS_YR_PREV_PREV_YR) COMP_SALES_THIS_YR_PREV_PREV_YR,
        SUM(COMP_TRANS_THIS_YR_PREV_PREV_YR) COMP_TRANS_THIS_YR_PREV_PREV_YR,
        SUM(COMP_SALES_LAST_YR_PREV_PREV_YR) COMP_SALES_LAST_YR_PREV_PREV_YR,
        SUM(COMP_TRANS_LAST_YR_PREV_PREV_YR) COMP_TRANS_LAST_YR_PREV_PREV_YR
    FROM eligibility_daypart ed
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    csre.BRAND_ID,
    csre.OWNERSHIP_TYPE,
    csre.OWNER_ID,
    csre.REST_ID,
    ed.order_channel_id,
    channel.order_channel_hierarchy_l3_nm as ORDER_CHANNEL,
    channel.FULFILLMENT_CHANNEL_HIERARCHY_L3_NM as FULFILLMENT_CHANNEL,
    csre.COMP_TYPE,
    csre.FISC_YR_NBR,
    csre.FISC_PERIOD_NBR,
    csre.FISCAL_PERIOD_START_DATE,
    csre.PERIOD_START_DATE_PREV_YR,
    csre.IS_COMP_PERIOD_IND,
    csre.IS_COMPLETE_PERIOD_IND,
    ed.DERIVED_NET_SALES_AMT,
    ed.DERIVED_NET_TRANS_QTY,
    ed.COMP_SALES_THIS_YR_CURR_YR,
    ed.COMP_SALES_LAST_YR_CURR_YR,
    ed.COMP_TRANS_THIS_YR_CURR_YR,
    ed.COMP_TRANS_LAST_YR_CURR_YR,
    ed.COMP_SALES_THIS_YR_PREV_YR,
    ed.COMP_SALES_LAST_YR_PREV_YR,
    ed.COMP_TRANS_THIS_YR_PREV_YR,
    ed.COMP_TRANS_LAST_YR_PREV_YR,
    ed.COMP_SALES_THIS_YR_PREV_PREV_YR,
    ed.COMP_SALES_LAST_YR_PREV_PREV_YR,
    ed.COMP_TRANS_THIS_YR_PREV_PREV_YR,
    ed.COMP_TRANS_LAST_YR_PREV_PREV_YR
FROM period_agg_daypart ed
    INNER JOIN comp_sales_rest_eligibility csre
        ON
            ed.BRAND_ID = csre.BRAND_ID AND
            ed.REST_ID = csre.REST_ID AND
            ed.COMP_TYPE = csre.COMP_TYPE AND
            ed.FISC_YR_NBR = csre.FISC_YR_NBR AND
            ed.FISC_PERIOD_NBR = csre.FISC_PERIOD_NBR
    INNER JOIN IDM_%%envname%%.coredim_bv.channel_dim_bv channel ON channel.channel_id = ed.order_channel_id;

create or replace view COMP_SALES_CHANNEL_WEEK_LEVEL(
	BRAND_ID,
	OWNERSHIP_TYPE,
	OWNER_ID,
	REST_ID,
	CHANNEL_ID,
	ORDER_CHANNEL,
	FULFILLMENT_CHANNEL,
	COMP_TYPE,
	FISC_YR_NBR,
	FISC_WK_NBR,
	WK_START_DATE,
	WK_START_DATE_PREV_YR,
	IS_COMP_WK_IND,
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
with eligibility_date_dim as (
       SELECT 
           e.BRAND_ID,
           e.REST_ID,
           dd2.FISCAL_YEAR_NBR YR_NBR,
           dd2.FISCAL_WEEK_NBR WK_NBR,
           e.COMP_SALES_TYPE,
           CASE
               when e.COMP_SALES_TYPE = 'CAL' then dd2.CALENDAR_COMPARABLE_WEEK_START_DT
               when e.COMP_SALES_TYPE = 'FLEX' then dd2.CALENDAR_COMPARABLE_WEEK_START_DT
               when e.COMP_SALES_TYPE = 'FISC' then dd1.FISCAL_WEEK_START_DT
               END AS WK_START_DATE_PREV_YR,
           dd2.FISCAL_WEEK_START_DT FISCAL_WEEK_START_DATE
       FROM IDM_%%envname%%.COMP_SALES.COMP_SALES_ELEGIBILITY_COMP_DATE e
           INNER JOIN IDS_%%envname%%.INT_REF_BV.DATE_DIM_bv dd1 ON e.LAST_BT = dd1.CALENDAR_DT
           INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd2 ON e.BUSINESS_DATE = dd2.CALENDAR_DT
   ),
daypart_by_channel as (
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
),
weekly_restaurant as (
    SELECT
        w.BRAND_ID,
        w.REST_ID,
        w.YR_NBR,
        w.WK_NBR,
        ((w.WK_NBR - dd_begin.FISCAL_WEEK_NBR)+12*(w.YR_NBR - dd_begin.FISCAL_YEAR_NBR)) as begin_business,
        ((dd_end.FISCAL_WEEK_NBR-dd_begin.FISCAL_WEEK_NBR)+12*(dd_end.FISCAL_YEAR_NBR-dd_begin.FISCAL_YEAR_NBR)) as begin_end,
        r.OWNER_ID,
        CASE WHEN lower(r.OWNERSHIP_TYP)
            IN (
                'franchised',
                'territory franchise agreement',
                'direct-non-franchise tenant',
                'direct customer') THEN 'FRAN'
            WHEN lower(r.OWNERSHIP_TYP)
            IN ('company owned','company operated') THEN 'ICR'
            ELSE 'N/A' END AS OWNERSHIP_TYPE,
        rank() over (partition by r.BRAND_ID, r.STORE_ID, w.YR_NBR, w.WK_NBR order by r.EFFECTIVE_BEGIN_DT desc) as newest_dup
    FROM
        IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV r
        INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd_begin ON r.EFFECTIVE_BEGIN_DT = dd_begin.CALENDAR_DT
        INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd_end ON r.EFFECTIVE_END_DT = dd_end.CALENDAR_DT
        INNER JOIN IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_WEEKLY_BV w
        ON r.BRAND_ID = w.BRAND_ID
            AND r.STORE_ID = w.REST_ID
            AND  begin_business >= 0
            AND  begin_end >= 0
            AND begin_business <= begin_end
    WHERE w.COMP_SALES_TYPE = 'FISC'
    QUALIFY newest_dup = 1
),
comp_sales_rest_eligibility as (
    SELECT
        e_dd.BRAND_ID,
        d_r.OWNERSHIP_TYPE,
        d_r.OWNER_ID,
        e_dd.REST_ID,
        e_dd.COMP_SALES_TYPE COMP_TYPE,
        w.YR_NBR FISC_YR_NBR,
        w.WK_NBR FISC_WK_NBR,
        e_dd.FISCAL_WEEK_START_DATE,
        e_dd.WK_START_DATE_PREV_YR,
        w.COMP_SALES_WEEKLY_ELIGIBLE_IND IS_COMP_WK_IND,
        
        w.COMPLETE_WK_IND IS_COMPLETE_WK_IND
    FROM
        IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_WEEKLY_BV w
        INNER JOIN eligibility_date_dim e_dd
            ON
                e_dd.BRAND_ID = w.BRAND_ID AND
                e_dd.REST_ID = w.REST_ID AND
                e_dd.YR_NBR = w.YR_NBR AND
                e_dd.WK_NBR = w.WK_NBR AND
                e_dd.COMP_SALES_TYPE = w.COMP_SALES_TYPE
        INNER JOIN weekly_restaurant d_r
            ON
                d_r.BRAND_ID = w.BRAND_ID AND
                d_r.REST_ID = w.REST_ID AND
                d_r.YR_NBR = w.YR_NBR AND
                d_r.WK_NBR = w.WK_NBR
),
eligibility_daypart as (
    SELECT
        e.BRAND_ID,
        e.REST_ID,
        e.COMP_SALES_TYPE COMP_TYPE,
        dd.FISCAL_YEAR_NBR FISC_YR_NBR,
        dd.FISCAL_WEEK_NBR FISC_WK_NBR,
        d_curr_yr.order_channel_id,
        d_curr_yr.DERIVED_NET_SALES_USD_AMOUNT DERIVED_NET_SALES_AMT,
        d_curr_yr.DERIVED_NET_TRANS_QTY,
        IFF(
            e.COMP_SALES_WEEKLY_ELIGIBLE_IND ,
            d_curr_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_CURR_YR,
        IFF(
            e.COMP_SALES_WEEKLY_ELIGIBLE_IND ,
            d_curr_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_THIS_YR_CURR_YR,
        IFF(
            e.COMP_SALES_WEEKLY_ELIGIBLE_IND and
            e.last_period_eleg,
            d_last_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_CURR_YR,
        IFF(
            e.COMP_SALES_WEEKLY_ELIGIBLE_IND and
            e.last_period_eleg,
            d_last_yr.DERIVED_NET_TRANS_QTY , 0) COMP_TRANS_LAST_YR_CURR_YR,
        IFF(
            e.last_period_eleg ,
            d_last_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_PREV_YR,
        IFF(
            e.last_period_eleg ,
            d_last_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_THIS_YR_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.last_period_eleg,
            d_prev_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.last_period_eleg,
            d_prev_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_LAST_YR_PREV_YR,
        IFF(
            e.prev_period_eleg ,
            d_prev_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg ,
            d_prev_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_THIS_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.prev_prev_period_eleg,
            d_prev_prev_yr.DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg and
            e.prev_prev_period_eleg,
            d_prev_prev_yr.DERIVED_NET_TRANS_QTY, 0) COMP_TRANS_LAST_YR_PREV_PREV_YR
    FROM
        IDM_%%envname%%.COMP_SALES.COMP_SALES_ELEGIBILITY_COMP_DATE e
        INNER JOIN daypart_by_channel d_curr_yr
            ON
                e.BRAND_ID = d_curr_yr.BRAND_ID AND
                e.REST_ID = d_curr_yr.REST_ID AND
                e.BUSINESS_DATE = d_curr_yr.BUSINESS_DATE

        INNER JOIN daypart_by_channel d_last_yr
            ON
                e.BRAND_ID = d_last_yr.BRAND_ID AND
                e.REST_ID = d_last_yr.REST_ID AND
                e.last_bt = d_last_yr.BUSINESS_DATE

        INNER JOIN daypart_by_channel d_prev_yr
            ON
                e.BRAND_ID = d_prev_yr.BRAND_ID AND
                e.REST_ID = d_prev_yr.REST_ID AND
                e.prev_bt = d_prev_yr.BUSINESS_DATE

        INNER JOIN daypart_by_channel d_prev_prev_yr
            ON
                e.BRAND_ID = d_prev_prev_yr.BRAND_ID AND
                e.REST_ID = d_prev_prev_yr.REST_ID AND
                e.prev_prev_bt = d_prev_prev_yr.BUSINESS_DATE

        INNER JOIN IDS_%%envname%%.INT_REF_BV.DATE_DIM_BV dd
            ON e.BUSINESS_DATE = dd.CALENDAR_DT
),
period_agg_daypart AS (
    SELECT
        BRAND_ID,
        REST_ID,
        COMP_TYPE,
        FISC_YR_NBR,
        FISC_WK_NBR,
        ORDER_CHANNEL_ID,
        SUM(DERIVED_NET_SALES_AMT) DERIVED_NET_SALES_AMT,
        SUM(DERIVED_NET_TRANS_QTY) DERIVED_NET_TRANS_QTY,
        SUM(COMP_SALES_THIS_YR_CURR_YR) COMP_SALES_THIS_YR_CURR_YR,
        SUM(COMP_TRANS_THIS_YR_CURR_YR) COMP_TRANS_THIS_YR_CURR_YR,
        SUM(COMP_SALES_LAST_YR_CURR_YR) COMP_SALES_LAST_YR_CURR_YR,
        SUM(COMP_TRANS_LAST_YR_CURR_YR) COMP_TRANS_LAST_YR_CURR_YR,
        SUM(COMP_SALES_THIS_YR_PREV_YR) COMP_SALES_THIS_YR_PREV_YR,
        SUM(COMP_TRANS_THIS_YR_PREV_YR) COMP_TRANS_THIS_YR_PREV_YR,
        SUM(COMP_SALES_LAST_YR_PREV_YR) COMP_SALES_LAST_YR_PREV_YR,
        SUM(COMP_TRANS_LAST_YR_PREV_YR) COMP_TRANS_LAST_YR_PREV_YR,
        SUM(COMP_SALES_THIS_YR_PREV_PREV_YR) COMP_SALES_THIS_YR_PREV_PREV_YR,
        SUM(COMP_TRANS_THIS_YR_PREV_PREV_YR) COMP_TRANS_THIS_YR_PREV_PREV_YR,
        SUM(COMP_SALES_LAST_YR_PREV_PREV_YR) COMP_SALES_LAST_YR_PREV_PREV_YR,
        SUM(COMP_TRANS_LAST_YR_PREV_PREV_YR) COMP_TRANS_LAST_YR_PREV_PREV_YR
    FROM eligibility_daypart ed
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    csre.BRAND_ID,
    csre.OWNERSHIP_TYPE,
    csre.OWNER_ID,
    csre.REST_ID,
    ed.order_channel_id,
    channel.order_channel_hierarchy_l3_nm as ORDER_CHANNEL,
    channel.FULFILLMENT_CHANNEL_HIERARCHY_L3_NM as FULFILLMENT_CHANNEL,
    csre.COMP_TYPE,
    csre.FISC_YR_NBR,
    csre.FISC_WK_NBR,
    csre.FISCAL_WEEK_START_DATE,
    csre.WK_START_DATE_PREV_YR,
    csre.IS_COMP_WK_IND,
    csre.IS_COMPLETE_WK_IND,
    ed.DERIVED_NET_SALES_AMT,
    ed.DERIVED_NET_TRANS_QTY,
    ed.COMP_SALES_THIS_YR_CURR_YR,
    ed.COMP_SALES_LAST_YR_CURR_YR,
    ed.COMP_TRANS_THIS_YR_CURR_YR,
    ed.COMP_TRANS_LAST_YR_CURR_YR,
    ed.COMP_SALES_THIS_YR_PREV_YR,
    ed.COMP_SALES_LAST_YR_PREV_YR,
    ed.COMP_TRANS_THIS_YR_PREV_YR,
    ed.COMP_TRANS_LAST_YR_PREV_YR,
    ed.COMP_SALES_THIS_YR_PREV_PREV_YR,
    ed.COMP_SALES_LAST_YR_PREV_PREV_YR,
    ed.COMP_TRANS_THIS_YR_PREV_PREV_YR,
    ed.COMP_TRANS_LAST_YR_PREV_PREV_YR
FROM period_agg_daypart ed
    INNER JOIN comp_sales_rest_eligibility csre
        ON
            ed.BRAND_ID = csre.BRAND_ID AND
            ed.REST_ID = csre.REST_ID AND
            ed.COMP_TYPE = csre.COMP_TYPE AND
            ed.FISC_YR_NBR = csre.FISC_YR_NBR AND
            ed.FISC_WK_NBR = csre.FISC_WK_NBR 
    INNER JOIN IDM_%%envname%%.coredim_bv.channel_dim_bv channel ON channel.channel_id = ed.order_channel_id
;

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
),
daily_with_rest as (
    select 
        rest.brand_id,
        daily.rest_id,
        daily.business_date,
        rest.OWNERSHIP_TYP,
        rest.owner_id,
        daily.yr_nbr,
        daily.period_nbr,
        daily.wk_nbr,
        rank() over (partition by rest.brand_id, rest.store_id order by effective_end_dt desc) as newest_dup
    from IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_DAILY_BV daily
    inner join IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
    on rest.store_id = daily.rest_id
    and rest.brand_id = daily.brand_id
    and daily.business_date between rest.effective_begin_dt and coalesce(rest.effective_end_dt, daily.business_date)
    where daily.COMP_SALES_TYPE = 'FISC'
    QUALIFY newest_dup = 1
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
    daily_with_rest daily
    join elegibility_day_part
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
),
daily_with_rest as (
    select
        rest.brand_id,
        daily.rest_id,
        daily.business_date,
        rest.OWNERSHIP_TYP,
        rest.owner_id,
        daily.yr_nbr,
        daily.period_nbr,
        daily.wk_nbr,
        rank() over (partition by rest.brand_id, rest.store_id order by effective_end_dt desc) as newest_dup
    from IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_DAILY_BV daily
    inner join IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
    on rest.store_id = daily.rest_id
    and rest.brand_id = daily.brand_id
    and daily.business_date between rest.effective_begin_dt and coalesce(rest.effective_end_dt, daily.business_date)
    WHERE daily.comp_sales_type = 'FISC'
    QUALIFY newest_dup = 1
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
    daily_with_rest daily
    join elegibility_day_part
        on elegibility_day_part.brand_id = daily.brand_id
        AND elegibility_day_part.rest_id = daily.rest_id
        AND elegibility_day_part.business_date = daily.business_date;

create or replace view COMP_SALES_DAYPART_WEEK_LEVEL(
	BRAND_ID,
	OWNERSHIP_TYPE,
	OWNER_ID,
	REST_ID,
	DAYPART_NAME,
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
)
,elegibility_week_with_date as (
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
)
,max_owner_by_store_in_week as (
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
daypart_daily as (
    select
        dp.brand_id,
        dp.rest_id,
        date_dim.fiscal_year_nbr yr_nbr,
        date_dim.fiscal_week_nbr wk_nbr,
        daypart_name,
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
)
,preagg as (
    select
        elegibility_week_with_date.*,
        max_owner_by_store_in_week.owner_id,
        max_owner_by_store_in_week.ownership_typ,
        daypart_daily.daypart_name,
        daypart_daily.DERIVED_NET_SALES_AMT,
        daypart_daily.DERIVED_NET_TRANS_QTY,
        last.DERIVED_NET_SALES_AMT LAST_DERIVED_NET_SALES_AMT,
        last.DERIVED_NET_TRANS_QTY LAST_DERIVED_NET_TRANS_QTY,
        prev.DERIVED_NET_SALES_AMT PREV_DERIVED_NET_SALES_AMT,
        prev.DERIVED_NET_TRANS_QTY PREV_DERIVED_NET_TRANS_QTY,
        prev_prev.DERIVED_NET_SALES_AMT PREV_PREV_DERIVED_NET_SALES_AMT,
        prev_prev.DERIVED_NET_TRANS_QTY PREV_PREV_DERIVED_NET_TRANS_QTY
    from
        elegibility_week_with_date
        INNER JOIN max_owner_by_store_in_week USING(brand_id, rest_id, WK_START_DATE, WK_END_DATE)
        LEFT JOIN daypart_daily ON daypart_daily.brand_id = elegibility_week_with_date.brand_id
        AND daypart_daily.rest_id = elegibility_week_with_date.rest_id
        AND daypart_daily.yr_nbr = elegibility_week_with_date.yr_nbr
        AND daypart_daily.wk_nbr = elegibility_week_with_date.wk_nbr
        LEFT JOIN daypart_daily last ON last.brand_id = elegibility_week_with_date.brand_id
        AND last.rest_id = elegibility_week_with_date.rest_id
        AND last.yr_nbr = elegibility_week_with_date.last_yr
        AND last.wk_nbr = elegibility_week_with_date.wk_nbr
        AND last.daypart_name = daypart_daily.daypart_name
        LEFT JOIN daypart_daily prev ON prev.brand_id = elegibility_week_with_date.brand_id
        AND prev.rest_id = elegibility_week_with_date.rest_id
        AND prev.yr_nbr = elegibility_week_with_date.prev_yr
        AND prev.wk_nbr = elegibility_week_with_date.wk_nbr
        AND daypart_daily.daypart_name = prev.daypart_name
        LEFT JOIN daypart_daily prev_prev ON prev_prev.brand_id = elegibility_week_with_date.brand_id
        AND prev_prev.rest_id = elegibility_week_with_date.rest_id
        AND prev_prev.yr_nbr = elegibility_week_with_date.prev_prev_yr
        AND prev_prev.wk_nbr = elegibility_week_with_date.wk_nbr
        AND daypart_daily.daypart_name = prev_prev.daypart_name
)
select
    BRAND_ID,
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
    OWNER_ID,
    REST_ID,
    DAYPART_NAME,
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

create or replace view COMP_SALES_FLAG_REF(
	BRAND_ID,
	OWNERSHIP_TYPE,
	REST_ID,
	YR_NBR,
	PERIOD_NBR,
	WK_NBR,
	BUSINESS_DATE,
	BUSINESS_DATE_PREV_YR,
	COMP_TYPE,
	IS_COMP_DAY_IND,
	IS_COMP_WK_IND,
	IS_COMP_PERIOD_IND
) as
with restaurant_ownership as(
      SELECT DISTINCT
        e.BRAND_ID,
        e.REST_ID,
        e.BUSINESS_DATE,
        e.YR_NBR,
        e.PERIOD_NBR,
        e.WK_NBR,
        CASE WHEN lower(o.OWNERSHIP_TYP) IN ('franchised','territory franchise agreement', 'direct-non-franchise tenant', 'direct customer') THEN 'FRAN'
            WHEN lower(o.OWNERSHIP_TYP) IN ('company owned','company operated') THEN 'ICR'
            ELSE 'N/A' END AS OWNERSHIP_TYPE,
        rank() over (partition by o.BRAND_ID, o.STORE_ID order by o.effective_end_dt desc) as newest_dup
      FROM IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV o
      INNER JOIN IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_DAILY_BV e
        ON e.BRAND_ID = o.BRAND_ID AND e.REST_ID = o.STORE_ID
      AND e.BUSINESS_DATE between o.effective_begin_dt and coalesce(o.effective_end_dt, e.BUSINESS_DATE)
      QUALIFY newest_dup = 1
    )
    SELECT
        ecd.BRAND_ID,
        ro.OWNERSHIP_TYPE,
        ecd.REST_ID,
        ro.YR_NBR,
        ro.PERIOD_NBR,
        ro.WK_NBR,
        ro.BUSINESS_DATE,
        ecd.LAST_BT as BUSINESS_DATE_PREV_YR,
        ecd.COMP_SALES_TYPE as COMP_TYPE,
        ecd.COMP_SALES_DAILY_ELIGIBLE_IND IS_COMP_DAY_IND,
        ecd.COMP_SALES_WEEKLY_ELIGIBLE_IND IS_COMP_WK_IND,
        ecd.COMP_SALES_PERIOD_ELIGIBLE_IND IS_COMP_PERIOD_IND
    FROM
        IDM_%%envname%%.COMP_SALES.COMP_SALES_ELEGIBILITY_COMP_DATE ecd
        INNER JOIN restaurant_ownership ro
        ON
        ecd.BRAND_ID = ro.BRAND_ID AND
        ecd.REST_ID = ro.REST_ID AND
        ecd.BUSINESS_DATE = ro.BUSINESS_DATE;

create or replace view COMP_SALES_PERIOD_LEVEL(
	BRAND_ID,
	OWNERSHIP_TYPE,
	OWNER_ID,
	REST_ID,
	COMP_TYPE,
	FISC_YR_NBR,
	FISC_PERIOD_NBR,
	FISCAL_PERIOD_START_DATE,
	PERIOD_START_DATE_PREV_YR,
	IS_COMP_PERIOD_IND,
	IS_COMPLETE_PERIOD_IND,
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
with eligibility_date_dim as (
    SELECT distinct
        e.BRAND_ID, 
        e.REST_ID,
        dd2.FISCAL_YEAR_NBR YR_NBR,
        dd2.FISCAL_PERIOD_NBR PERIOD_NBR,
        e.COMP_SALES_TYPE,
        CASE 
            when e.COMP_SALES_TYPE = 'CAL' then dd2.CALENDAR_COMPARABLE_PERIOD_START_DT
            when e.COMP_SALES_TYPE = 'FLEX' then dd2.CALENDAR_COMPARABLE_PERIOD_START_DT
            when e.COMP_SALES_TYPE = 'FISC' then dd1.FISCAL_PERIOD_START_DT
            END AS PERIOD_START_DATE_PREV_YR,
        dd2.FISCAL_PERIOD_START_DT FISCAL_PERIOD_START_DATE
    FROM IDM_%%envname%%.COMP_SALES.COMP_SALES_ELEGIBILITY_COMP_DATE e 
        INNER JOIN IDS_%%envname%%.INT_REF_BV.DATE_DIM_bv dd1 ON e.LAST_BT = dd1.CALENDAR_DT
        INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd2 ON e.BUSINESS_DATE = dd2.CALENDAR_DT
),
daily_restaurant as (
    SELECT
        p.BRAND_ID, 
        p.REST_ID, 
        p.YR_NBR,
        p.PERIOD_NBR,
        ((p.PERIOD_NBR - dd_begin.FISCAL_PERIOD_NBR)+12*(p.YR_NBR - dd_begin.FISCAL_YEAR_NBR)) as begin_business,
        ((dd_end.FISCAL_PERIOD_NBR-dd_begin.FISCAL_PERIOD_NBR)+12*(dd_end.FISCAL_YEAR_NBR-dd_begin.FISCAL_YEAR_NBR)) as begin_end,
        r.OWNER_ID,
        CASE WHEN lower(r.OWNERSHIP_TYP) 
            IN (
                'franchised',
                'territory franchise agreement', 
                'direct-non-franchise tenant', 
                'direct customer') THEN 'FRAN' 
            WHEN lower(r.OWNERSHIP_TYP) 
            IN ('company owned','company operated') THEN 'ICR' 
            ELSE 'N/A' END AS OWNERSHIP_TYPE,
        rank() over (partition by r.BRAND_ID, r.STORE_ID, p.YR_NBR, p.PERIOD_NBR order by r.EFFECTIVE_BEGIN_DT desc) as newest_dup
    FROM
        IDM_%%envname%%.COREDIM_BV.RESTAURANT_SCD_DIM_BV r 
        INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd_begin ON r.EFFECTIVE_BEGIN_DT = dd_begin.CALENDAR_DT
        INNER JOIN IDS_%%envname%%.INT_REF_bv.DATE_DIM_BV dd_end ON r.EFFECTIVE_END_DT = dd_end.CALENDAR_DT
        INNER JOIN IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_PERIOD_BV p
        ON r.BRAND_ID = p.BRAND_ID
        AND r.STORE_ID = p.REST_ID
        AND  begin_business >= 0
        AND  begin_end >= 0
        AND begin_business <= begin_end
    WHERE p.COMP_SALES_TYPE = 'FISC'
    QUALIFY newest_dup = 1
),
comp_sales_rest_eligibility as (
    SELECT distinct
        e_dd.BRAND_ID,
        d_r.OWNERSHIP_TYPE,
        d_r.OWNER_ID,
        e_dd.REST_ID,
        e_dd.COMP_SALES_TYPE COMP_TYPE,
        p.YR_NBR FISC_YR_NBR,
        p.PERIOD_NBR FISC_PERIOD_NBR,
        e_dd.FISCAL_PERIOD_START_DATE,
        e_dd.PERIOD_START_DATE_PREV_YR,
        p.COMP_SALES_PERIOD_ELIGIBLE_IND IS_COMP_PERIOD_IND, 
        p.COMPLETE_PERIOD_IND IS_COMPLETE_PERIOD_IND
    FROM 
        IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_ELIGIBILITY_PERIOD_BV p
        INNER JOIN eligibility_date_dim e_dd
        ON 
        e_dd.BRAND_ID = p.BRAND_ID AND
        e_dd.REST_ID = p.REST_ID AND
        e_dd.YR_NBR = p.YR_NBR AND
        e_dd.PERIOD_NBR = p.PERIOD_NBR AND
        (e_dd.COMP_SALES_TYPE = p.COMP_SALES_TYPE OR e_dd.COMP_SALES_TYPE = 'FLEX')
        INNER JOIN daily_restaurant d_r 
        ON 
        d_r.BRAND_ID = p.BRAND_ID AND
        d_r.REST_ID = p.REST_ID AND
        d_r.YR_NBR = p.YR_NBR AND
        d_r.PERIOD_NBR = p.PERIOD_NBR
),
daypart_sum as (
    SELECT
        BRAND_ID, 
        REST_ID,
        BUSINESS_DATE,
        SUM(DERIVED_NET_SALES_USD_AMOUNT) SUM_DERIVED_NET_SALES_USD_AMOUNT,
        SUM(TRANS_QTY) SUM_TRANS_QTY
    FROM IDM_%%envname%%.COMP_SALES_BV.COMP_SALES_REST_DAYPART_SALES_BV 
    GROUP BY 1, 2, 3
),
eligibility_daypart as (
    SELECT
        e.BRAND_ID, 
        e.REST_ID,
        e.COMP_SALES_TYPE COMP_TYPE,
        dd.FISCAL_YEAR_NBR FISC_YR_NBR,
        dd.FISCAL_PERIOD_NBR FISC_PERIOD_NBR,
        e.BUSINESS_DATE,
        e.last_bt,
        e.prev_bt,
        e.prev_prev_bt,
        d_curr_yr.SUM_DERIVED_NET_SALES_USD_AMOUNT DERIVED_NET_SALES_AMT,
        d_curr_yr.SUM_TRANS_QTY DERIVED_NET_TRANS_QTY,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND , 
            d_curr_yr.SUM_DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_CURR_YR,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND , 
            d_curr_yr.SUM_TRANS_QTY, 0) COMP_TRANS_THIS_YR_CURR_YR,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND and 
            e.last_period_eleg, 
            d_last_yr.SUM_DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_CURR_YR,
        IFF(
            e.COMP_SALES_PERIOD_ELIGIBLE_IND and 
            e.last_period_eleg,
            d_last_yr.SUM_TRANS_QTY , 0) COMP_TRANS_LAST_YR_CURR_YR,
        IFF(
            e.last_period_eleg , 
            d_last_yr.SUM_DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_PREV_YR,
        IFF(
            e.last_period_eleg , 
            d_last_yr.SUM_TRANS_QTY, 0) COMP_TRANS_THIS_YR_PREV_YR,
        IFF(
            e.prev_period_eleg and 
            e.last_period_eleg, 
            d_prev_yr.SUM_DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_PREV_YR,
        IFF(
            e.prev_period_eleg and 
            e.last_period_eleg, 
            d_prev_yr.SUM_TRANS_QTY, 0) COMP_TRANS_LAST_YR_PREV_YR,
        IFF(
            e.prev_period_eleg , 
            d_prev_yr.SUM_DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_THIS_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg , 
            d_prev_yr.SUM_TRANS_QTY, 0) COMP_TRANS_THIS_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg and 
            e.prev_prev_period_eleg, 
            d_prev_prev_yr.SUM_DERIVED_NET_SALES_USD_AMOUNT, 0) COMP_SALES_LAST_YR_PREV_PREV_YR,
        IFF(
            e.prev_period_eleg and 
            e.prev_prev_period_eleg, 
            d_prev_prev_yr.SUM_TRANS_QTY, 0) COMP_TRANS_LAST_YR_PREV_PREV_YR
    FROM 
        IDM_%%envname%%.COMP_SALES.COMP_SALES_ELEGIBILITY_COMP_DATE e 
        INNER JOIN daypart_sum d_curr_yr
        ON
        e.BRAND_ID = d_curr_yr.BRAND_ID AND
        e.REST_ID = d_curr_yr.REST_ID AND
        e.BUSINESS_DATE = d_curr_yr.BUSINESS_DATE

        INNER JOIN daypart_sum d_last_yr
        ON
        e.BRAND_ID = d_last_yr.BRAND_ID AND
        e.REST_ID = d_last_yr.REST_ID AND
        e.last_bt = d_last_yr.BUSINESS_DATE

        INNER JOIN daypart_sum d_prev_yr
        ON
        e.BRAND_ID = d_prev_yr.BRAND_ID AND
        e.REST_ID = d_prev_yr.REST_ID AND
        e.prev_bt = d_prev_yr.BUSINESS_DATE

        INNER JOIN daypart_sum d_prev_prev_yr
        ON
        e.BRAND_ID = d_prev_prev_yr.BRAND_ID AND
        e.REST_ID = d_prev_prev_yr.REST_ID AND
        e.prev_prev_bt = d_prev_prev_yr.BUSINESS_DATE

        INNER JOIN IDS_%%envname%%.INT_REF_BV.DATE_DIM_BV dd
        ON e.BUSINESS_DATE = dd.CALENDAR_DT
), 
period_agg_daypart as (
    select
        BRAND_ID,
        REST_ID,
        COMP_TYPE,
        FISC_YR_NBR,
        FISC_PERIOD_NBR,
        SUM(DERIVED_NET_SALES_AMT) DERIVED_NET_SALES_AMT,
        SUM(DERIVED_NET_TRANS_QTY) DERIVED_NET_TRANS_QTY,
        SUM(COMP_SALES_THIS_YR_CURR_YR) COMP_SALES_THIS_YR_CURR_YR,
        SUM(COMP_TRANS_THIS_YR_CURR_YR) COMP_TRANS_THIS_YR_CURR_YR,
        SUM(COMP_SALES_LAST_YR_CURR_YR) COMP_SALES_LAST_YR_CURR_YR,
        SUM(COMP_TRANS_LAST_YR_CURR_YR) COMP_TRANS_LAST_YR_CURR_YR,
        SUM(COMP_SALES_THIS_YR_PREV_YR) COMP_SALES_THIS_YR_PREV_YR,
        SUM(COMP_TRANS_THIS_YR_PREV_YR) COMP_TRANS_THIS_YR_PREV_YR,
        SUM(COMP_SALES_LAST_YR_PREV_YR) COMP_SALES_LAST_YR_PREV_YR,
        SUM(COMP_TRANS_LAST_YR_PREV_YR) COMP_TRANS_LAST_YR_PREV_YR,
        SUM(COMP_SALES_THIS_YR_PREV_PREV_YR) COMP_SALES_THIS_YR_PREV_PREV_YR,
        SUM(COMP_TRANS_THIS_YR_PREV_PREV_YR) COMP_TRANS_THIS_YR_PREV_PREV_YR,
        SUM(COMP_SALES_LAST_YR_PREV_PREV_YR) COMP_SALES_LAST_YR_PREV_PREV_YR,
        SUM(COMP_TRANS_LAST_YR_PREV_PREV_YR) COMP_TRANS_LAST_YR_PREV_PREV_YR
    from eligibility_daypart ed 
    group by 1, 2, 3, 4, 5
)
select
    csre.BRAND_ID,
    csre.OWNERSHIP_TYPE,
    csre.OWNER_ID,
    csre.REST_ID,
    csre.COMP_TYPE,
    csre.FISC_YR_NBR,
    csre.FISC_PERIOD_NBR,
    csre.FISCAL_PERIOD_START_DATE,
    csre.PERIOD_START_DATE_PREV_YR,
    csre.IS_COMP_PERIOD_IND, 
    csre.IS_COMPLETE_PERIOD_IND,
    ed.DERIVED_NET_SALES_AMT,
    ed.DERIVED_NET_TRANS_QTY,
    ed.COMP_SALES_THIS_YR_CURR_YR,
    ed.COMP_SALES_LAST_YR_CURR_YR,
    ed.COMP_TRANS_THIS_YR_CURR_YR,
    ed.COMP_TRANS_LAST_YR_CURR_YR,
    ed.COMP_SALES_THIS_YR_PREV_YR,
    ed.COMP_SALES_LAST_YR_PREV_YR,
    ed.COMP_TRANS_THIS_YR_PREV_YR,
    ed.COMP_TRANS_LAST_YR_PREV_YR,
    ed.COMP_SALES_THIS_YR_PREV_PREV_YR,
    ed.COMP_SALES_LAST_YR_PREV_PREV_YR,
    ed.COMP_TRANS_THIS_YR_PREV_PREV_YR,
    ed.COMP_TRANS_LAST_YR_PREV_PREV_YR
from period_agg_daypart ed 
    INNER JOIN comp_sales_rest_eligibility csre
    ON 
    ed.BRAND_ID = csre.BRAND_ID AND
    ed.REST_ID = csre.REST_ID AND
    ed.COMP_TYPE = csre.COMP_TYPE AND
    ed.FISC_YR_NBR = csre.FISC_YR_NBR AND
    ed.FISC_PERIOD_NBR = csre.FISC_PERIOD_NBR
;
