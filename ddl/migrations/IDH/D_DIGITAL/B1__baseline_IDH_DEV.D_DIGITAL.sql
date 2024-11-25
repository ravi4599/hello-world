create view IF NOT EXISTS BAG_ADDITIONS(
	DATASET_ID,
	ADDITION_DTTM,
	FULL_VISITOR_ID,
	SESSION_ID,
	SESSION_NBR,
	EVENT_CATEGORY,
	EVENT_ACTION,
	HIT_NBR,
	PRODUCT_NAME,
	PRODUCT_PRICE_AMT,
	QTY,
	CART_ORDER
) as ( 
  SELECT 
  distinct main.dataset_Id as DATASET_ID, 
  TO_CHAR(to_timestamp(main.VisitStartTime),'MM/DD/YYYY HH24:MI:SS.FF3') as ADDITION_DTTM, 
  main.FullVisitorId::string as FULL_VISITOR_ID, 
  main.VisitId::string as Session_Id, 
  main.VisitNumber::string as Session_Nbr, 
  e.hits_eventInfo_eventCategory::string as Event_Category, 
  e.hits_eventInfo_eventAction::string as Event_Action, 
  p.hits_hitNumber::string as Hit_Nbr, 
  p.hits_product_v2ProductName::string as Product_Name, 
  p.hits_product_productPrice / POW(10, 6) as Product_Price_AMT, -- by default in source hits_product_productPrice is multiplied by 10^6 (e.g., 2.40 would be given as 2400000)
  ifnull(
    hits_product_productQuantity::int, 
    1
  ) as QTY, 
  DENSE_RANK() OVER (
    PARTITION BY Session_Nbr, 
    Session_Id, 
    ADDITION_DTTM, 
    main.FullVisitorId 
    ORDER BY 
      Hit_Nbr
  ) as Cart_Order 
FROM 
  RDS_DEV.BWW_BV.HITS_MAIN_BV main 
  JOIN RDS_DEV.BWW_BV.HITS_EVENTINFO_BV e on main.eventinfo_hash = e.eventinfo_hash 
  LEFT JOIN RDS_DEV.BWW_BV.HITS_PRODUCT_BV p ON main.visitnumber = p.visitnumber 
  and main.visitid = p.visitid 
  and main.fullvisitorid = p.fullvisitorid 
  and main.visitstarttime = p.visitstarttime 
  and main.hits_time = p.hits_time
  and main.hits_hitnumber = p.hits_hitnumber 
WHERE 
  (
    hits_eventinfo_eventAction = 'Add to Cart' 
    AND hits_eventInfo_eventCategory = 'Order'
  ) 
  OR (
    hits_eventinfo_eventAction = 'Add Item' 
    AND hits_eventInfo_eventCategory = 'Cart'
  ) 
  AND hits_eventInfo_eventAction IS NOT NULL
  union
  SELECT
        dataset_Id as DatasetId,
        to_char(to_timestamp(LEFT(event_timestamp, 13)), 'MM/DD/YYYY HH:MM:SS.FF3') as Timestamp, -- microseconds converted to milliseconds
        user_pseudo_id as FullVisitorId,
        IFF(LEN("'ga_session_id'") = 0, 'NULL',"'ga_session_id'")  as SessionId,
        IFF(LEN("'ga_session_number'") = 0, 'NULL', "'ga_session_number'") as SessionNumber,
        event_name as EventCategory,
        event_name as EventAction,
        NULL as HitNumber, -- not provided in GA_EVENTS, that's why all column has NULLs
        items_item_name as ProductName,
        items_price as ProductPrice,
        "'quantity'"::int as Quantity,
        DENSE_RANK() OVER (PARTITION BY SessionId, SessionNumber, FullVisitorId ORDER BY Timestamp) as CartOrder
    FROM RDS_DEV.BWW_BV.GA_EVENTS_BV
    PIVOT(listagg(event_params_value) for event_params_key in ('ga_session_id', 'ga_session_number', 'quantity')) as p
    WHERE event_name = 'add_to_cart' 

);
create view IF NOT EXISTS BAG_PURCHASE_FLOW_PROGRESS(
	FULL_VISITOR_ID,
	VISIT_ID,
	VISIT_NBR,
	ABANDONED_CART,
	PURCHASE_FLOW_STEP,
	PURCHASE_FLOW_NAME
) as (
    WITH MAX_STEP_BY_CUSTOMER AS (
    SELECT
        m.FULLVISITORID,
        m.VISITID::string as VISITID,
        m.VISITNUMBER,
        MAX(e.HITS_ECOMMERCEACTION_STEP::int) as Purchase_Flow_Step
    FROM RDS_DEV.BWW_BV.HITS_MAIN_BV m
    LEFT JOIN RDS_DEV.BWW_BV.HITS_ECOMMERCEACTION_BV e
    ON m.ecommerceaction_hash = e.ecommerceaction_hash
    GROUP BY 1,2,3
)
    SELECT
        SBC.FULLVISITORID AS FULL_VISITOR_ID,
        SBC.VISITID AS VISIT_ID,
        SBC.VISITNUMBER AS VISIT_NBR,
        CASE WHEN SBC.Purchase_Flow_Step between 1 and 5 THEN 'yes'
             WHEN SBC.Purchase_Flow_Step=6 THEN 'no'
             ELSE 'N/A' END AS ABANDONED_CART, -- step 6 is checkout complete and if customers max step is lower then 6 then he abandoned cart
        SBC.Purchase_Flow_Step AS Purchase_Flow_Step,
        CASE WHEN SBC.Purchase_Flow_Step=1 then 'VIEW_PRODUCT'
             WHEN SBC.Purchase_Flow_Step=2 then 'VIEW_PRODUCT_DETAIL'
             WHEN SBC.Purchase_Flow_Step=3 then 'ADD_TO_CART'
             WHEN SBC.Purchase_Flow_Step=4 then 'REMOVE_FROM_CART'
             WHEN SBC.Purchase_Flow_Step=5 then 'CHECKOUT_INITATED'
             WHEN SBC.Purchase_Flow_Step=6 then 'CHECKOUT_COMPLETED'
             WHEN SBC.Purchase_Flow_Step=7 then 'REFUND'
             WHEN SBC.Purchase_Flow_Step=8 then 'CHECKOUT_OPTIONS'
             ELSE 'UNKNOWN' END AS Purchase_Flow_Name
    FROM MAX_STEP_BY_CUSTOMER SBC
);
create view IF NOT EXISTS BAG_REMOVALS(
	DATASET_ID,
	REMOVAL_DTTM,
	FULL_VISITOR_ID,
	SESSION_ID,
	SESSION_NBR,
	HIT_NBR,
	EVENT_CATEGORY,
	EVENT_ACTION,
	PRODUCT_NAME,
	PRODUCT_PRICE_AMT,
	QTY
) as (
SELECT
    main.dataset_Id as DATASET_ID,
    TO_CHAR(to_timestamp(main.VisitStartTime), 'MM/DD/YYYY HH24:MI:SS') as REMOVAL_DTTM,
    main.FullVisitorId as FULL_VISITOR_ID,
    main.VisitId::string as Session_Id,
    main.VisitNumber::string as Session_Nbr,
    main.hits_hitnumber as Hit_Nbr,
    hits_eventInfo_eventCategory::string as Event_Category,
    hits_eventInfo_eventAction::string as Event_Action,
    hits_product_v2ProductName::string as Product_Name,
    hits_product_productPrice/POW(10,6) as Product_Price_amt, -- by default in source hits_product_productPrice is multiplied by 10^6 (e.g., 2.40 would be given as 2400000)
    IFNULL(hits_product_productQuantity::int, 1) as qty
FROM
    RDS_DEV.BWW_BV.HITS_MAIN_BV main
JOIN
    RDS_DEV.BWW_BV.HITS_EVENTINFO_BV ei
ON main.eventinfo_hash = ei.eventinfo_hash
LEFT JOIN
    RDS_DEV.BWW_BV.HITS_PRODUCT_BV p
ON main.visitnumber = p.visitnumber 
and main.visitid=p.visitid
and main.fullvisitorid = p.fullvisitorid
and main.visitstarttime = p.visitstarttime
and main.hits_time = p.hits_time
and main.hits_hitnumber = p.hits_hitnumber
WHERE
    hits_eventInfo_eventCategory = 'Cart'
    AND hits_eventInfo_eventAction = 'Remove'

UNION

SELECT
    distinct
    dataset_Id as DatasetId,
    TO_CHAR(to_timestamp(LEFT(event_timestamp, 13)), 'MM/DD/YYYY HH24:MI:SS') as Timestamp, -- microseconds converted to milliseconds
    user_pseudo_id as FullVisitorId,
    "'ga_session_id'" as SessionId,
    "'ga_session_number'" as SessionNumber,
    null as HitNumber,
    event_name as EventCategory,
    event_name as EventAction,
    items_item_name as ProductName,
    items_price as ProductPrice,
    "'quantity'"::int as Quantity
FROM
    RDS_DEV.BWW_BV.GA_EVENTS_BV
    PIVOT(listagg(event_params_value) for event_params_key in ('ga_session_id', 'ga_session_number', 'quantity')) as p
WHERE
    event_name = 'remove_from_cart'
);
create view IF NOT EXISTS CHECKOUT_SUMMARY(
	USER_ID,
	USER_PSEUDO_ID,
	GA_SESSION_ID,
	GA_SESSION_NBR,
	TRANS_ID,
	EVENT_DATE_UTC,
	EVENT_HHMMSS,
	EVENT_DATE,
	IS_DELIVERY_IND,
	PICKUP_TIME,
	SHIPPING,
	TAX_AMT,
	TIP,
	SUBTOTAL_AMT,
	VALUE
) as (
    with ecommerce_purchase_events as (
        SELECT
            distinct
            event_timestamp,
            TO_DATE(TO_TIMESTAMP(event_timestamp::string)) AS EVENT_DATE_UTC, --date and time is collected from event_timestamp column in UTC
            TO_TIME(TO_TIMESTAMP(event_timestamp::string)) AS EVENT_HHMMSS,
            TO_TIMESTAMP(event_timestamp::string) AS EVENT_DATETIME,
            user_id,
            user_pseudo_id,
            user_properties_key,
            user_properties_value,
            event_params_key,
            event_params_value
        FROM
           RDS_DEV.BWW_BV.GA_EVENTS_BV
        where
            event_name = 'ecommerce_purchase'
            and event_params_key IN (
                'tax',
                'tip',
                'shipping',
                'value',
                'isDelivery',
                'pickuptime',
                'transaction_id'
            )
    )
    select
        user_id,
        user_pseudo_id,
        ga_session_id,
        ga_session_number as ga_session_nbr,
        transaction_id as trans_id,
        EVENT_DATE_UTC,
        EVENT_HHMMSS ,
        EVENT_DATETIME as event_date,
        isDelivery as is_delivery_ind,
        pickuptime as pickup_time,
        shipping,
        tax as tax_amt,
        tip,(value - tax - shipping - tip) as subtotal_amt,
        value
    from
        ecommerce_purchase_events pivot(
            listagg(event_params_value) for event_params_key in (
                'isDelivery',
                'pickuptime',
                'shipping',
                'tax',
                'tip',
                'value',
                'transaction_id'
            )
        ) p1 (
            event_timestamp,
            user_id,
            user_pseudo_id,
            user_properties_key,
            user_properties_value,
            EVENT_DATE_UTC,
            EVENT_HHMMSS ,
            EVENT_DATETIME,
            isDelivery,
            pickuptime,
            shipping,
            tax,
            tip,
            value,
            transaction_id
        ) pivot(
            listagg(user_properties_value) for user_properties_key in ('ga_session_id', 'ga_session_number')
        ) p2 (
            event_timestamp,
            user_id,
            user_pseudo_id,
            EVENT_DATE_UTC,
            EVENT_HHMMSS ,
            EVENT_DATETIME,
            isDelivery,
            pickuptime,
            shipping,
            tax,
            tip,
            value,
            transaction_id,
            ga_session_id,
            ga_session_number
        )
 );
create view IF NOT EXISTS FINAL_PRODUCTS_PURCHASED(
	DATASET_ID,
	FULL_VISITOR_ID,
	SESSION_ID,
	SESSION_NBR,
	PRODUCT_PURCHASE_DTTM,
	PRODUCT_NAME,
	PRODUCT_QTY,
	IS_PROMO_IND
) as (
SELECT *
FROM(
        SELECT
        H.dataset_Id as dataset_Id,
        H.FullVisitorId as FULL_VISITOR_ID,
        H.VisitId::string as Session_Id,
        H.VisitNumber::string as Session_Nbr,
        TO_CHAR(DATEADD(
                    millisecond,
                    ZEROIFNULL(H.HITS_TIME),
                    TO_TIMESTAMP(H.visitStartTime)
                ), 'MM/DD/YYYY HH24:MI:SS') as PRODUCT_PURCHASE_DTTM,
        P.HITS_PRODUCT_V2PRODUCTNAME::string as Product_Name,
        P.HITS_PRODUCT_PRODUCTQUANTITY::string as Product_QTY,
        CASE WHEN UPPER(P.HITS_PRODUCT_V2PRODUCTNAME::string) LIKE ANY ('BOGO%', '%+%', '%LCS%', '%BUNDLE%', '%BUY%GET%') THEN 'Yes'
        ELSE 'No' END AS IS_PROMO_IND -- all products containting this characters are considered a promo
        FROM RDS_DEV.BWW_BV.HITS_MAIN_BV  as H
        JOIN RDS_DEV.BWW_BV.HITS_TRANSACTION_BV as T on H.TRANSACTION_HASH=T.TRANSACTION_HASH
        JOIN RDS_DEV.BWW_BV.HITS_PRODUCT_BV as P
            ON H.visitnumber = p.visitnumber 
            and H.visitid=p.visitid
            and H.fullvisitorid = p.fullvisitorid
            and H.visitstarttime = p.visitstarttime
            and H.hits_time = p.hits_time
            and H.hits_hitnumber = p.hits_hitnumber
        WHERE P.HITS_PRODUCT_V2PRODUCTNAME IS NOT NULL
        AND P.HITS_PRODUCT_PRODUCTQUANTITY IS NOT NULL
        AND T.HITS_TRANSACTION_TRANSACTIONID IS NOT NULL
    

    UNION

    SELECT DISTINCT
    dataset_Id,
    user_pseudo_id as FullVisitorId,
    "'ga_session_id'" as SessionId,
    "'ga_session_number'" as SessionNumber,
    TO_CHAR(to_timestamp(LEFT(event_timestamp, 13)), 'MM/DD/YYYY HH24:MI:SS') as Timestamp,
    items_item_name AS ProductName,
    items_quantity ProductQuantity,
    CASE WHEN UPPER(items_item_name) LIKE ANY ('BOGO%', '%+%', '%LCS%', '%BUNDLE%', '%BUY%GET%') THEN 'Yes'
    ELSE 'No' END AS IS_PROMO -- all products containting this characters are considered a promo
    FROM RDS_DEV.BWW_BV.GA_EVENTS_BV
    pivot(listagg(user_properties_value) for user_properties_key in ('ga_session_id','ga_session_number')) p
    WHERE 
        CASE
            WHEN TO_DATE(TO_TIMESTAMP(event_timestamp::string)) < TO_DATE('20210805', 'yyyyMMdd')
            THEN (event_name = 'ecommerce_purchase')
            ELSE (event_name = 'purchase')
        END -- in order to take only products which were purchased
    and items_item_name is not null and items_quantity is not null
 ));
create view IF NOT EXISTS SESSION_SUMMARY(
	FULL_VISITOR_ID,
	VISIT_NBR,
	VISIT_ID,
	DATE,
	USER_ID,
	PROFILE_ID,
	PROFILE_GUIDE,
	MEMBER_PHONE_NBR,
	FIRST_NAME,
	LAST_NAME,
	EMAIL_ADDRESS,
	CHANNEL_GROUPING,
	TRAFFIC_SOURCE_MEDIUM,
	TRAFFIC_SOURCE_SOURCE,
	TRAFFIC_SOURCE_CAMPAIGN,
	SESSION_START_TIME_UTC,
	SESSION_START_DATE_UTC,
	SESSION_START_HHMMSS_UTC,
	SESSION_END_TIME_UTC,
	SESSION_DURATION,
	LOCAL_TIMEZONE,
	SESSION_START_TIME_LOCAL,
	SESSION_END_TIME_LOCAL,
	LOAD_ID,
	LOAD_DATE,
	DATASET_ID,
	REST_NAME
) as (
SELECT
    FULL_VISITOR_ID,
    VISIT_NUMBER,
    VISIT_ID,
    DATE,
    USERID as USER_ID,
    PROFILE_ID,
    PROFILEGUID as PROFILE_GUIDE,
    MEMBER_PHONENUMBER as MEMBER_PHONE_NBR,
    FIRST_NAME,
    LAST_NAME,
    EMAIL_ADDRESS,
    CHANNELGROUPING as CHANNEL_GROUPING,
    TRAFFICSOURCE_MEDIUM as TRAFFIC_SOURCE_MEDIUM,
    TRAFFICSOURCE_SOURCE as TRAFFIC_SOURCE_SOURCE,
    TRAFFICSOURCE_CAMPAIGN as TRAFFIC_SOURCE_CAMPAIGN,
    START_SESSION_DATETIME_UTC AS SESSION_START_TIME_UTC,
    START_SESSION_DATE_UTC AS SESSION_START_DATE_UTC,
    START_SESSION_HHMMSS_UTC AS SESSION_START_HHMMSS_UTC,
    END_SESSION_DATETIME AS SESSION_END_TIME_UTC,
    SESSION_DURATION,
    store.TIME_ZONE AS LOCAL_TIMEZONE,
    ITPRCS_DEV.CSO.CONVERT_TIMESTAMP_FROM_UTC_TO_TIMEZONE(SESSION_START_TIME_UTC, LOCAL_TIMEZONE) AS SESSION_START_TIME_LOCAL,
    ITPRCS_DEV.CSO.CONVERT_TIMESTAMP_FROM_UTC_TO_TIMEZONE(SESSION_END_TIME_UTC, LOCAL_TIMEZONE) AS SESSION_END_TIME_LOCAL,
    sh.LOAD_ID,
    LOAD_DATE,
    DATASET_ID,
    NULLIF(sh.STORE_NUMBER, 0) AS rest_name
FROM (
    WITH SESSION_FACT_ENRICHED AS (
        SELECT
            S.BRAND_ID,
            S.FULL_VISITOR_ID,
            S.VISIT_NBR AS VISIT_NUMBER,
            S.VISIT_ID,
            S.BUSINESS_DT_KEY AS DATE,
            NULLIF(UD.USER_ID, '-1') AS USERID,
            LM.PROFILE_ID,
            LM.PROFILEGUID,
            LM.MEMBER_PHONENUMBER,
            LM.FIRST_NAME,
            LM.LAST_NAME,
            LM.EMAIL_ADDRESS,
            TSD.CHANNEL_GROUP_TYP AS CHANNELGROUPING,
            TSD.MEDIUM_NM AS TRAFFICSOURCE_MEDIUM,
            TSD.SOURCE_NM AS TRAFFICSOURCE_SOURCE,
            TSD.CAMPAIGN_NM AS TRAFFICSOURCE_CAMPAIGN,
            S.VISIT_START_TS AS START_SESSION_DATETIME_UTC,
            TO_DATE(S.VISIT_START_TS) AS START_SESSION_DATE_UTC,
            S.VISIT_START_TM AS START_SESSION_HHMMSS_UTC,
            DATEADD(
                second,
                ZEROIFNULL(S.SOURCE_TOTAL_TM_ONSITE_CNT),
                S.VISIT_START_TS
            ) AS END_SESSION_DATETIME,
            ZEROIFNULL(S.SOURCE_TOTAL_TM_ONSITE_CNT) AS SESSION_DURATION,
            S.STORE_KEY AS STORE_NUMBER,
            S.LOAD_ID,
            S.LOAD_DTTM AS LOAD_DATE,
            S.DATASET_ID
        FROM IDM_DEV.CBA_BV.SESSION_SUMMARY_FACT_BV AS S
        JOIN IDM_DEV.CBA_BV.USER_DIM_BV AS UD ON S.USER_KEY = UD.USER_KEY
        JOIN IDM_DEV.CBA_BV.TRAFFIC_SOURCE_DIM_BV AS TSD ON S.TRAFFIC_SOURCE_KEY = TSD.TRAFFIC_SOURCE_KEY
        LEFT JOIN RDS_DEV.BWW_BV.LOYALTY_MEMBER_BV AS LM ON UD.USER_ID = LM.ProfileGUID
        WHERE S.BRAND_ID = 'BWW'
    ),
    HITS_STORE_NUMBER AS(
        SELECT
            FULL_VISITOR_ID,
            VISIT_NUMBER,
            VISIT_ID,
            START_SESSION_DATETIME_UTC,
            Store_Number
        FROM(
            SELECT
                  FullVisitorID AS FULL_VISITOR_ID,
                  VisitNumber AS VISIT_NUMBER,
                  VisitID::string as VISIT_ID,
                  VISITSTARTTIME as START_SESSION_DATETIME_UTC,
                  CASE WHEN HITS_CUSTOMDIMENSIONS_INDEX::int=1 AND DATASET_ID in ('103688187', '241471416') AND LENGTH(TRIM(HITS_CUSTOMDIMENSIONS_VALUE)) > 0 AND HITS_CUSTOMDIMENSIONS_VALUE != 'iOS' AND HITS_CUSTOMDIMENSIONS_VALUE != 'test' THEN HITS_CUSTOMDIMENSIONS_VALUE::varchar ELSE NULL END AS Store_Number,
                  rank() over(PARTITION BY FULL_VISITOR_ID, VISIT_NUMBER, VISIT_ID, START_SESSION_DATETIME_UTC ORDER BY Store_Number) AS r
            FROM RDS_DEV.BWW_BV.HITS_CUSTOMDIMENSIONS_BV
            GROUP BY 1,2,3,4,5
        ) WHERE (r=1 and Store_Number is null) or Store_Number is not null
    )
    SELECT
        S.FULL_VISITOR_ID,
        S.VISIT_NUMBER,
        S.VISIT_ID,
        S.DATE,
        S.USERID,
        S.PROFILE_ID,
        S.PROFILEGUID,
        S.MEMBER_PHONENUMBER,
        S.FIRST_NAME,
        S.LAST_NAME,
        S.EMAIL_ADDRESS,
        S.CHANNELGROUPING,
        S.TRAFFICSOURCE_MEDIUM,
        S.TRAFFICSOURCE_SOURCE,
        S.TRAFFICSOURCE_CAMPAIGN,
        S.START_SESSION_DATETIME_UTC,
        S.START_SESSION_DATE_UTC,
        S.START_SESSION_HHMMSS_UTC,
        S.END_SESSION_DATETIME,
        S.SESSION_DURATION,
        S.LOAD_ID,
        S.LOAD_DATE,
        S.DATASET_ID,
        CASE WHEN S.DATASET_ID in ('126099547','241143169') THEN S.STORE_NUMBER::VARCHAR ELSE H.STORE_NUMBER END AS STORE_NUMBER
    FROM SESSION_FACT_ENRICHED AS S
    LEFT JOIN HITS_STORE_NUMBER AS H
        ON  H.VISIT_ID = S.VISIT_ID
        AND H.VISIT_NUMBER = S.VISIT_NUMBER
        AND H.FULL_VISITOR_ID = S.FULL_VISITOR_ID
        AND to_timestamp(H.START_SESSION_DATETIME_UTC) = S.START_SESSION_DATETIME_UTC
    ) sh
LEFT JOIN IDH_DEV.D_LOC.REST store on sh.STORE_NUMBER = store.rest_ID and store.BRAND_ID='bww');
create view IF NOT EXISTS TIME_ON_BAG(
	VISIT_NBR,
	VISIT_ID,
	FULL_VISITOR_ID,
	VISIT_START_TIME_TXT,
	VIEW_BAG_TIME_SECONDS
) as (
    SELECT
        visitNumber as VISIT_NBR,
        visitId as VISIT_ID,
        fullVisitorId as FULL_VISITOR_ID,
        visitStartTime as VISIT_START_TIME_TXT,
        viewBagTime_Seconds as VIEW_BAG_TIME_SECONDS
    FROM
        (
            WITH navigation_order_checkout_hits as (
            SELECT
                hm.visitNumber::string as visitNumber,
                hm.visitId::string as visitId,
                hm.fullVisitorId,
                hm.visitStartTime,
                hm.hits_hitNumber::int as hits_hitnumber,
                he.hits_eventInfo_eventCategory::string as ec,
                he.hits_eventInfo_eventAction::string as ea,
                hm.hits_time::int as hits_time
            FROM
                RDS_DEV.BWW_BV.HITS_MAIN_BV hm 
            JOIN RDS_DEV.BWW_BV.HITS_EVENTINFO_BV he 
                ON hm.eventinfo_hash = he.eventinfo_hash
            WHERE
                he.hits_eventInfo_eventCategory = 'Navigation'
                OR (
                    he.hits_eventInfo_eventCategory = 'Order'
                    AND he.hits_eventInfo_eventAction = 'Checkout'
                )
                AND hm.hits_time IS NOT NULL
        ),

        other_hits as (
            SELECT
                T.*,
                lead(T.hits_hitNumber) OVER (
                    partition BY T.visitNumber,
                    T.visitId,
                    T.fullVisitorId,
                    T.visitStartTime
                    ORDER BY
                        T.hits_hitNumber
                ) as o_lhn
            FROM
                (
                    SELECT
                        hm.visitNumber::string as visitNumber,
                        hm.visitId::string as visitId,
                        hm.fullVisitorId,
                        hm.visitStartTime,
                        hm.hits_hitNumber::int as hits_hitnumber,
                        he.hits_eventInfo_eventCategory::string as ec,
                        he.hits_eventInfo_eventAction::string as ea,
                        hm.hits_time::int as hits_time
                    FROM
                        RDS_DEV.BWW_BV.HITS_MAIN_BV hm 
                    JOIN RDS_DEV.BWW_BV.HITS_EVENTINFO_BV he 
                        ON hm.eventinfo_hash = he.eventinfo_hash
                    WHERE
                        he.hits_eventInfo_eventCategory <> 'Navigation'
                        AND NOT (
                            he.hits_eventInfo_eventCategory = 'Order'
                            AND he.hits_eventInfo_eventAction = 'Checkout'
                        )
                        AND he.hits_eventInfo_eventCategory IS NOT NULL
                        AND he.hits_eventInfo_eventAction IS NOT NULL
                        AND hm.hits_time IS NOT NULL
                ) as T
        ),

            /*
                Table min_navigation_order_checkout_in_hits is the result of left join to table other_hits, table navigation_order_checkout_hits.
                The column "n_oc_hits_number" represents the Navigation/(Order | Checkout) hit with the minumum hitNumber between other actions OR
                if the Navigation/(Order | Checkout) action(s) is/are the last event(s) of the session, get this one with the earliest appearance but greater than its predecessor.
            */
            min_navigation_order_checkout_in_hits as (
                SELECT
                    other.visitNumber as visitNumber,
                    other.visitId as visitId,
                    other.fullVisitorId as fullVisitorId,
                    other.visitStartTime as visitStartTime,
                    other.hits_number as o_hits_number,
                    other.o_ec as o_ec,
                    other.o_ea as o_ea,
                    other.hits_time as o_hits_time,
                    other.n_oc_hits_number as n_oc_hits_number,
                    n_oc.ec as n_oc_ec,
                    n_oc.ea as n_oc_ea,
                    n_oc.hits_time as n_oc_hits_time
                FROM
                    (
                        SELECT
                            other_hits.visitNumber as visitNumber,
                            other_hits.visitId as visitId,
                            other_hits.fullVisitorId as fullVisitorId,
                            other_hits.visitStartTime as visitStartTime,
                            other_hits.hits_hitNumber as hits_number,
                            ANY_VALUE(other_hits.ec) as o_ec,
                            ANY_VALUE(other_hits.ea) as o_ea,
                            ANY_VALUE(other_hits.hits_time) as hits_time,
                            min(n_oc_h.hits_hitNumber) as n_oc_hits_number
                        FROM
                            other_hits
                            LEFT JOIN navigation_order_checkout_hits as n_oc_h ON other_hits.visitNumber = n_oc_h.visitNumber
                            AND other_hits.visitId = n_oc_h.visitId
                            AND other_hits.fullVisitorId = n_oc_h.fullVisitorId
                            AND other_hits.visitStartTime = n_oc_h.visitStartTime
                            AND IFF(other_hits.o_lhn IS NULL,
                                    n_oc_h.hits_hitNumber > other_hits.hits_hitNumber,
                                    n_oc_h.hits_hitNumber BETWEEN other_hits.hits_hitNumber AND other_hits.o_lhn)
                        GROUP BY
                            other_hits.visitNumber,
                            other_hits.visitId,
                            other_hits.fullVisitorId,
                            other_hits.visitStartTime,
                            other_hits.hits_hitNumber
                    ) as other
                    LEFT JOIN navigation_order_checkout_hits as n_oc ON other.visitNumber = n_oc.visitNumber
                    AND other.visitId = n_oc.visitId
                    AND other.fullVisitorId = n_oc.fullVisitorId
                    AND other.visitStartTime = n_oc.visitStartTime
                    AND other.n_oc_hits_number = n_oc.hits_hitNumber
            ),

            viewbag_hits as (
                SELECT
                    P.*,
                    lead(P.hits_time) OVER (partition BY P.visitNumber, P.visitId, P.fullVisitorId, P.visitStartTime ORDER BY P.hitNumber) as vh_lht
                FROM
                    (
                        SELECT
                            min_navigation_order_checkout_in_hits.visitNumber as visitNumber,
                            min_navigation_order_checkout_in_hits.visitId as visitId,
                            min_navigation_order_checkout_in_hits.fullVisitorId as fullVisitorId,
                            min_navigation_order_checkout_in_hits.visitStartTime as visitStartTime,
                            min_navigation_order_checkout_in_hits.o_hits_number as hitNumber,
                            min_navigation_order_checkout_in_hits.o_ec as Category,
                            min_navigation_order_checkout_in_hits.o_ea as Action_Type,
                            min_navigation_order_checkout_in_hits.o_hits_time as hits_time
                        FROM
                            min_navigation_order_checkout_in_hits
                        UNION
                        SELECT
                            min_navigation_order_checkout_in_hits.visitNumber as visitNumber,
                            min_navigation_order_checkout_in_hits.visitId as visitId,
                            min_navigation_order_checkout_in_hits.fullVisitorId as fullVisitorId,
                            min_navigation_order_checkout_in_hits.visitStartTime as visitStartTime,
                            min_navigation_order_checkout_in_hits.n_oc_hits_number as hitNumber,
                            min_navigation_order_checkout_in_hits.n_oc_ec as Category,
                            min_navigation_order_checkout_in_hits.n_oc_ea as Action_Type,
                            min_navigation_order_checkout_in_hits.n_oc_hits_time as hits_time
                        FROM
                            min_navigation_order_checkout_in_hits
                    ) as P
                WHERE
                    (
                        (Category = 'Order' AND Action_Type = 'Add to Cart')
                        OR (Category = 'Cart' AND Action_Type IN ('View Cart', 'Save Changes', 'Edit', 'Close'))
                        OR (Category = 'Checkout Start' AND Action_Type = 'Click')
                        OR (Category = 'Order' AND Action_Type = 'Checkout')
                        OR (Category = 'Navigation')
                    )
            ),

        distinct_sessions_key as (
            SELECT
                visit_nbr,
                visit_id,
                full_visitor_id,
                DATE_PART(EPOCH_SECOND,visit_start_ts)::int as visitStartTime,
                source_total_tm_onsite_cnt as totals_timeOnSite
            FROM
                IDM_DEV.CBA_BV.SESSION_SUMMARY_FACT_BV
        )


            SELECT
                visitNumber,
                visitId,
                fullVisitorId,
                visitStartTime,
                CAST((SUM(nexttime - hits_time) / 1000) as integer) as viewBagTime_Seconds
            FROM
                (
                    SELECT
                        distinct h.visitNumber as visitNumber,
                        h.visitId as visitId,
                        h.fullVisitorId as fullVisitorId,
                        h.visitStartTime as visitStartTime,
                        h.hitNumber as hitNumber,
                        h.Category as Category,
                        h.Action_Type as Action_Type,
                        h.hits_Time as hits_Time,
                        CASE
                        /*
                            "nexttime" column creation, based on the next user action hit_Time. Needed to know how long user spend on the Cart.

                            Categories and Action_Type listed below represent the situation when the user closed the cart.
                            The only "hit_time" is needed, that's why the "nexttime" column is these cases is set as NULL, otherwise, time from the given actions will be counted.
                        */
                        WHEN (h.Category = 'Cart' AND h.Action_Type IN ('Close', 'Edit'))
                             OR (h.Category = 'Checkout Start' AND h.Action_Type = 'Click')
                             OR (h.Category = 'Order' AND h.Action_Type = 'Checkout')
                             OR h.Category = 'Navigation'
                        THEN NULL
                        /*
                            If the user begins new session i.e. due to midnight, the visitNumber, visitID, fullVisitorId are the same but visitStartTime is different.
                            In mentioned case, totals_timeOnSite from Session table is needed to use.
                            One second was added to not receive result of subtraction below zero, i.e. hit_Time = 385.284;  totals_timeOnSite = 385
                        */
                        WHEN h.vh_lht IS NULL
                        THEN (ses.totals_timeOnSite + 1) * 1000
                        ELSE h.vh_lht
                        END AS nexttime
                    FROM
                        viewbag_hits as h
                        JOIN distinct_sessions_key as ses ON h.visitNumber = ses.visit_nbr
                        AND h.visitId = ses.visit_id
                        AND h.fullVisitorId = ses.full_visitor_id
                        AND h.visitStartTime = ses.visitStartTime
                )
            WHERE
                nexttime IS NOT NULL
            GROUP BY
                visitNumber,
                visitId,
                fullVisitorId,
                visitStartTime
        )
    WHERE
        viewBagTime_Seconds <> 0
);