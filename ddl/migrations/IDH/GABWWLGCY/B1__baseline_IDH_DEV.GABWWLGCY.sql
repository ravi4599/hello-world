create TABLE IF NOT EXISTS FLYWAY_SCHEMA_HISTORY (
	INSTALLED_RANK NUMBER(38,0) NOT NULL,
	VERSION VARCHAR(50),
	DESCRIPTION VARCHAR(200),
	TYPE VARCHAR(20) NOT NULL,
	SCRIPT VARCHAR(1000) NOT NULL,
	CHECKSUM NUMBER(38,0),
	INSTALLED_BY VARCHAR(100) NOT NULL,
	INSTALLED_ON TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	EXECUTION_TIME NUMBER(38,0) NOT NULL,
	SUCCESS BOOLEAN NOT NULL,
	primary key (INSTALLED_RANK)
);
create view IF NOT EXISTS ADD_TO_CART_VAPP(
	DATASETID,
	TIMESTAMP,
	VISITORID,
	SESSIONID,
	SESSIONNUMBER,
	EVENTCATEGORY,
	EVENTACTION,
	HITNUMBER,
	PRODUCTNAME,
	PRODUCTPRICE,
	QUANTITY,
	CARTORDER
) as (
        SELECT
            *
        FROM
            (
                (
                    SELECT
                        ANY_VALUE(dataset_Id) as DatasetId,
                        TO_CHAR(
                            to_timestamp(VisitStartTime),
                            'MM/DD/YYYY HH24:MI:SS.FF3'
                        ) as Timestamp,
                        FullVisitorId as VisitorId,
                        VisitId::string as SessionId,
                        VisitNumber::string as SessionNumber,
                        ANY_VALUE(hits_eventInfo_eventCategory) as EventCategory,
                        ANY_VALUE(hits_eventInfo_eventAction) as EventAction,
                        hits_hitNumber as HitNumber,
                        ANY_VALUE(hits_product_v2ProductName) as ProductName,
                        ANY_VALUE(hits_product_productPrice / POW(10, 6)) as ProductPrice,
                        ANY_VALUE(ifnull(hits_product_productQuantity::int, 1
                )
            ) as Quantity,
            DENSE_RANK() OVER (
                PARTITION BY SessionNumber,
                SessionId,
                Timestamp,
                VisitorId
                ORDER BY
                    HitNumber
            ) as CartOrder
        FROM
            RDS_DEV.BWW.HIT
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
        GROUP BY
            Timestamp,
            VisitorId,
            SessionId,
            SessionNumber,
            HitNumber
    )
UNION
    (
        SELECT
            ANY_VALUE(dataset_Id) as DatasetId,
            to_char(
                to_timestamp(LEFT(event_timestamp, 13)),
                'MM/DD/YYYY HH:MM:SS.FF3'
            ) as Timestamp,
            user_pseudo_id as VisitorId,
            IFF(
                LEN("'ga_session_id'") = 0,
                'NULL',
                "'ga_session_id'"
            ) as SessionId,
            IFF(
                LEN("'ga_session_number'") = 0,
                'NULL',
                "'ga_session_number'"
            ) as SessionNumber,
            ANY_VALUE(event_name) as EventCategory,
            ANY_VALUE(event_name) as EventAction,
            NULL as HitNumber,
            ANY_VALUE(items_item_name) as ProductName,
            ANY_VALUE(items_price) as ProductPrice,
            ANY_VALUE("'quantity'"::int) as Quantity,
            DENSE_RANK() OVER (
                PARTITION BY SessionId,
                SessionNumber,
                VisitorId
                ORDER BY
                    Timestamp
            ) as CartOrder
        FROM
            RDS_DEV.BWW.GA_EVENTS PIVOT(
                listagg(event_params_value) for event_params_key in ('ga_session_id', 'ga_session_number', 'quantity')
            ) as p
        WHERE
            event_name = 'add_to_cart'
        GROUP BY
            Timestamp,
            VisitorId,
            SessionId,
            SessionNumber
    )
)
);
create view IF NOT EXISTS CART_ADDITIONS(
	DATASETID,
	TIMESTAMP,
	FULLVISITORID,
	SESSIONID,
	SESSIONNUMBER,
	EVENTCATEGORY,
	EVENTACTION,
	HITNUMBER,
	PRODUCTNAME,
	PRODUCTPRICE,
	QUANTITY,
	CARTORDER
) as (
    SELECT
        distinct dataset_Id as DatasetId,
        TO_CHAR(
            to_timestamp(VisitStartTime),
            'MM/DD/YYYY HH24:MI:SS.FF3'
        ) as Timestamp,
        FullVisitorId as FullVisitorId,
        VisitId::string as SessionId,
        VisitNumber::string as SessionNumber,
        hits_eventInfo_eventCategory as EventCategory,
        hits_eventInfo_eventAction as EventAction,
        hits_hitNumber as HitNumber,
        hits_product_v2ProductName as ProductName,
        hits_product_productPrice / POW(10, 6) as ProductPrice,
        -- by default in source hits_product_productPrice is multiplied by 10^6 (e.g., 2.40 would be given as 2400000)
        ifnull(hits_product_productQuantity::int, 1
) as Quantity,
DENSE_RANK() OVER (
    PARTITION BY SessionNumber,
    SessionId,
    Timestamp,
    FullVisitorId
    ORDER BY
        HitNumber
) as CartOrder
FROM
    RDS_DEV.BWW.HIT
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
UNION
SELECT
    distinct dataset_Id as DatasetId,
    to_char(
        to_timestamp(LEFT(event_timestamp, 13)),
        'MM/DD/YYYY HH:MM:SS.FF3'
    ) as Timestamp,
    -- microseconds converted to milliseconds         
    user_pseudo_id as FullVisitorId,         IFF(LEN("'ga_session_id'") = 0, 'NULL',"'ga_session_id'")  as SessionId,         IFF(LEN("'ga_session_number'") = 0, 'NULL', "'ga_session_number'") as SessionNumber,         event_name as EventCategory,         event_name as EventAction,         NULL as HitNumber, -- not provided in GA_EVENTS, that's why all column has NULLs         
    items_item_name as ProductName,         items_price as ProductPrice,         "'quantity'"::int as Quantity,         DENSE_RANK() OVER (PARTITION BY SessionId, SessionNumber, FullVisitorId ORDER BY Timestamp) as CartOrder     FROM RDS_DEV.BWW.GA_EVENTS     PIVOT(listagg(event_params_value) for event_params_key in ('ga_session_id', 'ga_session_number', 'quantity')) as p     WHERE event_name = 'add_to_cart' );
create view IF NOT EXISTS CART_PURCHASE_FLOW_PROGRESS_VAPP(
	FULLVISITORID,
	VISITID,
	VISITNUMBER,
	ABANDONED_CART,
	PURCHASE_FLOW_STEP,
	PURCHASE_FLOW_NAME
) as (
    WITH MAX_STEP_BY_CUSTOMER AS (
        SELECT
            FULLVISITORID,
            VISITID::string as VISITID,
            VISITNUMBER,
            MAX(HITS_ECOMMERCEACTION_STEP) as Purchase_Flow_Step
        FROM RDS_DEV.BWW_BV.HIT_BV
        GROUP BY 1,2,3
    )
    SELECT
        SBC.FULLVISITORID,
        SBC.VISITID,
        SBC.VISITNUMBER,
        CASE WHEN SBC.Purchase_Flow_Step between 1 and 5 THEN 'yes'
             WHEN SBC.Purchase_Flow_Step=6 THEN 'no'
             ELSE 'N/A' END AS abandoned_cart, -- step 6 is checkout complete and if customers max step is lower then 6 then he abandoned cart
        SBC.Purchase_Flow_Step,
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
create view IF NOT EXISTS CART_PURCHASE_FLOW_PROGRESS_VAPP_V2(
	FULLVISITORID,
	VISITID,
	VISITNUMBER,
	ABANDEONED_CART,
	PURCHASE_FLOW_STEP,
	PURCHASE_FLOW_NAME
) as (
    WITH MAX_STEP_BY_CUSTOMER AS (
    SELECT
        m.FULLVISITORID,
        m.VISITID::string as VISITID,
        m.VISITNUMBER,
        MAX(e.HITS_ECOMMERCEACTION_STEP) as Purchase_Flow_Step
    FROM RDS_DEV.BWW_BV.HITS_MAIN_BV m
    LEFT JOIN RDS_DEV.BWW_BV.HITS_ECOMMERCEACTION_BV e
    ON m.ecommerceaction_hash = e.ecommerceaction_hash
    GROUP BY 1,2,3
)
    SELECT
        SBC.FULLVISITORID,
        SBC.VISITID,
        SBC.VISITNUMBER,
        CASE WHEN SBC.Purchase_Flow_Step between 1 and 5 THEN 'yes'
             WHEN SBC.Purchase_Flow_Step=6 THEN 'no'
             ELSE 'N/A' END AS abandeoned_cart, -- step 6 is checkout complete and if customers max step is lower then 6 then he abandoned cart
        SBC.Purchase_Flow_Step,
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
create view IF NOT EXISTS CART_REMOVALS(
	DATASETID,
	TIMESTAMP,
	FULLVISITORID,
	SESSIONID,
	SESSIONNUMBER,
	EVENTCATEGORY,
	EVENTACTION,
	PRODUCTNAME,
	PRODUCTPRICE,
	QUANTITY
) as (
                SELECT
                    distinct dataset_Id as DatasetId,
                    TO_CHAR(
                        to_timestamp(VisitStartTime),
                        'MM/DD/YYYY HH24:MI:SS'
                    ) as Timestamp,
                    FullVisitorId as FullVisitorId,
                    VisitId::string as SessionId,
                    VisitNumber::string as SessionNumber,
                    hits_eventInfo_eventCategory as EventCategory,
                    hits_eventInfo_eventAction as EventAction,
                    hits_product_v2ProductName as ProductName,
                    hits_product_productPrice / POW(10, 6) as ProductPrice,
                    -- by default in source hits_product_productPrice is multiplied by 10^6 (e.g., 2.40 would be given as 2400000)     
                IFNULL(hits_product_productQuantity::int, 1) as Quantity FROM     RDS_DEV.BWW.HIT WHERE     hits_eventInfo_eventCategory = 'Cart'     AND hits_eventInfo_eventAction = 'Remove' UNION SELECT     distinct     dataset_Id as DatasetId,     TO_CHAR(to_timestamp(LEFT(event_timestamp, 13)), 'MM/DD/YYYY HH24:MI:SS') as Timestamp, -- microseconds converted to milliseconds    
                user_pseudo_id as FullVisitorId,     "'ga_session_id'" as SessionId,     "'ga_session_number'" as SessionNumber,     event_name as EventCategory,     event_name as EventAction,     items_item_name as ProductName,     items_price as ProductPrice,     "'quantity'"::int as Quantity FROM     RDS_DEV.BWW.GA_EVENTS     PIVOT(listagg(event_params_value) for event_params_key in ('ga_session_id', 'ga_session_number', 'quantity')) as p WHERE     event_name = 'remove_from_cart' );
create view IF NOT EXISTS CHECKOUT_SUMMARY(
	USER_ID,
	USER_PSEUDO_ID,
	GA_SESSION_ID,
	GA_SESSION_NUMBER,
	TRANSACTION_ID,
	EVENT_DATE_UTC,
	EVENT_HHMMSS,
	EVENT_DATETIME,
	ISDELIVERY,
	PICKUPTIME,
	SHIPPING,
	TAX,
	TIP,
	SUBTOTAL,
	VALUE
) as (
    with ecommerce_purchase_events as (
        SELECT
            distinct event_timestamp,
            TO_DATE(TO_TIMESTAMP(event_timestamp::string)) AS EVENT_DATE_UTC,
            --date and time is collected from event_timestamp column in UTC
            TO_TIME(TO_TIMESTAMP(event_timestamp::string)) AS EVENT_HHMMSS,
            TO_TIMESTAMP(event_timestamp::string) AS EVENT_DATETIME,
            user_id,
            user_pseudo_id,
            user_properties_key,
            user_properties_value,
            event_params_key,
            event_params_value
        FROM
            RDS_DEV.BWW.GA_EVENTS
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
        ga_session_number,
        transaction_id,
        EVENT_DATE_UTC,
        EVENT_HHMMSS,
        EVENT_DATETIME,
        isDelivery,
        pickuptime,
        shipping,
        tax,
        tip,(value - tax - shipping - tip) as subtotal,
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
            EVENT_HHMMSS,
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
            EVENT_HHMMSS,
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
	FULLVISITORID,
	SESSIONID,
	SESSIONNUMBER,
	TIMESTAMP,
	PRODUCTNAME,
	PRODUCTQUANTITY,
	IS_PROMO
) as (
    SELECT
        *
    FROM(
            SELECT
                DISTINCT H.dataset_Id as dataset_Id,
                H.FullVisitorId as FullVisitorId,
                H.VisitId::string as SessionId,
                H.VisitNumber::string as SessionNumber,
                TO_CHAR(
                    DATEADD(
                        millisecond,
                        ZEROIFNULL(H.HITS_TIME),
                        TO_TIMESTAMP(S.visitStartTime)
                    ),
                    'MM/DD/YYYY HH24:MI:SS'
                ) as Timestamp,
                H.HITS_PRODUCT_V2PRODUCTNAME as ProductName,
                H.HITS_PRODUCT_PRODUCTQUANTITY as ProductQuantity,
                CASE
                    WHEN UPPER(H.HITS_PRODUCT_V2PRODUCTNAME) LIKE ANY ('BOGO%', '%+%', '%LCS%', '%BUNDLE%', '%BUY%GET%') THEN 'Yes'
                    ELSE 'No'
                END AS IS_PROMO -- all products containting this characters are considered a promo
            FROM
                RDS_DEV.BWW.HIT as H
                join RDS_DEV.BWW.SESSION as S on H.VisitID = S.VisitID
                and H.VisitNumber = S.VisitNumber
                and H.FullVisitorID = S.FullVisitorID
            WHERE
                H.HITS_PRODUCT_V2PRODUCTNAME IS NOT NULL
                AND H.HITS_PRODUCT_PRODUCTQUANTITY IS NOT NULL
                AND H.HITS_TRANSACTION_TRANSACTIONID IS NOT NULL -- purchase is made only if there is transaction_id
            UNION
            SELECT
                DISTINCT dataset_Id,
                user_pseudo_id as FullVisitorId,
                "'ga_session_id'" as SessionId,
                "'ga_session_number'" as SessionNumber,
                TO_CHAR(
                    to_timestamp(LEFT(event_timestamp, 13)),
                    'MM/DD/YYYY HH24:MI:SS'
                ) as Timestamp,
                items_item_name AS ProductName,
                items_quantity ProductQuantity,
                CASE
                    WHEN UPPER(items_item_name) LIKE ANY ('BOGO%', '%+%', '%LCS%', '%BUNDLE%', '%BUY%GET%') THEN 'Yes'
                    ELSE 'No'
                END AS IS_PROMO -- all products containting this characters are considered a promo
            FROM
                RDS_DEV.BWW.GA_EVENTS pivot(
                    listagg(user_properties_value) for user_properties_key in ('ga_session_id', 'ga_session_number')
                ) p
            WHERE
                event_name = 'ecommerce_purchase' -- in order to take only products which were purchased     
        and items_item_name is not null and items_quantity is not null  ));
create view IF NOT EXISTS SESSION_CAMPAIGN_VAPP(
	VISITOR_ID,
	VISIT_ID,
	VISIT_NBR,
	VISIT_START_TM,
	CHANNEL_GROUPING_NM,
	TRAFFIC_SOURCE_MEDIUM_NM,
	TRAFFIC_SOURCE_NM,
	TRAFFIC_SOURCE_CAMPAIGN_NM,
	LOAD_ID,
	LOAD_DTTM
) as
SELECT
    SESSION.FULLVISITORID,
    SESSION.VISITID,
    SESSION.VISITNUMBER,
    SESSION.VISITSTARTTIME,
    SESSION.CHANNELGROUPING,
    SESSION.TRAFFICSOURCE_MEDIUM,
    SESSION.TRAFFICSOURCE_SOURCE,
    SESSION.TRAFFICSOURCE_CAMPAIGN,
    SESSION.LOAD_ID,
    SESSION.LOAD_DATE
FROM
    RDS_DEV.BWW.SESSION;
create view IF NOT EXISTS SESSION_SUMMARY_VAPP(
	FULL_VISITOR_ID,
	VISIT_NUMBER,
	VISIT_ID,
	DATE,
	USERID,
	PROFILE_ID,
	PROFILEGUID,
	MEMBER_PHONENUMBER,
	FIRST_NAME,
	LAST_NAME,
	EMAIL_ADDRESS,
	CHANNELGROUPING,
	TRAFFICSOURCE_MEDIUM,
	TRAFFICSOURCE_SOURCE,
	TRAFFICSOURCE_CAMPAIGN,
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
	STORENUMBER
) as (
SELECT
    FULL_VISITOR_ID,
    VISIT_NUMBER,
    VISIT_ID,
    DATE,
    USERID,
    PROFILE_ID,
    PROFILEGUID,
    MEMBER_PHONENUMBER,
    FIRST_NAME,
    LAST_NAME,
    EMAIL_ADDRESS,
    CHANNELGROUPING,
    TRAFFICSOURCE_MEDIUM,
    TRAFFICSOURCE_SOURCE,
    TRAFFICSOURCE_CAMPAIGN,
    START_SESSION_DATETIME_UTC AS SESSION_START_TIME_UTC,
    START_SESSION_DATE_UTC AS SESSION_START_DATE_UTC,
    START_SESSION_HHMMSS_UTC AS SESSION_START_HHMMSS_UTC,
    END_SESSION_DATETIME AS SESSION_END_TIME_UTC,
    SESSION_DURATION,
    store.TIMEZONE AS LOCAL_TIMEZONE,
    ITPRCS_DEV.CSO.CONVERT_TIMESTAMP_FROM_UTC_TO_TIMEZONE(SESSION_START_TIME_UTC, LOCAL_TIMEZONE) AS SESSION_START_TIME_LOCAL,
    ITPRCS_DEV.CSO.CONVERT_TIMESTAMP_FROM_UTC_TO_TIMEZONE(SESSION_END_TIME_UTC, LOCAL_TIMEZONE) AS SESSION_END_TIME_LOCAL,
    sh.LOAD_ID,
    LOAD_DATE,
    DATASET_ID,
    NULLIF(sh.STORE_NUMBER, 0) AS StoreNumber
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
LEFT JOIN POLARIS_DEV.SHDM.STORE_DIM store on sh.STORE_NUMBER = store.STORE_ID::STRING and store.BRAND_ID='bww');
create view IF NOT EXISTS TIME_ON_BAG(
	VISITNUMBER,
	VISITID,
	FULLVISITORID,
	VISITSTARTTIME,
	VIEWBAGTIME_SECONDS
) as (
        SELECT
            visitNumber,
            visitId,
            fullVisitorId,
            visitStartTime,
            viewBagTime_Seconds
        FROM
            (
                WITH navigation_order_checkout_hits as (
                    SELECT
                        distinct visitNumber::string as visitNumber,
                        visitId::string as visitId,
                        fullVisitorId,
                        visitStartTime,
                        hits_hitNumber,
                        hits_eventInfo_eventCategory as ec,
                        hits_eventInfo_eventAction as ea,
                        hits_time
                    FROM
                        RDS_DEV.BWW.HIT
                    WHERE
                        hits_eventInfo_eventCategory = 'Navigation'
                        OR (
                            hits_eventInfo_eventCategory = 'Order'
                            AND hits_eventInfo_eventAction = 'Checkout'
                        )
                        AND hits_time IS NOT NULL
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
                                distinct visitNumber::string as visitNumber,
                                visitId::string as visitId,
                                fullVisitorId,
                                visitStartTime,
                                hits_hitNumber,
                                hits_eventInfo_eventCategory as ec,
                                hits_eventInfo_eventAction as ea,
                                hits_time
                            FROM
                                RDS_DEV.BWW.HIT
                            WHERE
                                hits_eventInfo_eventCategory <> 'Navigation'
                                AND NOT (
                                    hits_eventInfo_eventCategory = 'Order'
                                    AND hits_eventInfo_eventAction = 'Checkout'
                                )
                                AND hits_eventInfo_eventCategory IS NOT NULL
                                AND hits_eventInfo_eventAction IS NOT NULL
                                AND hits_time IS NOT NULL
                        ) as T
                ),
                /*                 Table min_navigation_order_checkout_in_hits is the result of left join to table other_hits, table navigation_order_checkout_hits.                 The column "n_oc_hits_number" represents the Navigation/(Order | Checkout) hit with the minumum hitNumber between other actions OR                 if the Navigation/(Order | Checkout) action(s) is/are the last event(s) of the session, get this one with the earliest appearance but greater than its predecessor.             */
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
                                AND IFF(
                                    other_hits.o_lhn IS NULL,
                                    n_oc_h.hits_hitNumber > other_hits.hits_hitNumber,
                                    n_oc_h.hits_hitNumber BETWEEN other_hits.hits_hitNumber
                                    AND other_hits.o_lhn
                                )
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
                        lead(P.hits_time) OVER (
                            partition BY P.visitNumber,
                            P.visitId,
                            P.fullVisitorId,
                            P.visitStartTime
                            ORDER BY
                                P.hitNumber
                        ) as vh_lht
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
                            (
                                Category = 'Order'
                                AND Action_Type = 'Add to Cart'
                            )
                            OR (
                                Category = 'Cart'
                                AND Action_Type IN ('View Cart', 'Save Changes', 'Edit', 'Close')
                            )
                            OR (
                                Category = 'Checkout Start'
                                AND Action_Type = 'Click'
                            )
                            OR (
                                Category = 'Order'
                                AND Action_Type = 'Checkout'
                            )
                            OR (Category = 'Navigation')
                        )
                ),
                distinct_sessions_key as (
                    SELECT
                        distinct visitNumber,
                        visitId,
                        fullVisitorId,
                        visitStartTime,
                        totals_timeOnSite
                    FROM
                        RDS_DEV.BWW.SESSION
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
                                /*                             "nexttime" column creation, based on the next user action hit_Time. Needed to know how long user spend on the Cart.                             Categories and Action_Type listed below represent the situation when the user closed the cart.                             The only "hit_time" is needed, that's why the "nexttime" column is these cases is set as NULL, otherwise, time from the given actions will be counted.                         */
                                WHEN (
                                    h.Category = 'Cart'
                                    AND h.Action_Type IN ('Close', 'Edit')
                                )
                                OR (
                                    h.Category = 'Checkout Start'
                                    AND h.Action_Type = 'Click'
                                )
                                OR (
                                    h.Category = 'Order'
                                    AND h.Action_Type = 'Checkout'
                                )
                                OR h.Category = 'Navigation' THEN NULL
                                /*                             If the user begins new session i.e. due to midnight, the visitNumber, visitID, fullVisitorId are the same but visitStartTime is different.                             In mentioned case, totals_timeOnSite from Session table is needed to use.                             One second was added to not receive result of subtraction below zero, i.e. hit_Time = 385.284;  totals_timeOnSite = 385                         */
                                WHEN h.vh_lht IS NULL THEN (ses.totals_timeOnSite + 1) * 1000
                                ELSE h.vh_lht
                            END AS nexttime
                        FROM
                            viewbag_hits as h
                            JOIN distinct_sessions_key as ses ON h.visitNumber = ses.visitNumber
                            AND h.visitId = ses.visitId
                            AND h.fullVisitorId = ses.fullVisitorId
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
create view IF NOT EXISTS TRANSFORM_SESSION_CAMPAIGN_VAPP(
	FULL_VISITOR_ID,
	VISIT_NUMBER,
	VISIT_ID,
	DATE,
	CHANNELGROUPING,
	TRAFFICSOURCE_MEDIUM,
	TRAFFICSOURCE_SOURCE,
	TRAFFICSOURCE_CAMPAIGN,
	START_SESSION_DATETIME_UTC,
	START_SESSION_DATE_UTC,
	START_SESSION_HHMMSS_UTC,
	END_SESSION_DATETIME,
	SESSION_DURATION,
	LOAD_ID,
	LOAD_DATE,
	DATASET_ID
) as (
        SELECT
            S.FULLVISITORID as FULL_VISITOR_ID,
            S.VISITNUMBER as VISIT_NUMBER,
            S.VisitId::string as VISIT_ID,
            S.DATE,
            ANY_VALUE(S.channelgrouping) as CHANNELGROUPING,
            ANY_VALUE(S.TRAFFICSOURCE_MEDIUM) as TRAFFICSOURCE_MEDIUM,
            ANY_VALUE(S.TrafficSource_source) as TRAFFICSOURCE_SOURCE,
            ANY_VALUE(S.TRAFFICSOURCE_CAMPAIGN) as TRAFFICSOURCE_CAMPAIGN,
            ANY_VALUE(TO_TIMESTAMP(S.visitStartTime)) as START_SESSION_DATETIME_UTC,
            ANY_VALUE(TO_DATE(TO_TIMESTAMP(S.visitStartTime))) as START_SESSION_DATE_UTC,
            ANY_VALUE(TO_TIME(TO_TIMESTAMP(S.visitStartTime))) as START_SESSION_HHMMSS_UTC,
            ANY_VALUE(
                DATEADD(
                    second,
                    ZEROIFNULL(S.totals_timeOnSite),
                    TO_TIMESTAMP(S.visitStartTime)
                )
            ) as END_SESSION_DATETIME,
            ANY_VALUE(ZEROIFNULL(S.totals_timeOnSite)) AS SESSION_DURATION,
            ANY_VALUE(S.LOAD_ID) as LOAD_ID,
            ANY_VALUE(S.LOAD_DATE) as LOAD_DATE,
            ANY_VALUE(S.dataset_id) as DATASET_ID
        FROM
            RDS_DEV.BWW.SESSION S
        GROUP BY
            1,
            2,
            3,
            4
    );
create or replace pipe PIPE auto_ingest=true integration='UDP_STORAGE_INT_DEV_QUEUE' as copy into IDH_DEV.BWW.a
  from @IDH_DEV.BWW.stage_test
  file_format = (type = 'CSV');
create or replace pipe PIPE_QA auto_ingest=true integration='UDP_STORAGE_INT_QA_QUEUE' as copy into IDH_DEV.BWW.b
  from @IDH_DEV.BWW.stage_test
  file_format = (type = 'CSV');
create or replace pipe TEST auto_ingest=true integration='UDP_STORAGE_INT_DEV_QUEUE' as copy into IDH_DEV.BWW.A
 FROM @stage_test
 --PATTERN = '.csv'
  file_format = (type = 'CSV');