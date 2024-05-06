CREATE OR REPLACE VIEW seaware.seaware_revenue_fact AS
SELECT snapshot_date,
       RES_ID,
       GUEST_ID,
       package_id,
       ship_id,
       agency_id,
       agent_id,
       sail_id,
       shorex_id,
	   hotel_res_req_id,
       nvl(invoice_item_type_id, -1) AS invoice_item_type_id,
       nvl(price_area_id, -1) AS price_area_id,
       addon_id,
       promotion_id,
       Amount,
       VOYAGE_AMOUNT,
       NVL(TAXESandFEES, 0) AS TAXESandFEES,
       NVL(MANUAL_ADJ, 0) AS MANUAL_ADJ,
       NVL(VOYAGE_PROTECT, 0) AS INSURANCE,
       NVL((VOYAGE_PROTECT *.542),0) AS AON_INSURACE_PAYMENT,
       ((VOYAGE_AMOUNT)* COMMISSION_PERCENT)/100 AS VOYAGE_COMMISSION_AMT, --((VOYAGE_AMOUNT+NVL(MANUAL_ADJ,0))* COMMISSION_PERCENT )/100 AS VOYAGE_COMMISSION_AMT,
(TAXESandFEES *COMMISSION_PERCENT)/ 100 AS TAX_COMMISION_AMT,
                                                                                                                                                                   nvl(VOYAGE_AMOUNT*0.025, 0) AS VOYAGE_CREDIT_CARD_FEE,
                                                                                                                                                                   nvl(INSURANCE*0.025, 0) AS INSURANCE_CREDIT_CARD_FEE,
                                                                                                                                                                   nvl(TAXESandFEES*0.025, 0) AS TAXESandFEES_CREDIT_CARD_FEE,
                                                                                                                                                                   (nvl(VOYAGE_AMOUNT, 0)+nvl(TAXESandFEES, 0)) AS ACTUAL_GROSS_TICKET_REVENUE, --(nvl(VOYAGE_AMOUNT,0)+nvl(TAXESandFEES,0)+nvl(MANUAL_ADJ,0)) AS ACTUAL_GROSS_TICKET_REVENUE,
NVL(VOYAGE_PROTECT, 0) AS ACTUAL_GROSS_OTHER_REVENUE,
                                                                                                                                                                                                                                                                                                                                              (ACTUAL_GROSS_TICKET_REVENUE+ACTUAL_GROSS_OTHER_REVENUE) AS ACTUAL_GROSS_TOTAL_REVENUE, --(nvl(VOYAGE_AMOUNT,0)+nvl(TAXESandFEES,0)-nvl(VOYAGE_COMMISSION_AMT,0)- (VOYAGE_CREDIT_CARD_FEE+INSURANCE_CREDIT_CARD_FEE+TAXESandFEES_CREDIT_CARD_FEE )) AS ACTUAL_NET_NET_TICKET_REVENUE,
--(ACTUAL_NET_NET_TICKET_REVENUE) AS ACTUAL_NET_NET_TOTAL_REVENUE,
NET_DUE,
                                                                  currency,
                                                                  currency_rate,
                                                                  commission_percent,
                                                                  (Amount * currency_rate) AS Amount_USD ,
                                                                  (VOYAGE_AMOUNT * currency_rate) AS VOYAGE_AMOUNT_USD ,
                                                                  (TAXESandFEES * currency_rate) AS TAXESandFEES_USD,
                                                                  (MANUAL_ADJ * currency_rate) AS MANUAL_ADJ_USD ,
                                                                  (INSURANCE * currency_rate) AS INSURANCE_USD ,
                                                                  (AON_INSURACE_PAYMENT * currency_rate) AS AON_INSURACE_PAYMENT_USD ,
                                                                  (VOYAGE_COMMISSION_AMT * currency_rate) AS VOYAGE_COMMISSION_AMT_USD,
                                                                  (TAX_COMMISION_AMT * currency_rate) AS TAX_COMMISION_AMT_USD,
                                                                  (VOYAGE_CREDIT_CARD_FEE * currency_rate) AS VOYAGE_CREDIT_CARD_FEE_USD,
                                                                  (INSURANCE_CREDIT_CARD_FEE * currency_rate) AS INSURANCE_CREDIT_CARD_FEE_USD,
                                                                  (TAXESandFEES_CREDIT_CARD_FEE * currency_rate) AS TAXESandFEES_CREDIT_CARD_FEE_USD ,
                                                                  (ACTUAL_GROSS_TICKET_REVENUE * currency_rate) AS ACTUAL_GROSS_TICKET_REVENUE_USD ,
                                                                  (ACTUAL_GROSS_OTHER_REVENUE * currency_rate) AS ACTUAL_GROSS_OTHER_REVENUE_USD,
                                                                  (ACTUAL_GROSS_TOTAL_REVENUE * currency_rate) AS ACTUAL_GROSS_TOTAL_REVENUE_USD --(ACTUAL_NET_NET_TOTAL_REVENUE * currency_rate ) AS   ACTUAL_NET_NET_TOTAL_REVENUE_USD

FROM
  (SELECT FACT.snapshot_date as snapshot_date,
          FACT.res_id as res_id,
          FACT.guest_id as guest_id,
          CASE WHEN FACT.package_id=-1 THEN PKG.package_id ELSE FACT.package_id END as package_id,
          ship_id,
          agency_id,
          agent_id,
          PKG.sail_id as sail_id,
          shorex_id,
		  hotel_res_req_id,
          FACT.invoice_item_type_id,
          FACT.price_area_id,
          addon_id,
          promotion_id,
          SUM(amount) AS amount,
          SUM (CASE
                   WHEN INVOICE_ITEM_TYPE = 'VOYAGE FARE' THEN AMOUNT
               END) AS VOYAGE_AMOUNT, --SUM (CASE WHEN INVOICE_ITEM_TYPE =  'VOYAGE FARE' AND  PRICE_AREA <> 'MANUAL ADJ' THEN AMOUNT END ) AS VOYAGE_AMOUNT,
SUM(CASE
        WHEN INVOICE_ITEM_TYPE = 'TAXES & FEES' THEN AMOUNT
    END) AS TAXESandFEES,
                                                                                                                                                             SUM(CASE
                                                                                                                                                                     WHEN PRICE_AREA = 'MANUAL ADJ' THEN AMOUNT
                                                                                                                                                                 END) AS MANUAL_ADJ,
                                                                                                                                                             SUM(CASE
                                                                                                                                                                     WHEN INVOICE_ITEM_TYPE = 'INSURANCE' THEN AMOUNT
                                                                                                                                                                 END) AS VOYAGE_PROTECT,
                                                                                                                                                             NET_DUE,
                                                                                                                                                             COMMISSION_PERCENT,
                                                                                                                                                             currency,
                                                                                                                                                             currency_rate
   FROM hive_schema_stg.seaware_revenue_fact FACT
   LEFT  JOIN seaware.seaware_invoice_item_type_lkp INV ON FACT.invoice_item_type_id = INV.invoice_item_type_id
   LEFT  JOIN seaware.seaware_price_area_lkp PA ON FACT.price_area_id = PA.price_area_id 
   LEFT JOIN (select distinct fct.snapshot_date,fct.res_id,fct.guest_id,fct.package_id,fct.sail_id from hive_schema_stg.seaware_revenue_fact fct 
join hive_schema_stg.seaware_package_dim pd on pd.package_id = fct.package_id 
where fct.package_id<>-1 and fct.sail_id<>-1 and pd.package_class='VOYAGE') PKG ON FACT.snapshot_date=PKG.snapshot_date and FACT.res_id=PKG.res_id and FACT.guest_id=PKG.guest_id 
    GROUP BY FACT.snapshot_date,
            FACT.res_id,
            FACT.guest_id,
            PKG.package_id,
			FACT.package_id,
            ship_id,
            agency_id,
            agent_id,
            PKG.sail_id,
            shorex_id,
			hotel_res_req_id,
            FACT.invoice_item_type_id,
            FACT.price_area_id,
            addon_id,
            promotion_id,
            COMMISSION_PERCENT,
            currency,
            currency_rate,
            NET_DUE) BASE_SQL WITH NO SCHEMA binding;




GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;
GRANT USAGE ON SCHEMA seaware TO  group integration_user_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;
grant select on all tables in schema seaware to group integration_user_group ;