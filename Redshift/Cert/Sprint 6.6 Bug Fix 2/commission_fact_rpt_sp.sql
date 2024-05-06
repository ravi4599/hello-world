CREATE OR REPLACE PROCEDURE seaware.commission_fact_rpt_sp()
	LANGUAGE plpgsql
AS $$ 	 	 	 	 	 	                                                                    

BEGIN
delete from  seaware.commission_fact_rpt;
INSERT INTO seaware.commission_fact_rpt
with s2 as
    (select max(snapshot_date) as max_snap_date from hive_schema_stg.seaware_commission_fact)
select
    seaware_agency_dim.src_agency_id seaware_agency_id
  , crm_agency_master.id             crm_agency_id
  , seaware_agency_dim.agency_name
  , crm_agency_master.billingcity
  , crm_agency_master.Currency_Type__c
  , crm_agent_master.seaware_agent_id__c seaware_agent_id
  , crm_agent_master.id                  crm_agent_id
  , crm_agent_master.name                agent_name
  , crm_agent_master.agent_status__c
  , seaware_reservation_dim.src_res_id
  , seaware_reservation_dim.res_status
  , seaware_ship_dim.ship_name ship_name
  , seaware_sail_dim.sail_date_from
  , seaware_package_dim.package_name product_name
  , seaware_commission_type_lkp.commission_code
  , seaware_invoice_item_type_lkp.invoice_item_type
  , revenue_fact.booking_currency
  , revenue_fact.item_amount
  , seaware_commission_fact.currency_code commission_currency
  , seaware_commission_fact.commission_amount
  , seaware_commission_fact.commission_payout_date
  , case
        when (
                revenue_fact.net_due=0
                and datediff(day,seaware_commission_fact.snapshot_date,seaware_sail_dim.sail_date_from)<=120
                and datediff(day,tran.trans_time_stamp,next_day(CURRENT_DATE,'Friday'))>2
            )
            then
                (
                    case
                        when mod(date_part(w,tran.trans_time_stamp)::INT,2)=0
                            then next_day(tran.trans_time_stamp,'Friday')
                            else dateadd(day,7,next_day(tran.trans_time_stamp,'Friday'))
                    end
                )
    end expected_payout_date
from
    s2
    inner join
        hive_schema_stg.seaware_commission_fact seaware_commission_fact
        on
            s2.max_snap_date = seaware_commission_fact.snapshot_date
    left join
        hive_schema_stg.seaware_commission_type_lkp seaware_commission_type_lkp
        on
            seaware_commission_fact.commission_type_skey = seaware_commission_type_lkp.commission_type_id
    left join
        hive_schema_stg.seaware_invoice_item_type_lkp seaware_invoice_item_type_lkp
        on
            seaware_commission_fact.invoice_item_type_id = seaware_invoice_item_type_lkp.invoice_item_type_id
    left join
        hive_schema_stg.seaware_agency_dim seaware_agency_dim
        on
            seaware_commission_fact.agency_id = seaware_agency_dim.agency_id
    left join
        hive_schema_stg.crm_agency_master crm_agency_master
        on
            seaware_agency_dim.src_agency_id = crm_agency_master.seaware_agency_id__c and crm_agency_master.rec_end_dttm='9999-12-31 00:00:00' 
    left join
        hive_schema_stg.crm_agent_master crm_agent_master
        on
            seaware_commission_fact.agent_id = crm_agent_master.agent_id
    left join
        hive_schema_stg.seaware_reservation_dim seaware_reservation_dim
        on
            seaware_commission_fact.res_id = seaware_reservation_dim.res_id
    left join
        hive_schema_stg.seaware_sail_dim seaware_sail_dim
        on
            seaware_commission_fact.sail_id = seaware_sail_dim.sail_id
    left join
        hive_schema_stg.seaware_package_dim seaware_package_dim
        on
            seaware_package_dim.package_id = seaware_commission_fact.package_id
    left join
        seaware.seaware_ship_dim seaware_ship_dim
        on
            seaware_sail_dim.ship_id = seaware_ship_dim.ship_id
    left join
        (
            select
                res_id
              , trans_time_stamp
            from
                hive_schema_stg.seaware_transaction_evt a
            where
                trans_id =
                (
                    select
                        max(trans_id)
                    from
                        hive_schema_stg.seaware_transaction_evt b
                    where
                        a.res_id        =b.res_id
                        and lower(b.trans_type)='pmnt'
                        and lower(trans_status)='ok'
                )
        )
        tran
        on
            tran.res_id = seaware_reservation_dim.res_id
    left join
        (
            select
                res_id
              , revn.guest_id
              , revn.package_id
              , max(revn.currency) booking_currency
              , sum(revn.amount)   item_amount
              , max(revn.net_due)  net_due
              , inv.invoice_item_type
            from
                hive_schema_stg.seaware_revenue_fact revn
                left join
                    hive_schema_stg.seaware_invoice_item_type_lkp inv
                    on
                        revn.invoice_item_type_id = inv.invoice_item_type_id
            where
                revn.snapshot_date =
                (
                    select
                        max(snapshot_date)
                    from
                        hive_schema_stg.seaware_revenue_fact
                )
            group by
                res_id
              , revn.guest_id
              , revn.package_id
              , inv.invoice_item_type
        )
        revenue_fact
        on
            revenue_fact.res_id                = seaware_commission_fact.res_id
            and revenue_fact.guest_id          = seaware_commission_fact.guest_id
            and revenue_fact.package_id        = seaware_commission_fact.package_id
            and revenue_fact.invoice_item_type = seaware_invoice_item_type_lkp.invoice_item_type;
     
END;

 

           $$
;
