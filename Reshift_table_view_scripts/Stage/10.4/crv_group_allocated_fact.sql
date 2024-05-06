create or replace view seaware.group_allocated_Fact
AS
  SELECT GROUP_ID,
   package_id            ,
   invoice_item_type_id  ,
   price_area_id         ,
   ship_id               ,
   agent_id              ,
   agency_id             ,
   promotion_id          ,
   addon_id              ,
   sail_id               ,
   amount                ,
   currency              ,
   currency_rate         ,
   sum(amount) as allocated_amt 
   FROM seaware.seaware_revenue_fact FACT 
   INNER JOIN seaware.seaware_reservation_dim RD on FACT.RES_ID = RD.RES_ID 
   INNER JOIN seaware.seaware_group_dim GD on GD.SRC_GROUP_ID = RD.SRC_GROUP_ID and GD.rec_end_dttm = '9999-12-31' 
   where snapshot_date = (select max(snapshot_date) from seaware.seaware_revenue_fact)
      group by GROUP_ID,
   package_id            ,
   invoice_item_type_id  ,
   price_area_id         ,
   ship_id               ,
   agent_id              ,
   agency_id             ,
   promotion_id          ,
   addon_id              ,
   sail_id               ,
   amount                ,
   currency              ,
   currency_rate        
with no schema binding;