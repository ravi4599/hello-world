CREATE OR REPLACE PROCEDURE seaware.seaware_revenue_commission_analysis_cube_sp()
	LANGUAGE plpgsql
AS $$
	
	
BEGIN 
drop table if exists seaware.seaware_revenue_commission_analysis_cube_temp;

create table seaware.seaware_revenue_commission_analysis_cube_temp as 
(
			select
				revn.snapshot_date
              , revn.res_id
              , revn.guest_id
			  , revn.package_id
			  , inv.invoice_item_type
			  , inv.invoice_item_subtype
			  , inv.invoice_item_type_id
			  , revn.sail_id
			  , revn.ship_id
			  , revn.addon_id
			  , revn.price_area_id 
			  , revn.currency booking_currency
			  , revn.currency_rate booking_currency_rate 
			  , revn.promotion_id
			  , revn.ship_room_req_id
			  , revn.hotel_res_req_id 
			  , revn.guest_flight_id 
			  , revn.coupon_class_id 	
              , max(revn.agency_id) agency_id --Using agent & agency ids from reservation_dim in main query 
			  , max(revn.agent_id) agent_id --Using agent & agency ids from reservation_dim in main query 
			,sum(revn.amount) amount
 			,sum(revn.voyage_amount) voyage_amount
			,sum(case when invoice_item_type='DISCOUNT' then amount END) as discount  
			,sum(revn.taxesandfees) taxesandfees
			,sum(revn.manual_adj) manual_adj
			,sum(revn.insurance) insurance
			,sum(revn.aon_insurace_payment) aon_insurace_payment
			,sum(revn.voyage_commission_amt) voyage_commission_amt
			,sum(revn.tax_commision_amt) tax_commision_amt
			,sum(revn.voyage_credit_card_fee) voyage_credit_card_fee
			,sum(revn.insurance_credit_card_fee) insurance_credit_card_fee
			,sum(revn.taxesandfees_credit_card_fee) taxesandfees_credit_card_fee
			--,sum(revn.actual_gross_ticket_revenue) actual_gross_ticket_revenue
			,(sum(revn.actual_gross_ticket_revenue) + sum(case when invoice_item_type='DISCOUNT' then amount else 0 END))  actual_gross_ticket_revenue
			,sum(revn.actual_gross_other_revenue) actual_gross_other_revenue
			,sum(revn.actual_gross_onboard_revenue) actual_gross_onboard_revenue
			,sum(revn.actual_gross_total_revenue) actual_gross_total_revenue
			,max(net_due) net_due
			,sum(revn.amount_usd) amount_usd
			,sum(revn.voyage_amount_usd) voyage_amount_usd
			,sum(case when invoice_item_type='DISCOUNT' then amount_usd END) as discount_usd   
			,sum(revn.taxesandfees_usd) taxesandfees_usd
			,sum(revn.manual_adj_usd) manual_adj_usd
			,sum(revn.insurance_usd) insurance_usd
			,sum(revn.aon_insurace_payment_usd) aon_insurace_payment_usd
			,sum(revn.voyage_commission_amt_usd) voyage_commission_amt_usd
			,sum(revn.tax_commision_amt_usd) tax_commision_amt_usd
			,sum(revn.voyage_credit_card_fee_usd) voyage_credit_card_fee_usd
			,sum(revn.insurance_credit_card_fee_usd) insurance_credit_card_fee_usd
			,sum(revn.taxesandfees_credit_card_fee_usd) taxesandfees_credit_card_fee_usd
			--,sum(revn.actual_gross_ticket_revenue_usd) actual_gross_ticket_revenue_usd 
			,(sum(revn.actual_gross_ticket_revenue_usd) + sum(case when invoice_item_type='DISCOUNT' then amount_usd else 0 END)) actual_gross_ticket_revenue_usd
			,sum(revn.actual_gross_other_revenue_usd) actual_gross_other_revenue_usd
			,sum(revn.actual_gross_onboard_revenue_usd) actual_gross_onboard_revenue_usd
			,sum(revn.actual_gross_total_revenue_usd) actual_gross_total_revenue_usd
			,sum(case when invoice_item_type='HOTEL FARE' then amount END) as hotel_fare
			,sum(case when invoice_item_type='HOTEL FARE' then amount END)*revn.currency_rate as hotel_fare_usd
			,nvl(hotel_fare     *0.025, 0)                         AS hotel_credit_card_fee
			,nvl(hotel_fare_usd     *0.025, 0)                         AS hotel_credit_card_fee_usd 
			,sum(case when invoice_item_type='SHORE THINGS' then amount END) as shorethings_fare
			,sum(case when invoice_item_type='SHORE THINGS' then amount END)*revn.currency_rate as shorethings_fare_usd 
			,max(sd.sail_days) PCDs
			,max(sd.sail_days) sail_days
			--,sum(revn.actual_gross_ticket_revenue)/max(sd.sail_days) actual_ticket_gpd
			,(sum(revn.actual_gross_ticket_revenue) + sum(case when invoice_item_type='DISCOUNT' then amount else 0 END))/max(sd.sail_days) actual_ticket_gpd 
			--,(sum(revn.actual_gross_ticket_revenue)/max(sd.sail_days))*revn.currency_rate as actual_ticket_gpd_usd
			,((sum(revn.actual_gross_ticket_revenue) + sum(case when invoice_item_type='DISCOUNT' then amount else 0 END))/max(sd.sail_days))*revn.currency_rate as actual_ticket_gpd_usd
			,sum(revn.actual_gross_onboard_revenue)/max(sd.sail_days) actual_onboard_gpd 
			,(sum(revn.actual_gross_onboard_revenue)/max(sd.sail_days))*revn.currency_rate as actual_onboard_gpd_usd 
			,sum(revn.actual_gross_other_revenue)/max(sd.sail_days) actual_other_gpd 
			,(sum(revn.actual_gross_other_revenue)/max(sd.sail_days))*revn.currency_rate as actual_other_gpd_usd 
			,sum(revn.actual_gross_total_revenue)/max(sd.sail_days) actual_total_gpd
			,(sum(revn.actual_gross_total_revenue)/max(sd.sail_days))*revn.currency_rate as actual_total_gpd_usd
			,2770 * max(sd.sail_days) as APCDs
			,sum(sd.sail_days) as total_sail_days 
			--,sum(sd.sail_days)/(2770 * max(sd.sail_days)) as load_factor 
			, -1 as charge_id, null as charge_component_code, null as charge_state, null as action_type, null as charge_explanation, 
null as charge_change_area, '1900-01-01'::date as charge_action_timestamp, null as charge_is_added_manually, null as charge_is_paid, null::timestamp as charge_paid_timestamp, null as charge_comments, -1 as charge_src_record_id,
sum(0) as charge_amount,
1 as row_num 
            from
                seaware.seaware_revenue_fact revn
                left join
                    hive_schema_stg.seaware_invoice_item_type_lkp inv
                    on
                        revn.invoice_item_type_id = inv.invoice_item_type_id
				left join 
					hive_schema_stg.seaware_sail_dim sd	
					on sd.sail_id = revn.sail_id 
            where
                revn.snapshot_date =
                (
                    select
                        max(snapshot_date)
                    from
                        hive_schema_stg.seaware_revenue_fact
                )
            group by
				revn.snapshot_date
              , revn.res_id
              , revn.guest_id
			  , revn.package_id
              , inv.invoice_item_type
			  , inv.invoice_item_subtype
			  , inv.invoice_item_type_id
			  , revn.sail_id
			  , revn.ship_id 
			  , revn.price_area_id 
			  , revn.addon_id
			  , revn.ship_room_req_id
			  , revn.hotel_res_req_id 
			  , revn.guest_flight_id 
			  , revn.coupon_class_id 
			  , booking_currency
			  , booking_currency_rate 
			  , promotion_id
			  , sail_days
			  , charge_id
			  , charge_component_code
			  , charge_state
			  , charge_explanation 
			  , charge_change_area
			  , charge_action_timestamp
			  , charge_is_added_manually
			  , charge_is_paid
			  , charge_paid_timestamp 
			  , charge_comments
			  , charge_src_record_id 		  
UNION 
            select
			  null as snapshot_date
              , rd_curr.res_id
              , case when rel.guest_id is null then rel_seqn1.guest_id else rel.guest_id end as guest_id  
			  , case when pd.package_id is null then pd_voy.package_id else pd.package_id end as package_id 
			  , null as invoice_item_type
			  , null as invoice_item_subtype
			  , -1 invoice_item_type_id
			  , sd.sail_id
			  , sh.ship_id
			  , -1 addon_id
			  , -1 price_area_id 
			  , null as booking_currency
			  , null as booking_currency_rate 
			  , -1 as promotion_id
			  , -1 as ship_room_req_id
			  , -1 as hotel_res_req_id 	
			  , -1 as guest_flight_id 
			  , -1 as coupon_class_id 
              , ad.agency_id --Using agent & agency ids from reservation_dim 
			  , agt.agent_id --Using agent & agency ids from reservation_dim 
			,0 amount
 			,0 voyage_amount
			,0 discount 
			,0 taxesandfees
			,0 manual_adj
			,0 insurance
			,0 aon_insurace_payment
			,0 voyage_commission_amt
			,0 tax_commision_amt
			,0 voyage_credit_card_fee
			,0 insurance_credit_card_fee
			,0 taxesandfees_credit_card_fee
			,0 actual_gross_ticket_revenue
			,0 actual_gross_other_revenue
			,0 actual_gross_onboard_revenue
			,0 actual_gross_total_revenue
			,0 net_due
			,0 amount_usd
			,0 voyage_amount_usd
			,0 discount_usd 
			,0 taxesandfees_usd
			,0 manual_adj_usd
			,0 insurance_usd
			,0 aon_insurace_payment_usd
			,0 voyage_commission_amt_usd
			,0 tax_commision_amt_usd
			,0 voyage_credit_card_fee_usd
			,0 insurance_credit_card_fee_usd
			,0 taxesandfees_credit_card_fee_usd
			,0 actual_gross_ticket_revenue_usd
			,0 actual_gross_other_revenue_usd
			,0 actual_gross_onboard_revenue_usd
			,0 actual_gross_total_revenue_usd
			,0 as hotel_fare
			,0 as hotel_fare_usd
			,0 as hotel_credit_card_fee
			,0 AS hotel_credit_card_fee_usd 
			,0 as shorethings_fare
			,0 as shorethings_fare_usd 
			,0 as PCDs
			,0 as sail_days
			,0 as actual_ticket_gpd
			,0 as actual_ticket_gpd_usd
			,0 as actual_onboard_gpd 
			,0 as actual_onboard_gpd_usd 
			,0 as actual_other_gpd 
			,0 as actual_other_gpd_usd 
			,0 as actual_total_gpd
			,0 as actual_total_gpd_usd
			,0 as APCDs
			,0 as total_sail_days
			, c.charge_id, c.charge_component_code, c.charge_state, c.action_type, c.explanation as charge_explanation, 
c.change_area as charge_change_area, c.action_timestamp as charge_action_timestamp, c.is_added_manually as charge_is_added_manually, c.is_paid as charge_is_paid, c.paid_timestamp as charge_paid_timestamp, c.comments as charge_comments,c.src_record_id as charge_src_record_id,
c.amount as charge_amount,
row_number() over (partition by c.res_id,case when rel.guest_id is null then rel_seqn1.guest_id else rel.guest_id end,case when pd.package_id is null then pd_voy.package_id else pd.package_id end,c.charge_id order by c.src_record_id asc) as row_num     	
from seaware.seaware_charge_evt c 
left join hive_schema_stg.seaware_reservation_dim rd on c.res_id=rd.res_id 
left join hive_schema_stg.seaware_reservation_dim rd_curr on rd.src_res_id=rd_curr.src_res_id and rd_curr.rec_end_dttm='9999-12-31' 
--left join hive_schema_stg.seaware_res_guest_rel rel on rd_curr.res_id=rel.res_id and c.guest_seqn = rel.guest_seqn 
left join (select res_id, guest_id, guest_seqn from hive_schema_stg.seaware_res_guest_rel union select distinct res_id, gd.guest_id, guest_seqn from hive_schema_stg.seaware_revenue_fact f 
join hive_schema_stg.seaware_guest_dim gd on f.guest_id = gd.guest_id 
where snapshot_date = (select max(snapshot_date) from hive_schema_stg.seaware_revenue_fact)) rel on rd_curr.res_id=rel.res_id and c.guest_seqn = rel.guest_seqn 
left join hive_schema_stg.seaware_package_dim pd on c.src_package_id=pd.src_package_id and pd.rec_end_dttm='9999-12-31' 
left join hive_schema_stg.seaware_sail_dim sd on rd.src_sail_id = sd.src_sail_id and sd.rec_end_dttm ='9999-12-31' 
left join hive_schema_stg.seaware_ship_dim sh on sd.ship_id = sh.ship_id 
left join hive_schema_stg.seaware_package_dim pd_voy on rd_curr.src_sail_id = pd_voy.src_sail_id and pd_voy.package_class='VOYAGE'	and pd_voy.rec_end_dttm = '9999-12-31'
left join hive_schema_stg.seaware_res_guest_rel rel_seqn1 on rel_seqn1.res_id = rd_curr.res_id and rel_seqn1.guest_seqn =1 
--left join hive_schema_stg.crm_agency_master agc on rd_curr.seaware_agency_id = agc.seaware_agency_id__c and agc.rec_end_dttm ='9999-12-31' 
left join hive_schema_stg.seaware_agency_dim ad on ad.src_agency_id = rd_curr.seaware_agency_id and ad.rec_end_dttm = '9999-12-31' 
left join (select am1.* from hive_schema_stg.crm_agent_master am1 left JOIN hive_schema_stg.hvtb_nbx_core_crm_contact cc ON cc.id = am1.id where am1.rec_end_dttm like '9999%' and cc.isdeleted = false) agt on rd_curr.seaware_agent_id = agt.seaware_agent_id__c and agt.rec_end_dttm = '9999-12-31' 
where rd_curr.res_id is not null and (rel.guest_id is not null or rel_seqn1.guest_id is not null) 
and c.rec_end_dttm = '9999-12-31' 
);

drop table if exists seaware.seaware_revenue_commission_analysis_cube_swap;

--delete from seaware.seaware_revenue_commission_analysis_cube_rpt;

create table seaware.seaware_revenue_commission_analysis_cube_swap as 
--insert into seaware.seaware_revenue_commission_analysis_cube_rpt 
with s2 as
    (select max(snapshot_date) as max_snap_date from hive_schema_stg.seaware_commission_fact),
	s1 as 
	(select max(snapshot_date) as max_snap_date from hive_schema_stg.seaware_revenue_fact) 	
select main.*,
/*(base_commission_voyagefare + bonus_commission) as voyagefare_commission,
(base_commission_voyagefare_usd + bonus_commission_usd) as voyagefare_commission_usd, 
(base_commission_voyagefare + bonus_commission + base_commission_taxesandfees) as protected_commission,
(base_commission_voyagefare_usd + bonus_commission_usd + base_commission_taxesandfees_usd) as protected_commission_usd,
(base_commission_voyagefare + base_commission_taxesandfees) as base_protected_commission,
(base_commission_voyagefare_usd + base_commission_taxesandfees_usd) as base_protected_commission_usd,
(base_commission_voyagefare + bonus_commission + (base_commission_voyagefare + bonus_commission + base_commission_taxesandfees) + base_commission_taxesandfees + red_hot_bonus_commission_voyage_fare) as ticket_commissions,
(base_commission_voyagefare_usd + bonus_commission_usd + (base_commission_voyagefare_usd + bonus_commission_usd + base_commission_taxesandfees_usd) + base_commission_taxesandfees_usd + red_hot_bonus_commission_voyage_fare_usd) as ticket_commissions_usd,
	(base_commission_shorex + base_commission_sailorloot) as Commissions_for_onboard_items,
	(base_commission_shorex_usd + base_commission_sailorloot_usd) as Commissions_for_onboard_items_usd,
	(base_commission_insurance + base_commission_hotelfare) as Commissions_for_other_revenue_items,
	(base_commission_insurance_usd + base_commission_hotelfare_usd) as Commissions_for_other_revenue_items_usd,*/--MSH-68908 Removing commission related KPIs
	(voyage_credit_card_fee + insurance_credit_card_fee + taxesandfees_credit_card_fee + hotel_credit_card_fee) as credit_card_fees,
	(voyage_credit_card_fee_usd + insurance_credit_card_fee_usd + taxesandfees_credit_card_fee_usd + hotel_credit_card_fee_usd) as credit_card_fees_usd,
	(insurance_credit_card_fee + hotel_credit_card_fee) as credit_card_fees_Other_revenue_items,
	(insurance_credit_card_fee_usd + hotel_credit_card_fee_usd) as credit_card_fees_Other_revenue_items_usd,
	/*(actual_gross_ticket_revenue - (base_commission_voyagefare + bonus_commission + (base_commission_voyagefare + bonus_commission + base_commission_taxesandfees) + base_commission_taxesandfees + red_hot_bonus_commission_voyage_fare) - (voyage_credit_card_fee + insurance_credit_card_fee + taxesandfees_credit_card_fee + hotel_credit_card_fee)) as net_ticket_revenue,
	(actual_gross_ticket_revenue_usd - (base_commission_voyagefare_usd + bonus_commission_usd + (base_commission_voyagefare_usd + bonus_commission_usd + base_commission_taxesandfees_usd) + base_commission_taxesandfees_usd + red_hot_bonus_commission_voyage_fare_usd) - (voyage_credit_card_fee_usd + insurance_credit_card_fee_usd + taxesandfees_credit_card_fee_usd + hotel_credit_card_fee_usd)) as net_ticket_revenue_usd,*/--MSH-68908 Removing commission related KPIs
	CURRENT_TIMESTAMP as etl_ld_dt 
from 	
(
	select
    revenue_comm_etc.invoice_item_type as  invoice_item_type,
	revenue_comm_etc.invoice_item_subtype,
	revenue_comm_etc.booking_currency,
	revenue_comm_etc.booking_currency_rate,
	revenue_comm_etc.amount,
	revenue_comm_etc.voyage_amount as voyage_fare_amount_excl_discounts,
	revenue_comm_etc.discount,
	nvl(revenue_comm_etc.voyage_amount,0) + nvl(revenue_comm_etc.discount,0) as total_voyage_fare_amount_incl_discounts, 
	revenue_comm_etc.taxesandfees,
	revenue_comm_etc.manual_adj,
	revenue_comm_etc.insurance,
	revenue_comm_etc.aon_insurace_payment,
	revenue_comm_etc.voyage_commission_amt,
	revenue_comm_etc.tax_commision_amt,
	revenue_comm_etc.voyage_credit_card_fee,
	revenue_comm_etc.insurance_credit_card_fee,
	revenue_comm_etc.taxesandfees_credit_card_fee,
	revenue_comm_etc.actual_gross_ticket_revenue,
	revenue_comm_etc.actual_gross_other_revenue,
	revenue_comm_etc.actual_gross_onboard_revenue,
	revenue_comm_etc.actual_gross_total_revenue,
	/*case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and seaware_guest_dim.guest_seqn=1 then revenue_comm_etc.net_due else 0 end as net_due,*/
	revenue_comm_etc.net_due,
	revenue_comm_etc.amount_usd,
	revenue_comm_etc.voyage_amount_usd as voyage_fare_amount_excl_discounts_usd,
	revenue_comm_etc.discount_usd,
	nvl(revenue_comm_etc.voyage_amount_usd,0) + nvl(revenue_comm_etc.discount_usd,0) as total_voyage_fare_amount_incl_discounts_usd,
	revenue_comm_etc.taxesandfees_usd,
	revenue_comm_etc.manual_adj_usd,
	revenue_comm_etc.insurance_usd,
	revenue_comm_etc.aon_insurace_payment_usd,
	revenue_comm_etc.voyage_commission_amt_usd,
	revenue_comm_etc.tax_commision_amt_usd,
	revenue_comm_etc.voyage_credit_card_fee_usd,
	revenue_comm_etc.insurance_credit_card_fee_usd,
	revenue_comm_etc.taxesandfees_credit_card_fee_usd,
	revenue_comm_etc.actual_gross_ticket_revenue_usd,
	revenue_comm_etc.actual_gross_other_revenue_usd,
	revenue_comm_etc.actual_gross_onboard_revenue_usd,
	revenue_comm_etc.actual_gross_total_revenue_usd,
	revenue_comm_etc.hotel_fare,
	revenue_comm_etc.hotel_fare_usd,
	revenue_comm_etc.hotel_credit_card_fee,
	revenue_comm_etc.hotel_credit_card_fee_usd, 
	revenue_comm_etc.shorethings_fare,
	revenue_comm_etc.shorethings_fare_usd, 
	revenue_comm_etc.PCDs,
	revenue_comm_etc.actual_ticket_gpd,
	revenue_comm_etc.actual_ticket_gpd_usd,
	revenue_comm_etc.actual_onboard_gpd,
	revenue_comm_etc.actual_onboard_gpd_usd, 
	revenue_comm_etc.actual_other_gpd,
	revenue_comm_etc.actual_other_gpd_usd, 
	revenue_comm_etc.actual_total_gpd,
	revenue_comm_etc.actual_total_gpd_usd,
	revenue_comm_etc.APCDs,
	revenue_comm_etc.total_sail_days,
	revenue_comm_etc.load_factor,
	seaware_price_area_lkp.price_area_id,
	seaware_price_area_lkp.price_area,
	revenue_comm_etc.currency_code as commission_currency_code,
	revenue_comm_etc.currency_rate as commission_currency_rate,
	--seaware_commission_fact.invoice_item_type as invoice_item_type_commission, 
	revenue_comm_etc.commission_percent,
	/*case when (revenue_comm_etc.amount is not null and revenue_comm_etc.amount<>0) then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end as commission_amount,*/ 
	/*case when (((lead(revenue_comm_etc.comm_row_num,1) over (partition by revenue_comm_etc.res_id,revenue_comm_etc.guest_id, revenue_comm_etc.package_id, revenue_comm_etc.invoice_item_type, revenue_comm_etc.charge_id, revenue_comm_etc.addon_id order by revenue_comm_etc.comm_row_num)>1) or revenue_comm_etc.comm_row_num>1) and (revenue_comm_etc.invoice_item_type <> 'VOYAGE FARE' and  revenue_comm_etc.commission_code <> 'BONUS'))  
	then revenue_comm_etc.commission_amount 
	else
		case when (revenue_comm_etc.amount is not null and revenue_comm_etc.amount<>0) then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end 
	end as commission_amount,*/--V13 Display commission amount from commission fact and not as invoice amount percentage. 
	/*Display split of commission amount where there are multiple commission records for given invoice entry. If there are multiple invoice entries and single commission entry, then show the commission amount as percentage of the revenue amount*/ 	
	revenue_comm_etc.commission_amount,
	/*case when revenue_comm_etc.commission_fare is not null then
	case when (revenue_comm_etc.amount is not null and revenue_comm_etc.amount<>0) then revenue_comm_etc.amount else revenue_comm_etc.commission_fare end end as commission_fare,*/
	/*case when (((lead(revenue_comm_etc.comm_row_num,1) over (partition by revenue_comm_etc.res_id,revenue_comm_etc.guest_id, revenue_comm_etc.package_id, revenue_comm_etc.invoice_item_type, revenue_comm_etc.charge_id, revenue_comm_etc.addon_id order by revenue_comm_etc.comm_row_num)>1) or revenue_comm_etc.comm_row_num>1) and (revenue_comm_etc.invoice_item_type <> 'VOYAGE FARE' and  revenue_comm_etc.commission_code <> 'BONUS'))  
	then revenue_comm_etc.commission_fare  
	else
		case when (revenue_comm_etc.amount is not null) then revenue_comm_etc.amount else revenue_comm_etc.commission_fare end 
	end as commission_fare, */--V13 Display commission amount from commission fact and not as invoice amount percentage. 
	revenue_comm_etc.commission_fare,
	revenue_comm_etc.commission_payout_date,
	case
        when (
                revenue_comm_etc.net_due=0
                and datediff(day,revenue_comm_etc.snapshot_date,seaware_sail_dim.sail_date_from)<=120
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
    end expected_payout_date,
/*revenue_comm_etc.base_commission_voyagefare_regular,
revenue_comm_etc.base_commission_voyagefare_protected,
revenue_comm_etc.base_commission_voyagefare_manual,
(revenue_comm_etc.base_commission_voyagefare_regular + revenue_comm_etc.base_commission_voyagefare_protected + revenue_comm_etc.base_commission_voyagefare_manual) as base_commission_voyagefare,
revenue_comm_etc.base_commission_taxesandfees_regular,
revenue_comm_etc.base_commission_taxesandfees_protected,
revenue_comm_etc.base_commission_taxesandfees_manual,
(revenue_comm_etc.base_commission_taxesandfees_regular + revenue_comm_etc.base_commission_taxesandfees_protected + revenue_comm_etc.base_commission_taxesandfees_manual) as base_commission_taxesandfees,
revenue_comm_etc.base_commission_shorex_regular,
revenue_comm_etc.base_commission_shorex_protected,
revenue_comm_etc.base_commission_shorex_manual,
(revenue_comm_etc.base_commission_shorex_regular + revenue_comm_etc.base_commission_shorex_protected + revenue_comm_etc.base_commission_shorex_manual) as base_commission_shorex,
revenue_comm_etc.base_commission_sailorloot_regular,
revenue_comm_etc.base_commission_sailorloot_protected,
revenue_comm_etc.base_commission_sailorloot_manual,
(revenue_comm_etc.base_commission_sailorloot_regular + revenue_comm_etc.base_commission_sailorloot_protected + revenue_comm_etc.base_commission_sailorloot_manual) as base_commission_sailorloot,
revenue_comm_etc.base_commission_hotelfare_regular,
revenue_comm_etc.base_commission_hotelfare_protected,
revenue_comm_etc.base_commission_hotelfare_manual,
(revenue_comm_etc.base_commission_hotelfare_regular + revenue_comm_etc.base_commission_hotelfare_protected + revenue_comm_etc.base_commission_hotelfare_manual) as base_commission_hotelfare,
revenue_comm_etc.base_commission_insurance_regular,
revenue_comm_etc.base_commission_insurance_protected,
(revenue_comm_etc.base_commission_insurance_regular + revenue_comm_etc.base_commission_insurance_protected) as base_commission_insurance,
revenue_comm_etc.bonus_commission_voyagefare_regular,
revenue_comm_etc.bonus_commission_voyagefare_protected,
revenue_comm_etc.bonus_commission_voyagefare_manual,
(revenue_comm_etc.bonus_commission_voyagefare_regular + revenue_comm_etc.bonus_commission_voyagefare_protected + revenue_comm_etc.bonus_commission_voyagefare_manual) as bonus_commission,
--revenue_comm_etc.voyagefare_commission,
revenue_comm_etc.gsa_commission_voyagefare,
revenue_comm_etc.gsa_commission_taxesandfees,
revenue_comm_etc.gsa_commission_insurance,
revenue_comm_etc.gsa_commission_shorex,
revenue_comm_etc.gsa_commission_sailorloot,
--revenue_comm_etc.protected_commission,
--revenue_comm_etc.base_protected_commission,
revenue_comm_etc.base_commission_other_bookable,
revenue_comm_etc.base_commission_voyagefare_regular_usd,
revenue_comm_etc.base_commission_voyagefare_protected_usd,
revenue_comm_etc.base_commission_voyagefare_manual_usd,
(revenue_comm_etc.base_commission_voyagefare_regular_usd + revenue_comm_etc.base_commission_voyagefare_protected_usd + revenue_comm_etc.base_commission_voyagefare_manual_usd) as base_commission_voyagefare_usd,
revenue_comm_etc.base_commission_taxesandfees_regular_usd,
revenue_comm_etc.base_commission_taxesandfees_protected_usd,
revenue_comm_etc.base_commission_taxesandfees_manual_usd,
(revenue_comm_etc.base_commission_taxesandfees_regular_usd + revenue_comm_etc.base_commission_taxesandfees_protected_usd + revenue_comm_etc.base_commission_taxesandfees_manual_usd) as base_commission_taxesandfees_usd,
revenue_comm_etc.base_commission_shorex_regular_usd,
revenue_comm_etc.base_commission_shorex_protected_usd,
revenue_comm_etc.base_commission_shorex_manual_usd,
(revenue_comm_etc.base_commission_shorex_regular_usd + revenue_comm_etc.base_commission_shorex_protected_usd + revenue_comm_etc.base_commission_shorex_manual_usd) as base_commission_shorex_usd,
revenue_comm_etc.base_commission_sailorloot_regular_usd,
revenue_comm_etc.base_commission_sailorloot_protected_usd,
revenue_comm_etc.base_commission_sailorloot_manual_usd,
(revenue_comm_etc.base_commission_sailorloot_regular_usd + revenue_comm_etc.base_commission_sailorloot_protected_usd + revenue_comm_etc.base_commission_sailorloot_manual_usd) as base_commission_sailorloot_usd,
revenue_comm_etc.base_commission_hotelfare_regular_usd,
revenue_comm_etc.base_commission_hotelfare_protected_usd,
revenue_comm_etc.base_commission_hotelfare_manual_usd,
(revenue_comm_etc.base_commission_hotelfare_regular_usd + revenue_comm_etc.base_commission_hotelfare_protected_usd + revenue_comm_etc.base_commission_hotelfare_manual_usd) as base_commission_hotelfare_usd,
revenue_comm_etc.base_commission_insurance_regular_usd,
revenue_comm_etc.base_commission_insurance_protected_usd,
(revenue_comm_etc.base_commission_insurance_regular_usd + revenue_comm_etc.base_commission_insurance_protected_usd) as base_commission_insurance_usd,
revenue_comm_etc.bonus_commission_voyagefare_regular_usd,
revenue_comm_etc.bonus_commission_voyagefare_protected_usd,
revenue_comm_etc.bonus_commission_voyagefare_manual_usd,
(revenue_comm_etc.bonus_commission_voyagefare_regular_usd + revenue_comm_etc.bonus_commission_voyagefare_protected_usd + revenue_comm_etc.bonus_commission_voyagefare_manual_usd) as bonus_commission_usd,
--revenue_comm_etc.voyagefare_commission_usd,
revenue_comm_etc.gsa_commission_voyagefare_usd,
revenue_comm_etc.gsa_commission_taxesandfees_usd,
revenue_comm_etc.gsa_commission_insurance_usd,
revenue_comm_etc.gsa_commission_shorex_usd,
revenue_comm_etc.gsa_commission_sailorloot_usd,
--revenue_comm_etc.protected_commission_usd,
--revenue_comm_etc.base_protected_commission_usd,
revenue_comm_etc.base_commission_other_bookable_usd,
	(case when seaware_reservation_dim.SUB_SALES_CHANNEL = 'FIRST MATES' then revenue_comm_etc.voyage_amount else 0 end) * 0.0425 as red_hot_bonus_commission_voyage_fare,
	(case when seaware_reservation_dim.SUB_SALES_CHANNEL = 'FIRST MATES' then revenue_comm_etc.voyage_amount_usd else 0 end) * 0.0425 as red_hot_bonus_commission_voyage_fare_usd,*/--MSH-68908 Removing commission related KPIs
	0 as net_onboard_revenue, -- awaiting clarification on formula
	0 as net_other_revenue, -- awaiting clarification on formula
	0 as net_net_ticket_revenue, -- awaiting clarification on formula
	revenue_comm_etc.commission_type_id,
	revenue_comm_etc.commission_code,
	revenue_comm_etc.commission_source,
	revenue_comm_etc.is_cancelled_comm,
	revenue_comm_etc.charge_id,
	revenue_comm_etc.charge_component_code,
	revenue_comm_etc.charge_state,
	revenue_comm_etc.action_type,
	revenue_comm_etc.charge_explanation, 
	revenue_comm_etc.charge_change_area,
	revenue_comm_etc.charge_action_timestamp,
	revenue_comm_etc.charge_is_added_manually,
	revenue_comm_etc.charge_is_paid,
	revenue_comm_etc.charge_paid_timestamp,
	revenue_comm_etc.charge_comments,
	revenue_comm_etc.charge_src_record_id,
	--revenue_comm_etc.charge_amount,	
	case when revenue_comm_etc.commission_source = 'PROTECTED' and  revenue_comm_etc.commission_code in ('SECONDARY','BONUS') then 0 else revenue_comm_etc.charge_amount end as charge_amount,--23/12/2020 Updated during unit testing as charge amount was duplicating for multiple commission records for same charge id. Charge amount will be shown only for 'PROTECTED' and 'STANDARD' commission record.
	geo_analysis.source geo_analysis_source,
	geo_analysis.country,
	geo_analysis.state, 
    geo_analysis.zip, 
    geo_analysis.lat_sailor, 
    geo_analysis.long_sailor, 
    geo_analysis.lat_miami, 
    geo_analysis.long_miami, 
    geo_analysis.distance_miles, 
    geo_analysis.distance_group, 
    geo_analysis.msa, 
    geo_analysis.mapped_msa, 
    geo_analysis.msa_personal_income, 
    geo_analysis.target_population_quantity, 
    geo_analysis.total_population_quantity,
	revenue_comm_etc.is_active_promotion,
	revenue_comm_etc.is_excluded_promotion,
	revenue_comm_etc.promo_mode,
	seaware_promotion_lkp.promotion_id as promotion_id,
	seaware_promotion_lkp.promo_code as promo_code,
	seaware_promotion_lkp.promo_name as promo_name,
	seaware_promotion_lkp.promo_group as promo_group,
	seaware_reservation_dim.res_id as res_id,
	seaware_reservation_dim.src_res_id as src_res_id,
	seaware_reservation_dim.res_init_date as res_init_date,
	seaware_reservation_dim.res_guest_count as res_guest_count,
	seaware_reservation_dim.res_mode as res_mode,
	seaware_reservation_dim.cancellation_date as res_cancellation_date,
	seaware_reservation_dim.bk_date as res_bk_date,
	seaware_reservation_dim.res_status as res_status,
	seaware_reservation_dim.operator as res_operator,
	seaware_reservation_dim.res_type as res_type,
	seaware_reservation_dim.is_internal as res_is_internal,
	seaware_reservation_dim.source_code as res_source_code,
	seaware_reservation_dim.effective_date as res_effective_date,
	seaware_reservation_dim.booking_source as res_booking_source,
	seaware_reservation_dim.sales_channel as res_sales_channel,
	seaware_reservation_dim.sub_sales_channel as res_sub_sales_channel,
	seaware_reservation_dim.sub_sub_sales_channel as res_sub_sub_sales_channel,
	seaware_reservation_dim.vip_status as vip_status,
	seaware_reservation_dim.stage as res_stage,
	seaware_reservation_dim.booking_source_erp as booking_source_erp,
	seaware_reservation_dim.last_modifiedby_id as res_last_modifiedby_id,
	seaware_reservation_dim.hotel_flag as res_hotel_flag,
	seaware_reservation_dim.opted_fvc as res_opted_fvc,
	seaware_reservation_dim.currency as reservation_currency,
	seaware_reservation_dim.currency_rate as reservation_currency_rate,
	seaware_reservation_dim.sec_agency_id as sec_agency_id,
	seaware_reservation_dim.sec_agent_id as sec_agent_id,
	--seaware_reservation_dim.cancellation_case as res_cancellation_case,
	crm_opportunity.access_key__c as opportunity_access_key,
	crm_opportunity.promo_code__c as opportunity_promo_code,
	seaware_guest_dim.guest_id as guest_id,
	seaware_guest_dim.src_guest_id as src_guest_id,
	seaware_guest_dim.guest_seqn as guest_seqn,
	seaware_guest_dim.guest_type as guest_type,
	seaware_guest_dim.client_id as client_id,
	seaware_guest_dim.age as guest_age,
	seaware_guest_dim.age_category as guest_age_category,
	seaware_guest_dim.gender as guest_gender,
	seaware_guest_dim.address_line1 as guest_address_line1,
	seaware_guest_dim.address_line2 as guest_address_line2,
	seaware_guest_dim.city as guest_city,
	seaware_guest_dim.country as guest_country,
	seaware_guest_dim.zip as guest_zip,
	seaware_guest_dim.state as guest_state,
	seaware_guest_dim.citizenship as guest_citizenship,
	seaware_guest_dim.citizenship_name as guest_citizenship_name,
	seaware_guest_dim.last_name as guest_last_name,
	seaware_guest_dim.first_name as guest_first_name,
	seaware_guest_dim.sex as guest_sex,
	seaware_guest_dim.tier_level as guest_tier_level,
	seaware_guest_dim.birthday as guest_birthday,
	seaware_guest_dim.email as guest_email,
	seaware_guest_dim.phone_intl_code as guest_phone_intl_code,
	seaware_guest_dim.phone_number as guest_phone_number,
	seaware_group_dim.group_id,
	seaware_group_dim.src_group_id,
	seaware_group_dim.group_init_date,
	seaware_group_dim.group_status,
	seaware_group_dim.group_type,
	seaware_group_dim.group_name,
	seaware_group_dim.group_mode,
	seaware_group_dim.is_internal as group_is_internal,
	seaware_group_dim.source_code as group_source_code,
	seaware_group_dim.office_code as group_office_code,
	seaware_group_dim.operator as group_operator,
	seaware_group_dim.n_of_guests as group_n_of_guests, 
	seaware_group_dim.cancellation_date as group_cancellation_date, 
	seaware_group_dim.sales_channel as group_sales_channel,
	seaware_group_dim.sub_sales_channel as group_sub_sales_channel,
	seaware_group_dim.sub_sub_sales_channel as group_sub_sub_sales_channel,
	seaware_booked_cabin_reservation_dim.price_category,
seaware_booked_cabin_reservation_dim.price_category_rank,
seaware_booked_cabin_reservation_dim.berth_category,
seaware_booked_cabin_reservation_dim.berth_category_rank,
seaware_booked_cabin_reservation_dim.assigned_category,
seaware_booked_cabin_reservation_dim.assigned_category_rank,
seaware_booked_cabin_reservation_dim.generic_category,
seaware_booked_cabin_reservation_dim.allocation_id as cabin_allocation_id,
--seaware_booked_cabin_reservation_dim.reserve_type as cabin_reserve_type,--Column removed as part of MSH-52688
seaware_allocation_lkp.allocation_inv_result as cabin_allocation_inv_result, 
seaware_cabin_master.cabin_id,
seaware_cabin_master.cabin_number,
seaware_cabin_master.ship as cabin_ship,
seaware_cabin_master.deck_number as cabin_deck_number,
seaware_cabin_master.cabin_name,
seaware_cabin_master.cabin_rank,
seaware_cabin_master.cabin_category,
seaware_cabin_master.cabin_category_desc,
seaware_cabin_master.cabin_category_rank,
seaware_cabin_master.cabin_category_generic,
seaware_cabin_master.cabin_category_generic_rank,
seaware_cabin_master.cabin_capacity,
seaware_sail_cabin_reserve_dim.reserve_type as cabin_reserve_type,
seaware_sail_cabin_reserve_dim.reserve_comments as cabin_reserve_comments,
seaware_sail_cabin_reserve_dim.is_reserve_allows_booking as cabin_is_reserve_allows_booking,
seaware_sail_cabin_reserve_dim.is_reserve_restricted as cabin_is_reserve_restricted,
seaware_sail_cabin_reserve_dim.is_sail_reserve as is_sail_reserve,
seaware_sail_cabin_reserve_dim.reserve_type_comments as cabin_reserve_type_comments,
revenue_comm_etc.addon_quantity,
/*revenue_comm_etc.addon_effective_date,
revenue_comm_etc.addon_is_default,
revenue_comm_etc.addon_is_auto,
revenue_comm_etc.addon_promotion_id,
revenue_comm_etc.addon_comments,
revenue_comm_etc.addon_status,
revenue_comm_etc.addon_notes,
revenue_comm_etc.addon_paid_date,*/
seaware_addon_lkp.addon_id,
seaware_addon_lkp.res_addon_code,
seaware_addon_lkp.addon_name,
seaware_addon_lkp.addon_category,
seaware_addon_lkp.addon_type,
revenue_comm_etc.src_coupon_id,
revenue_comm_etc.coupon_issue_date,
revenue_comm_etc.coupon_valid_from,
revenue_comm_etc.coupon_valid_to,
revenue_comm_etc.coupon_effective_from,
revenue_comm_etc.coupon_effective_to,
revenue_comm_etc.coupon_category,
revenue_comm_etc.coupon_class_coupon_dim,
revenue_comm_etc.coupon_class_code,
revenue_comm_etc.coupon_class_comments,
revenue_comm_etc.apply_as_payment_coupon_dim,
revenue_comm_etc.apply_as_discount_coupon_dim,
revenue_comm_etc.entity_type_coupon_dim,
revenue_comm_etc.entity_id_coupon_dim,
revenue_comm_etc.coupon_is_used,
revenue_comm_etc.coupon_is_active,
revenue_comm_etc.coupon_currency_code,
revenue_comm_etc.coupon_amount,
revenue_comm_etc.coupon_amount_left,
revenue_comm_etc.coupon_charge_code,
revenue_comm_etc.coupon_reason_code,
revenue_comm_etc.coupon_comments,
seaware_sail_dim.sail_id,
seaware_sail_dim.src_sail_id,
seaware_sail_dim.sail_days,
seaware_sail_dim.sail_route_code,
seaware_sail_dim.sail_port_from,
seaware_sail_dim.sail_port_to,
seaware_sail_dim.sail_geog_area_code,
seaware_sail_dim.sail_date_from,
seaware_sail_dim.sail_date_to,
seaware_ship_dim.ship_id,
seaware_ship_dim.ship,
seaware_ship_dim.ship_name,
case when seaware_package_dim.package_id is null then p_voy.package_id else seaware_package_dim.package_id end as package_id,
case when seaware_package_dim.package_id is null then p_voy.src_package_id else seaware_package_dim.src_package_id end as src_package_id,
case when seaware_package_dim.package_id is null then p_voy.package_class else seaware_package_dim.package_class end as package_class,
case when seaware_package_dim.package_id is null then p_voy.product_type else seaware_package_dim.product_type end as product_type,
case when seaware_package_dim.package_id is null then p_voy.package_type else seaware_package_dim.package_type end as package_type,
case when seaware_package_dim.package_id is null then p_voy.package_code else seaware_package_dim.package_code end as package_code,
case when seaware_package_dim.package_id is null then p_voy.package_name else seaware_package_dim.package_name end as package_name,
case when seaware_package_dim.package_id is null then p_voy.shorex_mode else seaware_package_dim.shorex_mode end as shorex_mode,
case when seaware_package_dim.package_id is null then p_voy.vacation_date else seaware_package_dim.vacation_date end as vacation_date,
case when seaware_package_dim.package_id is null then p_voy.shorex_timing else seaware_package_dim.shorex_timing end as shorex_timing,
case when seaware_package_dim.package_id is null then p_voy.geog_area_code else seaware_package_dim.geog_area_code end as geog_area_code,
case when seaware_package_dim.package_id is null then p_voy.season_code else seaware_package_dim.season_code end as season_code,
case when seaware_package_dim.package_id is null then p_voy.is_active else seaware_package_dim.is_active end as pkg_is_active,
case when seaware_package_dim.package_id is null then p_voy.shorex_total_spots else seaware_package_dim.shorex_total_spots end as shorex_total_spots,
case when seaware_package_dim.package_id is null then p_voy.shorex_min_spots else seaware_package_dim.shorex_min_spots end as shorex_min_spots,
p_voy.package_name as product_name,	
revenue_comm_etc.res_package_effective_date,--MSH-70985
/*Agency details from Seaware-Begin*/
seaware_agency_dim.agency_id seaware_ad_agency_id,
seaware_agency_dim.src_agency_id seaware_ad_src_agency_id,
seaware_agency_dim.agency_name as seaware_ad_agency_name, 
seaware_agency_dim.agency_code as seaware_ad_agency_code,
seaware_agency_dim.agency_sales_district as seaware_ad_agency_sales_district,
seaware_agency_dim.agency_country as seaware_ad_agency_country,
seaware_agency_dim.agency_state as seaware_ad_agency_state,
seaware_agency_dim.agency_zip as seaware_ad_agency_zip,
case when seaware_crm_agency_master.agency_id is null then crm_agency_master.agency_id else seaware_crm_agency_master.agency_id end as seaware_crm_agency_id,
case when seaware_crm_agency_master.seaware_agency_id__c is null then crm_agency_master.seaware_agency_id__c else seaware_crm_agency_master.seaware_agency_id__c end as seaware_agency_id,
/*seaware_crm_agency_master.seaware_parent_agency_id as seaware_parent_agency_id,
seaware_crm_agency_master.seaware_parent_agency_name as seaware_parent_agency_name,
seaware_crm_agency_master.seaware_parent_agency_firstname as seaware_parent_agency_firstname,
seaware_crm_agency_master.seaware_parent_agency_lastname as seaware_parent_agency_lastname,
*/ --Added as part of agency_link change. 
case when seaware_crm_agency_master.name is null then crm_agency_master.name else seaware_crm_agency_master.name end as seaware_crm_agency_name,
case when seaware_crm_agency_master.lastname is null then crm_agency_master.lastname else seaware_crm_agency_master.lastname end as seaware_agency_lastname,
case when seaware_crm_agency_master.firstname is null then crm_agency_master.firstname else seaware_crm_agency_master.firstname end as seaware_agency_firstname,
case when seaware_crm_agency_master.billingcity is null then crm_agency_master.billingcity else seaware_crm_agency_master.billingcity end as seaware_agecny_billingcity,
case when seaware_crm_agency_master.billingcountry is null then crm_agency_master.billingcountry else seaware_crm_agency_master.billingcountry end as seaware_agency_billingcountry,
case when seaware_crm_agency_master.billingpostalcode is null then crm_agency_master.billingpostalcode else seaware_crm_agency_master.billingpostalcode end as seaware_agency_billingpostalcode,
case when seaware_crm_agency_master.billingstate is null then crm_agency_master.billingstate else seaware_crm_agency_master.billingstate end as seaware_agency_billingstate,
case when seaware_crm_agency_master.billingstreet is null then crm_agency_master.billingstreet else seaware_crm_agency_master.billingstreet end as seaware_agency_billingstreet,
case when seaware_crm_agency_master.createddate is null then crm_agency_master.createddate else seaware_crm_agency_master.createddate end as seaware_agecny_createddate,
case when seaware_crm_agency_master.status__c is null then crm_agency_master.status__c else seaware_crm_agency_master.status__c end as seaware_agecny_status, 
case when seaware_crm_agency_master.territorynametext__c is null then crm_agency_master.territorynametext__c else seaware_crm_agency_master.territorynametext__c end as seaware_agency_territorynametext,
case when seaware_crm_agency_master.ownerid is null then crm_agency_master.ownerid else seaware_crm_agency_master.ownerid end as seaware_agecny_ownerid,
case when seaware_crm_agency_master.primary_affiliation__c is null then crm_agency_master.primary_affiliation__c else seaware_crm_agency_master.primary_affiliation__c end as seaware_agency_primary_affiliation,
case when seaware_crm_agency_master.parentid is null then crm_agency_master.parentid else seaware_crm_agency_master.parentid end as seaware_agency_parentid,
case when seaware_crm_agency_master.agency_classification__c is null then crm_agency_master.agency_classification__c else seaware_crm_agency_master.agency_classification__c end as seaware_agency_classification,
case when seaware_crm_agency_master.agency_sub_type__c is null then crm_agency_master.agency_sub_type__c else seaware_crm_agency_master.agency_sub_type__c end as seaware_agency_sub_type,
case when seaware_crm_agency_master.is_touroperator__c is null then crm_agency_master.is_touroperator__c else seaware_crm_agency_master.is_touroperator__c end as seaware_agency_is_touroperator,
case when seaware_crm_agency_master.client_id__c is null then crm_agency_master.client_id__c else seaware_crm_agency_master.client_id__c end as seaware_agency_client_id,
case when seaware_crm_agency_master.createdbyid is null then crm_agency_master.createdbyid else seaware_crm_agency_master.createdbyid end as seaware_agency_createdbyid,
case when seaware_crm_agency_master.lastmodifiedbyid is null then crm_agency_master.lastmodifiedbyid else seaware_crm_agency_master.lastmodifiedbyid end as seaware_agency_lastmodifiedbyid,
case when seaware_crm_agency_master.personhasoptedoutofemail is null then crm_agency_master.personhasoptedoutofemail else seaware_crm_agency_master.personhasoptedoutofemail end as seaware_agency_personhasoptedoutofemail,
case when seaware_crm_agency_master.hand_raiser__c is null then crm_agency_master.hand_raiser__c else seaware_crm_agency_master.hand_raiser__c end as seaware_agency_hand_raiser,
case when seaware_crm_agency_master.vip_tier__c is null then crm_agency_master.vip_tier__c else seaware_crm_agency_master.vip_tier__c end as seaware_agency_vip_tier,
case when seaware_crm_agency_master.webuser__c is null then crm_agency_master.webuser__c else seaware_crm_agency_master.webuser__c end as seaware_agency_webuser,
case when seaware_crm_agency_master.currency_type__c is null then crm_agency_master.currency_type__c else seaware_crm_agency_master.currency_type__c end as seaware_agency_currency_type,
/*Agency details from Seaware-End*/
/*Agency details from CRM-Begin*/
crm_agency_master.agency_id as crm_agency_id,
crm_agency_master.seaware_agency_id__c as crm_seaware_agency_id,
crm_agency_master.seaware_parent_agency_id as crm_seaware_parent_agency_id,
crm_parent_agency.name as crm_seaware_parent_agency_name,
crm_parent_agency.firstname as crm_seaware_parent_agency_firstname,
crm_parent_agency.lastname as crm_seaware_parent_agency_lastname,--Added as part of agency_link change. 
crm_agency_master.name as crm_agency_name,
crm_agency_master.lastname as crm_agency_lastname,
crm_agency_master.firstname as crm_agency_firstname,
crm_agency_master.billingcity as crm_agecny_billingcity,
crm_agency_master.billingcountry as crm_agency_billingcountry,
crm_agency_master.billingpostalcode as crm_agency_billingpostalcode,
crm_agency_master.billingstate as crm_agency_billingstate,
crm_agency_master.billingstreet as crm_agency_billingstreet,
crm_agency_master.createddate as crm_agecny_createddate,
crm_agency_master.status__c as crm_agecny_status, 
crm_agency_master.territorynametext__c as crm_agency_territorynametext,
crm_agency_master.ownerid as crm_agecny_ownerid,
crm_agency_master.primary_affiliation__c as crm_agency_primary_affiliation,
crm_agency_master.parentid as crm_agency_parentid,
crm_agency_master.agency_classification__c as crm_agency_classification,
crm_agency_master.agency_sub_type__c as crm_agency_sub_type,
crm_agency_master.is_touroperator__c as crm_agency_is_touroperator,
crm_agency_master.client_id__c as crm_agency_client_id,
crm_agency_master.createdbyid as crm_agency_createdbyid,
crm_agency_master.lastmodifiedbyid as crm_agency_lastmodifiedbyid,
crm_agency_master.personhasoptedoutofemail as crm_agency_personhasoptedoutofemail,
crm_agency_master.hand_raiser__c as crm_agency_hand_raiser,
crm_agency_master.vip_tier__c as crm_agency_vip_tier,
crm_agency_master.webuser__c as crm_agency_webuser,
crm_agency_master.currency_type__c as crm_agency_currency_type,
/*Agency details from CRM-End*/
/*Agent details from Seaware-Begin*/
case when seaware_crm_agent_master.agent_id is null then crm_agent_master.agent_id else seaware_crm_agent_master.agent_id end as seaware_agent_id,
case when seaware_crm_agent_master.seaware_agent_id__c is null then crm_agent_master.seaware_agent_id__c else seaware_crm_agent_master.seaware_agent_id__c end as seaware_src_agent_id,
case when seaware_crm_agent_master.name is null then crm_agent_master.name else seaware_crm_agent_master.name end as seaware_agent_name,
case when seaware_crm_agent_master.lastname is null then crm_agent_master.lastname else seaware_crm_agent_master.lastname end as seaware_agent_lastname,
case when seaware_crm_agent_master.firstname is null then crm_agent_master.firstname else seaware_crm_agent_master.firstname end as seaware_agent_firstname,
case when seaware_crm_agent_master.mailingcity is null then crm_agent_master.mailingcity else seaware_crm_agent_master.mailingcity end as seaware_agent_mailingcity,
case when seaware_crm_agent_master.mailingcountry is null then crm_agent_master.mailingcountry else seaware_crm_agent_master.mailingcountry end as seaware_agent_mailingcountry,
case when seaware_crm_agent_master.mailingpostalcode is null then crm_agent_master.mailingpostalcode else seaware_crm_agent_master.mailingpostalcode end as seaware_agent_mailingpostalcode,
case when seaware_crm_agent_master.mailingstate is null then crm_agent_master.mailingstate else seaware_crm_agent_master.mailingstate end as seaware_agent_mailingstate,
case when seaware_crm_agent_master.mailingstreet is null then crm_agent_master.mailingstreet else seaware_crm_agent_master.mailingstreet end as seaware_agent_mailingstreet,
case when seaware_crm_agent_master.seaware_agencyid__c is null then crm_agent_master.seaware_agencyid__c else seaware_crm_agent_master.seaware_agencyid__c end as seaware_agent_seaware_agencyid,
case when seaware_crm_agent_master.createddate is null then crm_agent_master.createddate else seaware_crm_agent_master.createddate end as seaware_agent_createddate,
case when seaware_crm_agent_master.agent_status__c is null then crm_agent_master.agent_status__c else seaware_crm_agent_master.agent_status__c end as seaware_agent_status,
case when seaware_crm_agent_master.agent_type_c is null then crm_agent_master.agent_type_c else seaware_crm_agent_master.agent_type_c end as seaware_agent_type,
case when seaware_crm_agent_master.email is null then crm_agent_master.email else seaware_crm_agent_master.email end as seaware_agent_email,
case when seaware_crm_agent_master.ownerid is null then crm_agent_master.ownerid else seaware_crm_agent_master.ownerid end as seaware_agent_ownerid,
case when seaware_crm_agent_master.role__c is null then crm_agent_master.role__c else seaware_crm_agent_master.role__c end as seaware_agent_role,
case when seaware_crm_agent_master.fmdc_approver__c is null then crm_agent_master.fmdc_approver__c else seaware_crm_agent_master.fmdc_approver__c end as seaware_agent_fmdc_approver,
case when seaware_crm_agent_master.preferred_contact_phone_number__c is null then crm_agent_master.preferred_contact_phone_number__c else seaware_crm_agent_master.preferred_contact_phone_number__c end as seaware_agent_preferred_phone_number, 
case when seaware_core_crm_user_vw.name is null then core_crm_user_vw.name else seaware_core_crm_user_vw.name end as seaware_contact_owner_name, 
/*Agent details from Seaware-End*/
/*Agent details from CRM-Begin*/
crm_agent_master.agent_id crm_agent_id,
crm_agent_master.seaware_agent_id__c as crm_seaware_src_agent_id,
crm_agent_master.name as crm_agent_name,
crm_agent_master.lastname as crm_agent_lastname,
crm_agent_master.firstname as crm_agent_firstname,
crm_agent_master.mailingcity as crm_agent_mailingcity,
crm_agent_master.mailingcountry as crm_agent_mailingcountry,
crm_agent_master.mailingpostalcode as crm_agent_mailingpostalcode,
crm_agent_master.mailingstate as crm_agent_mailingstate,
crm_agent_master.mailingstreet as crm_agent_mailingstreet,
crm_agent_master.seaware_agencyid__c as crm_agent_seaware_agencyid,
crm_agent_master.createddate as crm_agent_createddate,
crm_agent_master.agent_status__c as crm_agent_status,
crm_agent_master.agent_type_c as crm_agent_type,
crm_agent_master.email as crm_agent_email,
crm_agent_master.ownerid as crm_agent_ownerid,
crm_agent_master.role__c as crm_agent_role,
crm_agent_master.fmdc_approver__c as crm_agent_fmdc_approver, 
crm_agent_master.preferred_contact_phone_number__c as crm_agent_preferred_phone_number,
core_crm_user_vw.name as crm_contact_owner_name,
/*Agent details from CRM-End*/
seaware_hotel_res_req_dim.hotel_res_req_id AS "hotel_res_req_id (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.request_type AS "request_type (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.start_date AS "start_date (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.end_date AS "end_date (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.city_code AS "city_code (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.hotel_id AS "hotel_id (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.hotel_category AS "hotel_category (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.room_seq_number AS "room_seq_number (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.room_category AS "room_category (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.room_type AS "room_type (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.occupancy AS "occupancy (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.hotel_space_type AS "hotel_space_type (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.allocation_id AS "allocation_id (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.inv_result AS "inv_result (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.is_land_component AS "is_land_component (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.hotel_bed_type AS "hotel_bed_type (seaware_hotel_res_req_dim)",
seaware_hotel_res_req_dim.effective_date AS "effective_date (seaware_hotel_res_req_dim)",
seaware_hotel_lkp.hotel_code AS "hotel_code (seaware_hotel_lkp)",
seaware_hotel_lkp.hotel_name AS "hotel_name (seaware_hotel_lkp)",
seaware_hotel_lkp.hotel_type AS "hotel_type (seaware_hotel_lkp)",
seaware_hotel_lkp.is_active AS "is_active (seaware_hotel_lkp)",
seaware_hotel_lkp.hotel_rating AS "hotel_rating (seaware_hotel_lkp)",
seaware_hotel_lkp.address_type AS "address_type (seaware_hotel_lkp)",
seaware_hotel_lkp.address_line1 AS "address_line1 (seaware_hotel_lkp)",
seaware_hotel_lkp.address_line2 AS "address_line2 (seaware_hotel_lkp)",
seaware_hotel_lkp.address_line3 AS "address_line3 (seaware_hotel_lkp)",
seaware_hotel_lkp.address_line4 AS "address_line4 (seaware_hotel_lkp)",
seaware_hotel_lkp.address_city AS "address_city (seaware_hotel_lkp)",
seaware_hotel_lkp.state_code AS "state_code (seaware_hotel_lkp)",
seaware_hotel_lkp.zip AS "zip (seaware_hotel_lkp)",
seaware_hotel_lkp.country_code AS "country_code (seaware_hotel_lkp)",
"seaware_ship_room_request_dim"."ship_room_req_id" AS "ship_room_req_id (seaware_ship_room_request_dim)",
"seaware_ship_room_request_dim"."src_request_id" AS "src_request_id (seaware_ship_room_request_dim)",
"seaware_ship_room_request_dim"."effective_date" AS "effective_date (seaware_ship_room_request_dim)",
"seaware_ship_room_request_dim"."inventory_request_type" AS "inventory_request_type",
"seaware_ship_room_request_dim"."usage_start" AS "usage_start",
"seaware_ship_room_request_dim"."usage_duration" AS "usage_duration",
"seaware_ship_room_request_dim"."party_size" AS "party_size (seaware_ship_room_request_dim)",
"seaware_ship_room_request_dim"."quantity" AS "quantity",
"seaware_ship_room_request_dim"."allocation_id" AS "allocation_id (seaware_ship_room_request_dim)",
"seaware_ship_room_request_dim"."ship_room_id" AS "ship_room_id",
"seaware_ship_room_dim"."src_ship_room_id" AS "src_ship_room_id",
"seaware_ship_room_dim"."facility_id" AS "facility_id (seaware_ship_room_dim)",
"seaware_ship_room_dim"."room_number" AS "room_number",
"seaware_ship_room_dim"."room_name" AS "room_name",
"seaware_ship_room_dim"."room_size" AS "room_size",
"seaware_ship_room_dim"."max_occupancy" AS "max_occupancy (seaware_ship_room_dim)",
"seaware_ship_facility_lkp"."ship_code" AS "ship_code (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."facility_type" AS "facility_type (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."facility_code" AS "facility_code (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."facility_name" AS "facility_name (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."deck_number" AS "deck_number (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."facility_size" AS "facility_size (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."assign_mode" AS "assign_mode (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."facility_rank" AS "facility_rank (seaware_ship_facility_lkp)",
"seaware_ship_facility_lkp"."facility_subtype" AS "facility_subtype (seaware_ship_facility_lkp)",
seaware_guest_flight_rel.guest_flight_id as guest_flight_id,
seaware_guest_flight_rel.ticket_no as flight_ticket_no,
seaware_guest_flight_rel.is_booking_independent as flight_is_booking_independent,
seaware_guest_flight_rel.booking_status as flight_booking_status,
seaware_guest_flight_rel.fare_kind as flight_fare_kind,
seaware_guest_flight_rel.seaware_fare_base as flight_seaware_fare_base,
seaware_guest_flight_rel.seaware_total_tax as flight_seaware_total_tax,
seaware_guest_flight_rel.seaware_markup as flight_seaware_markup,
seaware_guest_flight_rel.amadeus_fare_base as flight_amadeus_fare_base,
seaware_guest_flight_rel.amadeus_total_tax as flight_amadeus_total_tax,
seaware_guest_flight_rel.is_paid as flight_is_paid,
seaware_guest_flight_rel.segment_kind as flight_segment_kind,
seaware_guest_flight_rel.segment_seqn as flight_segment_seqn,
seaware_guest_flight_rel.effective_date as flight_effective_date,
seaware_flight_dim_leg1.flight_no as leg1_flight_no,
seaware_flight_dim_leg1.airline as leg1_airline,
seaware_flight_dim_leg1.airline_code as leg1_airline_code,
seaware_flight_dim_leg1.departure_airport_name as leg1_departure_airport_name,
seaware_flight_dim_leg1.origin as leg1_flight_origin,
seaware_flight_dim_leg1.departure_date as leg1_flight_departure_date,
seaware_flight_dim_leg1.departure_time as leg1_flight_departure_time,
seaware_flight_dim_leg1.arrival_airport_name as leg1_arrival_airport_name,
seaware_flight_dim_leg1.destination as leg1_flight_destination,
seaware_flight_dim_leg1.arrival_date as leg1_flight_arrival_date,
seaware_flight_dim_leg1.arrival_time as leg1_flight_arrival_time,
seaware_flight_dim_leg1.operated_by as leg1_flight_operated_by,
seaware_flight_dim_leg1.airline_rloc as leg1_airline_rloc,
seaware_flight_dim_leg2.flight_no as leg2_flight_no,
seaware_flight_dim_leg2.airline as leg2_airline,
seaware_flight_dim_leg2.airline_code as leg2_airline_code,
seaware_flight_dim_leg2.departure_airport_name as leg2_departure_airport_name,
seaware_flight_dim_leg2.origin as leg2_flight_origin,
seaware_flight_dim_leg2.departure_date as leg2_flight_departure_date,
seaware_flight_dim_leg2.departure_time as leg2_flight_departure_time,
seaware_flight_dim_leg2.arrival_airport_name as leg2_arrival_airport_name,
seaware_flight_dim_leg2.destination as leg2_flight_destination,
seaware_flight_dim_leg2.arrival_date as leg2_flight_arrival_date,
seaware_flight_dim_leg2.arrival_time as leg2_flight_arrival_time,
seaware_flight_dim_leg2.operated_by as leg2_flight_operated_by,
seaware_flight_dim_leg2.airline_rloc as leg2_airline_rloc,
seaware_flight_dim_leg3.flight_no as leg3_flight_no,
seaware_flight_dim_leg3.airline as leg3_airline,
seaware_flight_dim_leg3.airline_code as leg3_airline_code,
seaware_flight_dim_leg3.departure_airport_name as leg3_departure_airport_name,
seaware_flight_dim_leg3.origin as leg3_flight_origin,
seaware_flight_dim_leg3.departure_date as leg3_flight_departure_date,
seaware_flight_dim_leg3.departure_time as leg3_flight_departure_time,
seaware_flight_dim_leg3.arrival_airport_name as leg3_arrival_airport_name,
seaware_flight_dim_leg3.destination as leg3_flight_destination,
seaware_flight_dim_leg3.arrival_date as leg3_flight_arrival_date,
seaware_flight_dim_leg3.arrival_time as leg3_flight_arrival_time,
seaware_flight_dim_leg3.operated_by as leg3_flight_operated_by,
seaware_flight_dim_leg3.airline_rloc as leg3_airline_rloc,
seaware_flight_dim_leg4.flight_no as leg4_flight_no,
seaware_flight_dim_leg4.airline as leg4_airline,
seaware_flight_dim_leg4.airline_code as leg4_airline_code,
seaware_flight_dim_leg4.departure_airport_name as leg4_departure_airport_name,
seaware_flight_dim_leg4.origin as leg4_flight_origin,
seaware_flight_dim_leg4.departure_date as leg4_flight_departure_date,
seaware_flight_dim_leg4.departure_time as leg4_flight_departure_time,
seaware_flight_dim_leg4.arrival_airport_name as leg4_arrival_airport_name,
seaware_flight_dim_leg4.destination as leg4_flight_destination,
seaware_flight_dim_leg4.arrival_date as leg4_flight_arrival_date,
seaware_flight_dim_leg4.arrival_time as leg4_flight_arrival_time,
seaware_flight_dim_leg4.operated_by as leg4_flight_operated_by,
seaware_flight_dim_leg4.airline_rloc as leg4_airline_rloc,
seaware_coupon_class_dim.coupon_class as coupon_class,
seaware_coupon_class_dim.coupon_class_desc as coupon_class_desc,
seaware_coupon_class_dim.gen_per_client as coupon_gen_per_client,
seaware_coupon_class_dim.apply_as_payment as apply_as_payment,
seaware_coupon_class_dim.apply_per_client as apply_per_client,
seaware_coupon_class_dim.cancel_with_res as cancel_with_res,
seaware_coupon_class_dim.apply_as_discount as apply_as_discount,
pi.package_type pkg_itinerary_package_type,
pi.location_type_from pkg_itinerary_location_type_from,
pi.location_type_to pkg_itinerary_location_type_to,	
pi.location_code_from pkg_itinerary_location_code_from,
pi.location_code_to pkg_itinerary_location_code_to,
pi.COMPONENT_TYPE as pkg_itinerary_component_type,
pi.COMPONENT_CODE as pkg_itinerary_component_code,
pi.CAN_BE_EXCLUDED as pkg_itinerary_can_be_excluded,
pi.IS_OPTIONAL as pkg_itinerary_is_optional,
pi.PACKAGE_LINKS_INCLUSIVE as pkg_itinerary_package_links_inclusive,
pi.DESCRIPTION as pkg_itinerary_description,
pi.SEQ_NUM as pkg_itinerary_seq_num,
pi.CITY_FROM as pkg_itinerary_city_from,
pi.CITY_TO as pkg_itinerary_city_to,
pi.OPTION_GROUP as pkg_itinerary_option_group, 
vl.DATE_FROM as vendor_link_date_from,
vl.DATE_TO as vendor_link_date_to,
v.vendor_id,
v.vendor_name,
v.VENDOR_CODE,
v.VENDOR_NAME_TYPED,
v.CONTACT as vendor_contact,
v.IS_ACTIVE as vendor_is_active,
v.EMAIL as vendor_email,
pt.PACKAGE_TYPE_ID,
pt.PACKAGE_TYPE as pt_PACKAGE_TYPE,
pt.LAND_DAYS,
pt.SAIL_DAYS as pt_sail_days,
pt.COMMENTS as tour_name,
pt.PRODUCT_TYPE as pt_product_type,
pt.LAND_DAYS_POST,
pt.IS_LAND_ONLY,
pt.IS_MULTI_SAIL,
pt.IS_SHOREX,
pt.IS_SECONDARY,
pt.IS_ACTIVE as pt_is_active,
pt.SHOREX_TIMING as pt_shorex_timing,
pt.EXTRA_SEAT_QUESTION,
pt.PRE_POST_MODE,
pt.PACKAGE_CLASS as pt_package_class,
pt.SAIL_SEGMENTS,
pt.CAPACITY as pt_capacity,
pt.ALLOW_SEGMENTS,
pt.PACKAGE_TYPE_NAME 
from
	(select  
case when revenue_comm.revenue_comm_res_id is null then 
	case when seaware_promotion_dim.res_id is null then 
		case when seaware_res_package_rel.res_id is null then 
			case when seaware_res_addon_rel.res_id is null then 
				seaware_coupon_dim.rg_res_id 
			else 
				seaware_res_addon_rel.res_id 	
			end 	
		else 
			seaware_res_package_rel.res_id 
		end 	
	else 
		seaware_promotion_dim.res_id 
	end 
else 	
	revenue_comm.revenue_comm_res_id 
end as res_id,
case when revenue_comm.revenue_comm_guest_id is null then 
	case when seaware_promotion_dim.guest_id is null then 
		case when seaware_res_package_rel.guest_id is null then 
			case when seaware_res_addon_rel.guest_id is null then 
				seaware_coupon_dim.guest_id 
			else 
				seaware_res_addon_rel.guest_id
			end 	
		else 
			seaware_res_package_rel.guest_id 
		end 	
	else 
		seaware_promotion_dim.guest_id 
	end 
else 	
	revenue_comm.revenue_comm_guest_id 
end as guest_id,
case when revenue_comm.revenue_comm_promotion_id is null then 
	seaware_promotion_dim.promotion_id  
else 	
	revenue_comm.revenue_comm_promotion_id 
end as promotion_id,
case when revenue_comm.revenue_comm_package_id is null then 
	case when seaware_promotion_dim.package_id is null then 
		seaware_res_package_rel.package_id  
	else 
		seaware_promotion_dim.package_id  
	end 
else 	
	revenue_comm.revenue_comm_package_id 
end as package_id,
seaware_res_package_rel.effective_date as res_package_effective_date,
case when revenue_comm.revenue_comm_addon_id is null then 
	seaware_res_addon_rel.addon_id   
else 	
	revenue_comm.revenue_comm_addon_id  
end as addon_id, 
seaware_promotion_dim.is_active as is_active_promotion,
seaware_promotion_dim.is_excluded as is_excluded_promotion,
seaware_promotion_dim.promo_mode,
seaware_res_addon_rel.quantity as addon_quantity,
/*seaware_res_addon_rel.effective_date as addon_effective_date,
seaware_res_addon_rel.is_default as addon_is_default,
seaware_res_addon_rel.is_auto as addon_is_auto,
seaware_res_addon_rel.promotion_id as addon_promotion_id,
seaware_res_addon_rel.comments as addon_comments,
seaware_res_addon_rel.addon_status as addon_status,
seaware_res_addon_rel.notes as addon_notes, 
seaware_res_addon_rel.paid_date as addon_paid_date,*/--Commenting out additional attributes from res_addon_rel as those are repeating the revenue numbers (revenue fact is aggregating the addon records where as the res_addon_rel is maintaining multiple entries if those are booked separately.
seaware_coupon_dim.src_coupon_id as src_coupon_id,
seaware_coupon_dim.issue_date as coupon_issue_date,
seaware_coupon_dim.valid_from as coupon_valid_from,
seaware_coupon_dim.valid_to as coupon_valid_to,
seaware_coupon_dim.effective_from as coupon_effective_from,
seaware_coupon_dim.effective_to as coupon_effective_to,
seaware_coupon_dim.coupon_category as coupon_category,
seaware_coupon_dim.coupon_class as coupon_class_coupon_dim,
seaware_coupon_dim.coupon_class_code as coupon_class_code,
seaware_coupon_dim.coupon_class_comments as coupon_class_comments,
seaware_coupon_dim.apply_as_payment as apply_as_payment_coupon_dim,
seaware_coupon_dim.apply_as_discount as apply_as_discount_coupon_dim,
seaware_coupon_dim.entity_type as entity_type_coupon_dim,
seaware_coupon_dim.entity_id as entity_id_coupon_dim,
seaware_coupon_dim.is_used as coupon_is_used,
seaware_coupon_dim.is_active as coupon_is_active,
seaware_coupon_dim.currency_code as coupon_currency_code,
seaware_coupon_dim.amount as coupon_amount,
seaware_coupon_dim.amount_left as coupon_amount_left,
seaware_coupon_dim.charge_code as coupon_charge_code,
seaware_coupon_dim.reason_code as coupon_reason_code,
seaware_coupon_dim.comments as coupon_comments,
/*CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_voyagefare_regular,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_voyagefare_protected,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_voyagefare_manual,
CASE WHEN (revenue_comm.invoice_item_type='TAXES & FEES' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_taxesandfees_regular,
CASE WHEN (revenue_comm.invoice_item_type='TAXES & FEES' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_taxesandfees_protected,
CASE WHEN (revenue_comm.invoice_item_type='TAXES & FEES' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_taxesandfees_manual,
CASE WHEN (revenue_comm.invoice_item_type='SHORE THINGS' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_shorex_regular,
CASE WHEN (revenue_comm.invoice_item_type='SHORE THINGS' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_shorex_protected,
CASE WHEN (revenue_comm.invoice_item_type='SHORE THINGS' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_shorex_manual,
CASE WHEN (revenue_comm.invoice_item_type in ('SAILOR LOOT','SAILOR LOOT +') and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_sailorloot_regular,
CASE WHEN (revenue_comm.invoice_item_type in ('SAILOR LOOT','SAILOR LOOT +') and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_sailorloot_protected,
CASE WHEN (revenue_comm.invoice_item_type in ('SAILOR LOOT','SAILOR LOOT +') and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_sailorloot_manual,
CASE WHEN (revenue_comm.invoice_item_type='HOTEL FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_hotelfare_regular,
CASE WHEN (revenue_comm.invoice_item_type='HOTEL FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_hotelfare_protected,
CASE WHEN (revenue_comm.invoice_item_type='HOTEL FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_hotelfare_manual,
CASE WHEN (revenue_comm.invoice_item_type='INSURANCE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_insurance_regular,
CASE WHEN (revenue_comm.invoice_item_type='INSURANCE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as base_commission_insurance_protected,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code = 'BONUS'   
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount   
       ELSE 0 
    END as bonus_commission_voyagefare_regular,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code = 'BONUS'   
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as bonus_commission_voyagefare_protected,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code = 'BONUS'   
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount   
       ELSE 0 
    END as bonus_commission_voyagefare_manual,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, VOYAGE FARE' THEN revenue_comm.commission_amount   
       ELSE 0 
    END as gsa_commission_voyagefare,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, TAXES & FEES' THEN revenue_comm.commission_amount   
       ELSE 0 
    END as gsa_commission_taxesandfees,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, INSURANCE' THEN revenue_comm.commission_amount   
       ELSE 0 
    END as gsa_commission_insurance,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, SHORE THINGS' THEN revenue_comm.commission_amount   
       ELSE 0 
    END as gsa_commission_shorex,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, SAILOR LOOT' THEN revenue_comm.commission_amount   
       ELSE 0 
    END as gsa_commission_sailorloot,
CASE WHEN (revenue_comm.invoice_item_type in ('VOYAGE FARE','TAXES & FEES','INSURANCE') ) THEN 0 ELSE revenue_comm.commission_amount    
    END as base_commission_other_bookable, 
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate    
       ELSE 0 
    END as base_commission_voyagefare_regular_usd,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_voyagefare_protected_usd,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate  
       ELSE 0 
    END as base_commission_voyagefare_manual_usd,
CASE WHEN (revenue_comm.invoice_item_type='TAXES & FEES' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_taxesandfees_regular_usd,
CASE WHEN (revenue_comm.invoice_item_type='TAXES & FEES' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_taxesandfees_protected_usd,
CASE WHEN (revenue_comm.invoice_item_type='TAXES & FEES' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_taxesandfees_manual_usd,
CASE WHEN (revenue_comm.invoice_item_type='SHORE THINGS' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_shorex_regular_usd,
CASE WHEN (revenue_comm.invoice_item_type='SHORE THINGS' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_shorex_protected_usd,
CASE WHEN (revenue_comm.invoice_item_type='SHORE THINGS' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_shorex_manual_usd,
CASE WHEN (revenue_comm.invoice_item_type in ('SAILOR LOOT','SAILOR LOOT +') and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_sailorloot_regular_usd,
CASE WHEN (revenue_comm.invoice_item_type in ('SAILOR LOOT','SAILOR LOOT +') and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_sailorloot_protected_usd,
CASE WHEN (revenue_comm.invoice_item_type in ('SAILOR LOOT','SAILOR LOOT +') and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_sailorloot_manual_usd,
CASE WHEN (revenue_comm.invoice_item_type='HOTEL FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_hotelfare_regular_usd,
CASE WHEN (revenue_comm.invoice_item_type='HOTEL FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_hotelfare_protected_usd,
CASE WHEN (revenue_comm.invoice_item_type='HOTEL FARE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_hotelfare_manual_usd,
CASE WHEN (revenue_comm.invoice_item_type='INSURANCE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_insurance_regular_usd,
CASE WHEN (revenue_comm.invoice_item_type='INSURANCE' and revenue_comm.commission_code in ('STANDARD', 'SECONDARY') 
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as base_commission_insurance_protected_usd,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code = 'BONUS'   
and revenue_comm.commission_source not in ('PROTECTED','MANUAL')) THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as bonus_commission_voyagefare_regular_usd,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code = 'BONUS'   
and revenue_comm.commission_source = 'PROTECTED') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as bonus_commission_voyagefare_protected_usd,
CASE WHEN (revenue_comm.invoice_item_type='VOYAGE FARE' and revenue_comm.commission_code = 'BONUS'   
and revenue_comm.commission_source = 'MANUAL') THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as bonus_commission_voyagefare_manual_usd,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, VOYAGE FARE' THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as gsa_commission_voyagefare_usd,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, TAXES & FEES' THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as gsa_commission_taxesandfees_usd,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, INSURANCE' THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as gsa_commission_insurance_usd,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, SHORE THINGS' THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as gsa_commission_shorex_usd,
CASE 
         WHEN revenue_comm.commission_source = 'GSA, BOOKING, SAILOR LOOT' THEN revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate   
       ELSE 0 
    END as gsa_commission_sailorloot_usd,
CASE WHEN (revenue_comm.invoice_item_type in ('VOYAGE FARE','TAXES & FEES','INSURANCE') ) THEN 0 ELSE revenue_comm.commission_amount * revenue_comm.rev_comm_currency_rate    
    END as base_commission_other_bookable_usd,*/--MSH-68908 Removing commission related KPIs
revenue_comm.* 
from 
(
	select 
	case when revenue_fact.snapshot_date is null then commission_fact.snapshot_date else revenue_fact.snapshot_date end as snapshot_date, 
	case when revenue_fact.res_id is null then commission_fact.res_id else revenue_fact.res_id end as revenue_comm_res_id, 
	case when revenue_fact.guest_id is null then commission_fact.guest_id else revenue_fact.guest_id end as revenue_comm_guest_id,
	case when revenue_fact.package_id is null then commission_fact.package_id else revenue_fact.package_id end as revenue_comm_package_id,
	case when revenue_fact.invoice_item_type is null then commission_fact.invoice_item_type else revenue_fact.invoice_item_type end as invoice_item_type,
	revenue_fact.invoice_item_subtype, 
	case when revenue_fact.invoice_item_type_id is null then commission_fact.invoice_item_type_id else revenue_fact.invoice_item_type_id end as invoice_item_type_id,
	case when revenue_fact.sail_id is null then commission_fact.sail_id else revenue_fact.sail_id end as sail_id,
	case when revenue_fact.ship_id is null then commission_fact.ship_id else revenue_fact.sail_id end as ship_id,
	revenue_fact.addon_id as revenue_comm_addon_id, 
	revenue_fact.price_area_id, revenue_fact.booking_currency, revenue_fact.booking_currency_rate, revenue_fact.promotion_id as revenue_comm_promotion_id,
	revenue_fact.ship_room_req_id, revenue_fact.hotel_res_req_id, revenue_fact.guest_flight_id, revenue_fact.coupon_class_id,  
	case when revenue_fact.agency_id is null then commission_fact.agency_id else revenue_fact.agency_id end as agency_id, 
	case when revenue_fact.agent_id is null then commission_fact.agent_id else revenue_fact.agent_id end as agent_id,  
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.amount else 0 end as amount, /*If there are multiple commission records for a given invoice entry, set the invoice amounts as 0 for the repeated records.*/
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.voyage_amount else 0 end as voyage_amount, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.discount else 0 end as discount, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.taxesandfees else 0 end as taxesandfees, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.manual_adj else 0 end as manual_adj, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.insurance else 0 end as insurance, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.aon_insurace_payment else 0 end as aon_insurace_payment, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.voyage_commission_amt else 0 end as voyage_commission_amt, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.tax_commision_amt else 0 end as tax_commision_amt, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.voyage_credit_card_fee else 0 end as voyage_credit_card_fee, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.insurance_credit_card_fee else 0 end as insurance_credit_card_fee, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.taxesandfees_credit_card_fee else 0 end as taxesandfees_credit_card_fee, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_ticket_revenue else 0 end as actual_gross_ticket_revenue, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_other_revenue else 0 end as actual_gross_other_revenue, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_onboard_revenue else 0 end as actual_gross_onboard_revenue, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_total_revenue else 0 end as actual_gross_total_revenue, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.net_due else 0 end as net_due, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.amount_usd else 0 end as amount_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.voyage_amount_usd else 0 end as voyage_amount_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.discount_usd else 0 end as discount_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.taxesandfees_usd else 0 end as taxesandfees_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.manual_adj_usd else 0 end as manual_adj_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.insurance_usd else 0 end as insurance_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.aon_insurace_payment_usd else 0 end as aon_insurace_payment_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.voyage_commission_amt_usd else 0 end as voyage_commission_amt_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.tax_commision_amt_usd else 0 end as tax_commision_amt_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.voyage_credit_card_fee_usd else 0 end as voyage_credit_card_fee_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.insurance_credit_card_fee_usd else 0 end as insurance_credit_card_fee_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.taxesandfees_credit_card_fee_usd else 0 end as taxesandfees_credit_card_fee_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_ticket_revenue_usd else 0 end as actual_gross_ticket_revenue_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_other_revenue_usd else 0 end as actual_gross_other_revenue_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_onboard_revenue_usd else 0 end as actual_gross_onboard_revenue_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_gross_total_revenue_usd else 0 end as actual_gross_total_revenue_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.hotel_fare else 0 end as hotel_fare, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.hotel_fare_usd else 0 end as hotel_fare_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.hotel_credit_card_fee else 0 end as hotel_credit_card_fee, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.hotel_credit_card_fee_usd else 0 end as hotel_credit_card_fee_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.shorethings_fare else 0 end as shorethings_fare, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.shorethings_fare_usd else 0 end as shorethings_fare_usd, 
	revenue_fact.PCDs, 
	revenue_fact.sail_days, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_ticket_gpd else 0 end as actual_ticket_gpd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_ticket_gpd_usd else 0 end as actual_ticket_gpd_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_onboard_gpd else 0 end as actual_onboard_gpd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_onboard_gpd_usd else 0 end as actual_onboard_gpd_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_other_gpd else 0 end as actual_other_gpd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_other_gpd_usd else 0 end as actual_other_gpd_usd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_total_gpd else 0 end as actual_total_gpd, 
	case when nvl(commission_fact.row_num,1)=1 then revenue_fact.actual_total_gpd_usd else 0 end as actual_total_gpd_usd, 
	revenue_fact.APCDs,
	revenue_fact.total_sail_days, 
	cast(revenue_fact.total_sail_days as float)/case when revenue_fact.APCDs=0 then 1 else cast(revenue_fact.APCDs as float) end as load_factor,
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.currency_code else null end as currency_code, /*If there are multiple revenue records for a given commission entry, set the commission amounts as 0 for the repeated records.*/
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.currency_rate else null end as currency_rate, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_type_skey else null end as commission_type_skey, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_percent else null end as commission_percent, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_amount else 0 end as commission_amount, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_fare else 0 end as commission_fare, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_payout_date else null end as commission_payout_date, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_type_id else null end as commission_type_id, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_code else null end as commission_code, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.commission_source else null end as commission_source, 
	case when nvl(revenue_fact.revn_row_num,1)=1 then commission_fact.is_cancelled else null end as is_cancelled_comm, 
	case when revenue_fact.booking_currency is null then commission_fact.currency_code else revenue_fact.booking_currency end as rev_comm_currency_code,
	case when revenue_fact.booking_currency_rate is null then commission_fact.currency_rate else revenue_fact.booking_currency_rate end as rev_comm_currency_rate,
	commission_fact.row_num as comm_row_num, 
	revenue_fact.revn_row_num,
	revenue_fact.charge_id, revenue_fact.charge_component_code, revenue_fact.charge_state, revenue_fact.action_type, revenue_fact.charge_explanation, 
revenue_fact.charge_change_area, revenue_fact.charge_action_timestamp, revenue_fact.charge_is_added_manually, revenue_fact.charge_is_paid, revenue_fact.charge_paid_timestamp, revenue_fact.charge_comments, revenue_fact.charge_src_record_id, 
case when nvl(commission_fact.row_num,1)=1 then revenue_fact.charge_amount else 0 end as charge_amount, 
revenue_fact.coupon_id 	
	 from
    	(
        select snapshot_date
              , res_id
              , guest_id
			  , package_id
			  , invoice_item_type
			  , invoice_item_subtype
			  , invoice_item_type_id
			  , sail_id
			  , ship_id
			  , addon_id
			  , price_area_id 
			  , booking_currency
			  , booking_currency_rate 
			  , promotion_id
			  , ship_room_req_id
			  , hotel_res_req_id 
			  , guest_flight_id 
			  , coupon_class_id 	
              , agency_id --Using agent & agency ids from reservation_dim in main query 
			  , agent_id --Using agent & agency ids from reservation_dim in main query 
			, amount
 			,voyage_amount
			,discount
			,taxesandfees
			,manual_adj
			,insurance
			,aon_insurace_payment
			,voyage_commission_amt
			,tax_commision_amt
			,voyage_credit_card_fee
			,insurance_credit_card_fee
			,taxesandfees_credit_card_fee
			,actual_gross_ticket_revenue
			,actual_gross_other_revenue
			,actual_gross_onboard_revenue
			,actual_gross_total_revenue
			,net_due
			,amount_usd
			,voyage_amount_usd
			,discount_usd 
			,taxesandfees_usd
			,manual_adj_usd
			,insurance_usd
			,aon_insurace_payment_usd
			,voyage_commission_amt_usd
			,tax_commision_amt_usd
			,voyage_credit_card_fee_usd
			,insurance_credit_card_fee_usd
			,taxesandfees_credit_card_fee_usd
			,actual_gross_ticket_revenue_usd
			,actual_gross_other_revenue_usd
			,actual_gross_onboard_revenue_usd
			,actual_gross_total_revenue_usd
			,hotel_fare
			,hotel_fare_usd
			,hotel_credit_card_fee
			,hotel_credit_card_fee_usd 
			,shorethings_fare
			,shorethings_fare_usd 
			,PCDs
			,sail_days
			,actual_ticket_gpd
			,actual_ticket_gpd_usd
			,actual_onboard_gpd 
			,actual_onboard_gpd_usd 
			,actual_other_gpd 
			,actual_other_gpd_usd 
			,actual_total_gpd
			,actual_total_gpd_usd
			,APCDs
			,total_sail_days 
			--,sum(sd.sail_days)/(2770 * max(sd.sail_days)) as load_factor 
			, charge_id, charge_component_code, charge_state, action_type, charge_explanation, 
			charge_change_area, charge_action_timestamp, charge_is_added_manually, charge_is_paid, charge_paid_timestamp, charge_comments, charge_src_record_id,
			charge_amount, row_num,0 as coupon_id,
			row_number() over (partition by revenue_charge.res_id, revenue_charge.guest_id, revenue_charge.package_id,revenue_charge.addon_id,revenue_charge.invoice_item_type,revenue_charge.charge_id order by revenue_charge.amount desc) as revn_row_num 
			from    
seaware.seaware_revenue_commission_analysis_cube_temp revenue_charge  	
        ) revenue_fact 
	full join
        (
		select cf.snapshot_date, cf.res_id, cf.guest_id, cf.package_id, 
		cf.addon_id, 
		cf.invoice_item_type_id, cf.ship_id, cf.agency_id, cf.agent_id, cf.sail_id, cf.currency_code, cf.currency_rate, cf.commission_type_skey, cf.commission_percent, cf.commission_amount, cf.commission_fare, cf.commission_payout_date,
		cf.charge_id, cf.is_cancelled,
		seaware_invoice_item_type_lkp.invoice_item_type invoice_item_type,
		seaware_commission_type_lkp.commission_type_id commission_type_id,
		seaware_commission_type_lkp.commission_code commission_code,
		seaware_commission_type_lkp.commission_source commission_source,
		row_number() over (partition by cf.res_id, cf.guest_id, cf.package_id,cf.addon_id,invoice_item_type,charge_id order by cf.commission_fare, cf.commission_amount desc) as row_num 	
		from hive_schema_stg.seaware_commission_fact cf   
		join s2 on s2.max_snap_date = cf.snapshot_date 
		left join hive_schema_stg.seaware_invoice_item_type_lkp seaware_invoice_item_type_lkp on cf.invoice_item_type_id = seaware_invoice_item_type_lkp.invoice_item_type_id 
		left join hive_schema_stg.seaware_commission_type_lkp seaware_commission_type_lkp on cf.commission_type_skey = seaware_commission_type_lkp.commission_type_id
		) 
		commission_fact 
		on
            revenue_fact.res_id                = commission_fact.res_id
            and revenue_fact.guest_id          = commission_fact.guest_id
            and revenue_fact.package_id        = commission_fact.package_id
			/*and revenue_fact.invoice_item_type = commission_fact.invoice_item_type*/ 
            and (case when revenue_fact.charge_id=-1 then revenue_fact.invoice_item_type = commission_fact.invoice_item_type else 1=1 end)   
			and revenue_fact.addon_id		   = commission_fact.addon_id --Part of Addon_id addition to commission_fact. 
			and (case when revenue_fact.charge_id<>-1 then (revenue_fact.charge_id = commission_fact.charge_id and revenue_fact.row_num=1) else 1=1 end) 
		where (nvl(revenue_fact.revn_row_num,1)=1 OR nvl(commission_fact.row_num,1)=1)		
	) revenue_comm 
	full join (select res_id, guest_id, addon_id, sum(quantity) quantity from hive_schema_stg.seaware_res_addon_rel where rec_end_dttm='9999-12-31' group by res_id, guest_id, addon_id) seaware_res_addon_rel 
	/*(select * from hive_schema_stg.seaware_addon_fact where snapshot_date=(select max(snapshot_date) from hive_schema_stg.seaware_addon_fact)) seaware_addon_fact*/  
	on (revenue_comm.revenue_comm_res_id = seaware_res_addon_rel.res_id and revenue_comm.revenue_comm_guest_id = seaware_res_addon_rel.guest_id and revenue_comm.revenue_comm_addon_id = seaware_res_addon_rel.addon_id)  
	full join (select * from hive_schema_stg.seaware_promotion_dim where rec_end_dttm ='9999-12-31' and is_excluded='N') as seaware_promotion_dim ON (revenue_comm.revenue_comm_res_id=seaware_promotion_dim.res_id and revenue_comm.revenue_comm_guest_id=seaware_promotion_dim.guest_id and revenue_comm.revenue_comm_package_id=seaware_promotion_dim.package_id and revenue_comm.revenue_comm_promotion_id=seaware_promotion_dim.promotion_id)--Per data in seaware_promotion_dim, it maintains all latest IDs. If not will have to replace this with alias 	
	full join (select rd.res_id, gd.guest_id, pd.package_id, rp_rel.* from hive_schema_stg.seaware_res_package_rel rp_rel 
	join hive_schema_stg.seaware_reservation_dim rd on rp_rel.src_res_id=rd.src_res_id and rd.rec_end_dttm ='9999-12-31' 
	join hive_schema_stg.seaware_guest_dim gd on rp_rel.src_guest_id = gd.src_guest_id and gd.rec_end_dttm ='9999-12-31' 
	join hive_schema_stg.seaware_package_dim pd on rp_rel.src_package_id = pd.src_package_id and pd.rec_end_dttm ='9999-12-31'
	where rp_rel.rec_end_dttm ='9999-12-31') seaware_res_package_rel 
	on (seaware_res_package_rel.res_id=revenue_comm.revenue_comm_res_id and seaware_res_package_rel.guest_id=revenue_comm.revenue_comm_guest_id and seaware_res_package_rel.package_id=revenue_comm.revenue_comm_package_id) 
	full join (select res_guest.*,cd.* from seaware.seaware_coupon_dim cd 
	join (
	select rd.src_res_id,rd.res_id rg_res_id,gd.guest_id,gd.client_id rg_client_id  
	from seaware.seaware_guest_dim gd 
	left join seaware.seaware_res_guest_rel rel on rel.guest_id=gd.guest_id 
	left join seaware.seaware_reservation_dim rd on rel.res_id=rd.res_id 
	where gd.rec_end_dttm='9999-12-31') res_guest on cd.entity_id=res_guest.rg_client_id and cd.entity_type='CLNT' and cd.res_id=res_guest.src_res_id  
where cd.rec_end_dttm='9999-12-31') seaware_coupon_dim on seaware_coupon_dim.res_id=revenue_comm.revenue_comm_res_id and seaware_coupon_dim.guest_id=revenue_comm.revenue_comm_guest_id and revenue_comm.coupon_id=seaware_coupon_dim.coupon_id  
	) revenue_comm_etc 		
    /*left join
        hive_schema_stg.seaware_commission_type_lkp seaware_commission_type_lkp
        on
            revenue_comm_etc.commission_type_skey = seaware_commission_type_lkp.commission_type_id */
	left join hive_schema_stg.seaware_price_area_lkp seaware_price_area_lkp 
		on seaware_price_area_lkp.price_area_id = revenue_comm_etc.price_area_id
	left join hive_schema_stg.seaware_promotion_lkp seaware_promotion_lkp 
		on seaware_promotion_lkp.promotion_id = revenue_comm_etc.promotion_id 
	full join (select res_id, guest_id from hive_schema_stg.seaware_res_guest_rel union select distinct res_id, guest_id from hive_schema_stg.seaware_revenue_fact rf join s1 on s1.max_snap_date = rf.snapshot_date) seaware_res_guest_rel 
		on 
		revenue_comm_etc.res_id = seaware_res_guest_rel.res_id and revenue_comm_etc.guest_id = seaware_res_guest_rel.guest_id 	
	full join
        (select * from seaware.seaware_reservation_dim where rec_end_dttm='9999-12-31') seaware_reservation_dim 
        on
          seaware_res_guest_rel.res_id = seaware_reservation_dim.res_id  
	full join 
		(select * from hive_schema_stg.seaware_guest_dim where rec_end_dttm='9999-12-31' 
		AND (lower(first_name) not like '%test%' and lower(first_name) not like 'sailor%')
	AND (lower(last_name) not like '%test%' and lower(last_name) not like '%sailor%' and lower(last_name) not like '%prod' and lower(last_name) not like '%hold')) seaware_guest_dim  
		on 
		seaware_res_guest_rel.guest_id = seaware_guest_dim.guest_id	 
	full join
        (select * from hive_schema_stg.seaware_package_dim where rec_end_dttm='9999-12-31' and is_active='Y' and (lower(package_name) not like '%test%' and lower(package_name) not like '%family%')) seaware_package_dim
        on
            revenue_comm_etc.package_id = seaware_package_dim.package_id 
	--full join for sail_dim is failing hence taking left join with reservation_dim and package_dim conditionally. This will list sailings as long as it has either reservations or has packages associated to it. 
	left join
        (select * from hive_schema_stg.seaware_sail_dim where rec_end_dttm='9999-12-31') seaware_sail_dim
        on
            --seaware_reservation_dim.sail_id = seaware_sail_dim.sail_id  		
			case when seaware_reservation_dim.res_id is null then (seaware_sail_dim.src_sail_id = seaware_package_dim.src_sail_id) else (seaware_reservation_dim.src_sail_id = seaware_sail_dim.src_sail_id) end   
	left join
        seaware.seaware_ship_dim seaware_ship_dim
        on
            seaware_sail_dim.ship_id = seaware_ship_dim.ship_id
	left join hive_schema_stg.seaware_package_dim p_voy on seaware_sail_dim.src_sail_id=p_voy.src_sail_id and  p_voy.package_class='VOYAGE'	and p_voy.rec_end_dttm = '9999-12-31' and p_voy.is_active='Y' and lower(p_voy.package_name) not like '%test%' 
	left join seaware.seaware_group_dim seaware_group_dim  
		on 
		    seaware_reservation_dim.src_group_id = seaware_group_dim.src_group_id and seaware_group_dim.rec_end_dttm='9999-12-31 00:00:00' 			
	LEFT JOIN 
       (select f.*,rd.src_res_id, gd.src_guest_id from seaware.seaware_booked_cabin_reservation_dim f  
       INNER JOIN seaware.seaware_reservation_dim rd ON (rd.res_id = f.res_id) 
       INNER JOIN seaware.seaware_guest_dim gd ON (gd.guest_id = f.guest_id)  
       where f.rec_end_dttm='9999-12-31 00:00:00' 
       ) seaware_booked_cabin_reservation_dim   
       on seaware_reservation_dim.src_res_id = seaware_booked_cabin_reservation_dim.src_res_id and seaware_guest_dim.src_guest_id=seaware_booked_cabin_reservation_dim.src_guest_id 
	LEFT JOIN crm.core_crm_opportunity_vw crm_opportunity ON crm_opportunity.reservation_number__c = seaware_reservation_dim.src_res_id   
	LEFT JOIN seaware.seaware_cabin_master seaware_cabin_master ON (seaware_booked_cabin_reservation_dim.cabin_id = seaware_cabin_master.cabin_id) 
	LEFT JOIN seaware.seaware_sail_cabin_reserve_dim seaware_sail_cabin_reserve_dim ON (seaware_booked_cabin_reservation_dim.sail_cabin_reserve_id = seaware_sail_cabin_reserve_dim.sail_cabin_reserve_id) 
	LEFT JOIN hive_schema_stg.seaware_allocation_lkp seaware_allocation_lkp ON (seaware_booked_cabin_reservation_dim.allocation_id=seaware_allocation_lkp.allocation_id) 
	left join hive_schema_stg.seaware_addon_lkp seaware_addon_lkp on (revenue_comm_etc.addon_id=seaware_addon_lkp.addon_id)	
	/*left join (select case when rf.res_id is null then cf.res_id else rf.res_id end as res_id,
case when rf.agency_id is null then cf.agency_id else rf.agency_id end as agency_id,
case when rf.agent_id is null then cf.agent_id else rf.agent_id end as agent_id  
from 
(select distinct res_id, agency_id,agent_id from seaware.seaware_revenue_fact rf join s1 on rf.snapshot_date = s1.max_snap_date) rf 
full join 
(select distinct res_id, agency_id,agent_id from seaware.seaware_commission_fact cf join s2 on cf.snapshot_date = s2.max_snap_date) cf 
on rf.res_id = cf.res_id) rev_comm_agency_agent on seaware_reservation_dim.res_id = rev_comm_agency_agent.res_id*/--MSH-71524 --MSH-70939 
	/*Agency & agent details from Seaware-Begin*/		
	left join
        hive_schema_stg.seaware_agency_dim seaware_agency_dim
        on seaware_agency_dim.src_agency_id = seaware_reservation_dim.agency_id and seaware_agency_dim.rec_end_dttm='9999-12-31'  --MSH-71524 /*seaware_agency_dim.agency_id = rev_comm_agency_agent.agency_id*/ /*revenue_comm_etc.agency_id */--MSH-70939
	left join crm.crm_agency_master seaware_crm_agency_master --replacing hive_schema_stg with crm for view level fields
		on seaware_crm_agency_master.seaware_agency_id__c = seaware_agency_dim.src_agency_id and seaware_crm_agency_master.rec_end_dttm='9999-12-31 00:00:00' 
	left join (select am1.* from crm.crm_agent_master am1 left JOIN hive_schema_stg.hvtb_nbx_core_crm_contact cc ON cc.id = am1.id where am1.rec_end_dttm like '9999%' and cc.isdeleted = false) seaware_crm_agent_master
        on seaware_crm_agent_master.seaware_agent_id__c = seaware_reservation_dim.agent_id and seaware_crm_agent_master.rec_end_dttm='9999-12-31 00:00:00'--MSH-71524
            /*seaware_crm_agent_master.agent_id = rev_comm_agency_agent.agent_id*//*revenue_comm_etc.agent_id*/ --MSH-70939 --and seaware_crm_agent_master.rec_end_dttm='9999-12-31 00:00:00' 	
	left join crm.core_crm_user_vw seaware_core_crm_user_vw on seaware_core_crm_user_vw.id = seaware_crm_agent_master.ownerid 		
	/*Agency & agent details from Seaware-End*/		
	/*Agency & agent details from CRM-Begin*/		
    left join
        hive_schema_stg.crm_agency_master crm_agency_master 
        on
            seaware_reservation_dim.seaware_agency_id = crm_agency_master.seaware_agency_id__c and crm_agency_master.rec_end_dttm='9999-12-31 00:00:00'  
	left join hive_schema_stg.crm_agency_master crm_parent_agency on crm_agency_master.seaware_parent_agency_id = crm_parent_agency.seaware_agency_id__c and crm_parent_agency.rec_end_dttm='9999-12-31' --Added as part of agency_link change. 
    left join
        (select am1.* from hive_schema_stg.crm_agent_master am1 left JOIN hive_schema_stg.hvtb_nbx_core_crm_contact cc ON cc.id = am1.id where am1.rec_end_dttm like '9999%' and cc.isdeleted = false) crm_agent_master
        on
            seaware_reservation_dim.seaware_agent_id = crm_agent_master.seaware_agent_id__c and crm_agent_master.rec_end_dttm='9999-12-31 00:00:00' 
	left join crm.core_crm_user_vw core_crm_user_vw	on core_crm_user_vw.id=crm_agent_master.ownerid 	
	/*Agency & agent details from CRM-End*/					
	left join seaware.seaware_hotel_res_req_dim seaware_hotel_res_req_dim ON (revenue_comm_etc.hotel_res_req_id = seaware_hotel_res_req_dim.hotel_res_req_id /*and seaware_hotel_res_req_dim.rec_end_dttm='9999-12-31 00:00:00'*/) 
	left join seaware.seaware_hotel_lkp seaware_hotel_lkp ON (seaware_hotel_res_req_dim.hotel_id = seaware_hotel_lkp.hotel_id)	
	left join seaware.seaware_ship_room_request_dim seaware_ship_room_request_dim ON (revenue_comm_etc.ship_room_req_id = seaware_ship_room_request_dim.ship_room_req_id)
	left join seaware.seaware_ship_room_dim seaware_ship_room_dim ON (seaware_ship_room_request_dim.ship_room_id = seaware_ship_room_dim.ship_room_id and seaware_ship_room_dim.rec_end_dttm='9999-12-31 00:00:00') 
    left join seaware.seaware_ship_facility_lkp seaware_ship_facility_lkp ON (seaware_ship_room_dim.facility_id = seaware_ship_facility_lkp.facility_id) 
	left join seaware.seaware_guest_flight_rel seaware_guest_flight_rel ON (seaware_guest_flight_rel.guest_flight_id = revenue_comm_etc.guest_flight_id) 
	left join seaware.seaware_flight_dim seaware_flight_dim_leg1 ON (seaware_guest_flight_rel.flight_id_leg_seqn_1 = seaware_flight_dim_leg1.flight_id) 	
	left join seaware.seaware_flight_dim seaware_flight_dim_leg2 ON (seaware_guest_flight_rel.flight_id_leg_seqn_2 = seaware_flight_dim_leg2.flight_id) 	
	left join seaware.seaware_flight_dim seaware_flight_dim_leg3 ON (seaware_guest_flight_rel.flight_id_leg_seqn_3 = seaware_flight_dim_leg3.flight_id) 	
	left join seaware.seaware_flight_dim seaware_flight_dim_leg4 ON (seaware_guest_flight_rel.flight_id_leg_seqn_4 = seaware_flight_dim_leg4.flight_id) 	
	left join seaware.seaware_coupon_class_dim seaware_coupon_class_dim ON (revenue_comm_etc.coupon_class_id = seaware_coupon_class_dim.coupon_class_id) 	
	left join hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type pt on case when seaware_package_dim.package_id is null then p_voy.package_type else seaware_package_dim.package_type end = pt.package_type
	left join (select pi.*,row_number() over(partition by package_type order by day_num_from, time_from asc) as rn from hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_itinerary pi) pi on case when seaware_package_dim.package_id is null then p_voy.package_type else seaware_package_dim.package_type end = pi.package_type and pi.rn = 1 
	left join hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type_vendor_link vl on case when seaware_package_dim.package_id is null then p_voy.package_type else seaware_package_dim.package_type end = vl.package_type
	left join hive_schema_stg.hvtb_nbx_landing_sw_rpl_vendor v on vl.vendor_id = v.vendor_id
    left join
        (
            select
                a.res_id
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
	left join seaware.geo_analysis geo_analysis 
		on seaware_guest_dim.country = geo_analysis.country and seaware_guest_dim.state = geo_analysis.state and seaware_guest_dim.zip = geo_analysis.zip 
	) main
;

drop table if exists seaware.seaware_revenue_commission_analysis_cube_temp;

drop table if exists seaware.seaware_revenue_commission_analysis_cube_rpt;

alter table seaware.seaware_revenue_commission_analysis_cube_swap rename to seaware_revenue_commission_analysis_cube_rpt;

END;
                  


$$
;
