CREATE OR REPLACE VIEW seaware.seaware_revenue_commission_analysis_cube as    
with s2 as
    (select max(snapshot_date) as max_snap_date from hive_schema_stg.seaware_commission_fact)
select main.*,
(base_commission_voyagefare + bonus_commission_voyagefare + protected_commission + base_commission_taxesandfees + bonus_commission_taxesandfees + red_hot_bonus_commission_voyage_fare) as ticket_commissions,
	(base_commission_voyagefare_usd + bonus_commission_voyagefare_usd + protected_commission_usd + base_commission_taxesandfees_usd + bonus_commission_taxesandfees_usd + red_hot_bonus_commission_voyage_fare_usd) as ticket_commissions_usd,
	(base_commission_shorex + base_commission_sailorloot) as Commissions_for_onboard_items,
	(base_commission_shorex_usd + base_commission_sailorloot_usd) as Commissions_for_onboard_items_usd,
	(base_commission_insurance + base_commission_hotel) as Commissions_for_other_revenue_items,
	(base_commission_insurance_usd + base_commission_hotel_usd) as Commissions_for_other_revenue_items_usd,
	(voyage_credit_card_fee + insurance_credit_card_fee + taxesandfees_credit_card_fee + hotel_credit_card_fee) as credit_card_fees,
	(voyage_credit_card_fee_usd + insurance_credit_card_fee_usd + taxesandfees_credit_card_fee_usd + hotel_credit_card_fee_usd) as credit_card_fees_usd,
	(insurance_credit_card_fee + hotel_credit_card_fee) as credit_card_fees_Other_revenue_items,
	(insurance_credit_card_fee_usd + hotel_credit_card_fee_usd) as credit_card_fees_Other_revenue_items_usd,
	(actual_gross_ticket_revenue - ticket_commissions - credit_card_fees) as net_ticket_revenue,
	(actual_gross_ticket_revenue_usd - ticket_commissions_usd - credit_card_fees_usd) as net_ticket_revenue_usd 
from 	
(
	select
    revenue_comm_etc.invoice_item_type as  invoice_item_type,
	revenue_comm_etc.invoice_item_subtype,
	revenue_comm_etc.booking_currency,
	revenue_comm_etc.booking_currency_rate,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.amount end as amount,
	--revenue_fact.amount,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.voyage_amount end as voyage_amount,
	--revenue_fact.voyage_amount,
	revenue_comm_etc.taxesandfees,
	revenue_comm_etc.manual_adj,
	revenue_comm_etc.insurance,
	revenue_comm_etc.aon_insurace_payment,
	revenue_comm_etc.voyage_commission_amt,
	revenue_comm_etc.tax_commision_amt,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.voyage_credit_card_fee end as voyage_credit_card_fee,
	--revenue_fact.voyage_credit_card_fee,
	revenue_comm_etc.insurance_credit_card_fee,
	revenue_comm_etc.taxesandfees_credit_card_fee,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_gross_ticket_revenue end as actual_gross_ticket_revenue,
	--revenue_fact.actual_gross_ticket_revenue,
	revenue_comm_etc.actual_gross_other_revenue,
	revenue_comm_etc.actual_gross_onboard_revenue,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_gross_total_revenue end as actual_gross_total_revenue,
	--revenue_fact.actual_gross_total_revenue,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.net_due end as net_due,
	--revenue_fact.net_due,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.amount_usd end as amount_usd,
	--revenue_fact.amount,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.voyage_amount_usd end as voyage_amount_usd,
	--revenue_fact.amount_usd,
	--revenue_fact.voyage_amount_usd,
	revenue_comm_etc.taxesandfees_usd,
	revenue_comm_etc.manual_adj_usd,
	revenue_comm_etc.insurance_usd,
	revenue_comm_etc.aon_insurace_payment_usd,
	revenue_comm_etc.voyage_commission_amt_usd,
	revenue_comm_etc.tax_commision_amt_usd,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.voyage_credit_card_fee_usd end as voyage_credit_card_fee_usd,
	--revenue_fact.voyage_credit_card_fee_usd,
	revenue_comm_etc.insurance_credit_card_fee_usd,
	revenue_comm_etc.taxesandfees_credit_card_fee_usd,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_gross_ticket_revenue_usd end as actual_gross_ticket_revenue_usd,
	--revenue_fact.actual_gross_ticket_revenue_usd,
	revenue_comm_etc.actual_gross_other_revenue_usd,
	revenue_comm_etc.actual_gross_onboard_revenue_usd,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_gross_total_revenue_usd end as actual_gross_total_revenue_usd,
	--revenue_fact.actual_gross_total_revenue_usd,
	revenue_comm_etc.hotel_fare,
	revenue_comm_etc.hotel_fare_usd,
	revenue_comm_etc.hotel_credit_card_fee,
	revenue_comm_etc.hotel_credit_card_fee_usd, 
	revenue_comm_etc.shorethings_fare,
	revenue_comm_etc.shorethings_fare_usd, 
	revenue_comm_etc.PCDs,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_ticket_gpd end as actual_ticket_gpd,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_ticket_gpd_usd end as actual_ticket_gpd_usd,
	--revenue_fact.actual_ticket_gpd,
	--revenue_fact.actual_ticket_gpd_usd,
	revenue_comm_etc.actual_onboard_gpd,
	revenue_comm_etc.actual_onboard_gpd_usd, 
	revenue_comm_etc.actual_other_gpd,
	revenue_comm_etc.actual_other_gpd_usd, 
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_total_gpd end as actual_total_gpd,
	case when revenue_comm_etc.invoice_item_type = 'VOYAGE FARE' and  seaware_commission_type_lkp.commission_code = 'BONUS' then 0 else revenue_comm_etc.actual_total_gpd_usd end as actual_total_gpd_usd,
	--revenue_fact.actual_total_gpd,
	--revenue_fact.actual_total_gpd_usd,
	revenue_comm_etc.APCDs,
	revenue_comm_etc.total_sail_days,
	revenue_comm_etc.load_factor,
	seaware_price_area_lkp.price_area_id,
	seaware_price_area_lkp.price_area,
	revenue_comm_etc.currency_code as commission_currency_code,
	revenue_comm_etc.currency_rate as commission_currency_rate,
	--seaware_commission_fact.invoice_item_type as invoice_item_type_commission, 
	revenue_comm_etc.commission_percent,
	case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end as commission_amount, --seaware_commission_fact.commission_amount,
	case when revenue_comm_etc.commission_fare is not null then
	case when revenue_comm_etc.amount is not null then revenue_comm_etc.amount else revenue_comm_etc.commission_fare end end as commission_fare, --seaware_commission_fact.commission_fare,
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
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, VOYAGE FARE' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as base_commission_voyagefare,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, VOYAGE FARE' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as base_commission_voyagefare_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, TAXES & FEES' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as base_commission_taxesandfees,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, TAXES & FEES' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as base_commission_taxesandfees_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, INSURANCE' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as base_commission_insurance,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, INSURANCE' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as base_commission_insurance_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, SHORE THINGS' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as base_commission_shorex,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, SHORE THINGS' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as base_commission_shorex_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, SAILOR LOOT' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as base_commission_sailorloot,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BASIC, BOOKING, SAILOR LOOT' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as base_commission_sailorloot_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BONUS, BOOKING, VOYAGE FARE' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as bonus_commission_voyagefare,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BONUS, BOOKING, VOYAGE FARE' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as bonus_commission_voyagefare_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BONUS, BOOKING, TAXES & FEES' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as bonus_commission_taxesandfees,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BONUS, BOOKING, TAXES & FEES' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as bonus_commission_taxesandfees_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, VOYAGE FARE' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as gsa_commission_voyagefare,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, VOYAGE FARE' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as gsa_commission_voyagefare_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, TAXES & FEES' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as gsa_commission_taxesandfees,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, TAXES & FEES' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as gsa_commission_taxesandfees_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, INSURANCE' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as gsa_commission_insurance,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, INSURANCE' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as gsa_commission_insurance_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, SHORE THINGS' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as gsa_commission_shorex,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, SHORE THINGS' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as gsa_commission_shorex_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, SAILOR LOOT' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as gsa_commission_sailorloot,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'GSA, BOOKING, SAILOR LOOT' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as gsa_commission_sailorloot_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'PROTECTED' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as protected_commission,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'PROTECTED' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as protected_commission_usd,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BONUS, BOOKING, HOTEL FARE' THEN case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end  
       ELSE 0 
    END as base_commission_hotel,
	CASE 
         WHEN seaware_commission_type_lkp.commission_source = 'BONUS, BOOKING, HOTEL FARE' THEN (case when revenue_comm_etc.amount is not null then (revenue_comm_etc.amount*nvl(revenue_comm_etc.commission_percent,0)/100) else revenue_comm_etc.commission_amount end) * revenue_comm_etc.currency_rate 
       ELSE 0 
    END as base_commission_hotel_usd,   
	(case when seaware_reservation_dim.SUB_SALES_CHANNEL = 'FIRST MATES' then revenue_comm_etc.voyage_amount else 0 end) * 0.0425 as red_hot_bonus_commission_voyage_fare,
	(case when seaware_reservation_dim.SUB_SALES_CHANNEL = 'FIRST MATES' then revenue_comm_etc.voyage_amount_usd else 0 end) * 0.0425 as red_hot_bonus_commission_voyage_fare_usd,
	0 as net_onboard_revenue, -- awaiting clarification on formula
	0 as net_other_revenue, -- awaiting clarification on formula
	0 as net_net_ticket_revenue, -- awaiting clarification on formula
	seaware_commission_type_lkp.commission_type_id,
	seaware_commission_type_lkp.commission_code,
	seaware_commission_type_lkp.commission_source,
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
	hbtb_nbx_sailor_tribes_vw.sailor_tribe, 
	hbtb_nbx_sailor_tribes_vw.sailor_subtribe,
	revenue_comm_etc.is_active_promotion,
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
seaware_booked_cabin_reservation_dim.reserve_type as cabin_reserve_type,
seaware_cabin_master.cabin_id,
seaware_cabin_master.cabin_number,
seaware_cabin_master.ship as cabin_ship,
seaware_cabin_master.deck_number as cabin_deck_number,
seaware_cabin_master.cabin_name,
seaware_cabin_master.cabin_rank,
seaware_cabin_master.cabin_category,
seaware_cabin_master.cabin_category_rank,
seaware_cabin_master.cabin_category_generic,
seaware_cabin_master.cabin_category_generic_rank,
seaware_cabin_master.cabin_capacity,
revenue_comm_etc.addon_quantity,
seaware_addon_lkp.addon_id,
seaware_addon_lkp.res_addon_code,
seaware_addon_lkp.addon_name,
seaware_addon_lkp.addon_category,
seaware_addon_lkp.addon_type,
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
seaware_package_dim.package_id as package_id,
seaware_package_dim.src_package_id as src_package_id,
seaware_package_dim.package_class as package_class,
seaware_package_dim.product_type as product_type,
seaware_package_dim.package_type as package_type,
seaware_package_dim.package_code as package_code,	
seaware_package_dim.package_name as package_name,	
seaware_package_dim.shorex_mode as shorex_mode,
seaware_package_dim.vacation_date as vacation_date,
seaware_package_dim.shorex_timing as shorex_timing,
seaware_package_dim.geog_area_code as geog_area_code,
seaware_package_dim.season_code as season_code,
seaware_package_dim.is_active as pkg_is_active,
p_voy.package_name as product_name,	
/*seaware_agency_dim.agency_id ad_agency_id,
seaware_agency_dim.src_agency_id,
seaware_agency_dim.agency_name as ad_agency_name, 
seaware_agency_dim.agency_code as ad_agency_code,
seaware_agency_dim.agency_sales_district,
seaware_agency_dim.agency_country,
seaware_agency_dim.agency_state,
seaware_agency_dim.agency_zip,*/
crm_agency_master.agency_id as crm_agency_id,
crm_agency_master.seaware_agency_id__c as seaware_agency_id,
crm_agency_master.name as crm_agency_name,
crm_agency_master.lastname as agency_lastname,
crm_agency_master.firstname as agency_firstname,
crm_agency_master.billingcity as agecny_billingcity,
crm_agency_master.billingcountry as agency_billingcountry,
crm_agency_master.billingpostalcode as agency_billingpostalcode,
crm_agency_master.billingstate as agency_billingstate,
crm_agency_master.billingstreet as agency_billingstreet,
crm_agency_master.createddate as agecny_createddate,
crm_agency_master.status__c as agecny_status, 
crm_agency_master.territorynametext__c as agency_territorynametext,
crm_agency_master.ownerid as agecny_ownerid,
crm_agency_master.primary_affiliation__c as agency_primary_affiliation,
crm_agency_master.parentid as agency_parentid,
crm_agency_master.agency_classification__c as agency_classification,
crm_agency_master.agency_sub_type__c as agency_sub_type,
crm_agency_master.is_touroperator__c as agency_is_touroperator,
crm_agency_master.client_id__c as agency_client_id,
crm_agency_master.createdbyid as agency_createdbyid,
crm_agency_master.lastmodifiedbyid as agency_lastmodifiedbyid,
crm_agency_master.personhasoptedoutofemail as agency_personhasoptedoutofemail,
crm_agency_master.hand_raiser__c as agency_hand_raiser,
crm_agency_master.vip_tier__c as agency_vip_tier,
crm_agency_master.webuser__c as agency_webuser,
crm_agency_master.currency_type__c as agency_currency_type,
crm_agent_master.agent_id,
crm_agent_master.seaware_agent_id__c as seaware_agent_id,
crm_agent_master.name as agent_name,
crm_agent_master.lastname as agent_lastname,
crm_agent_master.firstname as agent_firstname,
crm_agent_master.mailingcity as agent_mailingcity,
crm_agent_master.mailingcountry as agent_mailingcountry,
crm_agent_master.mailingpostalcode as agent_mailingpostalcode,
crm_agent_master.mailingstate as agent_mailingstate,
crm_agent_master.mailingstreet as agent_mailingstreet,
crm_agent_master.seaware_agencyid__c as agent_seaware_agencyid,
crm_agent_master.createddate as agent_createddate,
crm_agent_master.agent_status__c as agent_status,
crm_agent_master.agent_type_c as agent_type,
crm_agent_master.email as agent_email,
crm_agent_master.ownerid as agent_ownerid,
crm_agent_master.role__c as agent_role,
crm_agent_master.fmdc_approver__c as agent_fmdc_approver, 
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
pi.ITIN_RECORD_ID as pkg_itinerary_itin_record_id,
pi.package_type pkg_itinerary_package_type,
pi.location_type_from pkg_itinerary_location_type_from,
pi.location_type_to pkg_itinerary_location_type_to,	
pi.location_code_from pkg_itinerary_location_code_from,
pi.location_code_to pkg_itinerary_location_code_to,
pi.DAY_NUM_FROM as pkg_itinerary_day_num_from,
pi.time_from pkg_itinerary_time_from,
pi.DAY_NUM_TO as pkg_itinerary_day_num_to,
pi.time_to pkg_itinerary_time_to, 
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
			seaware_addon_fact.res_id 
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
			seaware_addon_fact.guest_id 
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
case when revenue_comm.revenue_comm_addon_id is null then 
	seaware_addon_fact.addon_id   
else 	
	revenue_comm.revenue_comm_addon_id  
end as addon_id, 
seaware_promotion_dim.is_active as is_active_promotion,
seaware_promotion_dim.promo_mode,
seaware_addon_fact.quantity as addon_quantity,
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
	revenue_fact.ship_room_req_id, revenue_fact.hotel_res_req_id,  	
	case when revenue_fact.agency_id is null then commission_fact.agency_id else revenue_fact.agency_id end as agency_id, 
	case when revenue_fact.agent_id is null then commission_fact.agent_id else revenue_fact.agent_id end as agent_id,  
	 revenue_fact.amount, revenue_fact.voyage_amount, revenue_fact.taxesandfees, revenue_fact.manual_adj, revenue_fact.insurance, revenue_fact.aon_insurace_payment, revenue_fact.voyage_commission_amt, revenue_fact.tax_commision_amt, revenue_fact.voyage_credit_card_fee, revenue_fact.insurance_credit_card_fee, revenue_fact.taxesandfees_credit_card_fee, revenue_fact.actual_gross_ticket_revenue, revenue_fact.actual_gross_other_revenue, revenue_fact.actual_gross_onboard_revenue, revenue_fact.actual_gross_total_revenue, revenue_fact.net_due, revenue_fact.amount_usd, revenue_fact.voyage_amount_usd, revenue_fact.taxesandfees_usd, revenue_fact.manual_adj_usd, revenue_fact.insurance_usd, revenue_fact.aon_insurace_payment_usd, revenue_fact.voyage_commission_amt_usd, revenue_fact.tax_commision_amt_usd, revenue_fact.voyage_credit_card_fee_usd, revenue_fact.insurance_credit_card_fee_usd, revenue_fact.taxesandfees_credit_card_fee_usd, revenue_fact.actual_gross_ticket_revenue_usd, revenue_fact.actual_gross_other_revenue_usd, revenue_fact.actual_gross_onboard_revenue_usd, revenue_fact.actual_gross_total_revenue_usd, revenue_fact.hotel_fare, revenue_fact.hotel_fare_usd, revenue_fact.hotel_credit_card_fee, revenue_fact.hotel_credit_card_fee_usd, revenue_fact.shorethings_fare, revenue_fact.shorethings_fare_usd, revenue_fact.PCDs, revenue_fact.sail_days, revenue_fact.actual_ticket_gpd, revenue_fact.actual_ticket_gpd_usd, revenue_fact.actual_onboard_gpd, revenue_fact.actual_onboard_gpd_usd, revenue_fact.actual_other_gpd, revenue_fact.actual_other_gpd_usd, revenue_fact.actual_total_gpd, revenue_fact.actual_total_gpd_usd, revenue_fact.APCDs,revenue_fact.total_sail_days, cast(revenue_fact.total_sail_days as float)/cast(revenue_fact.APCDs as float) as load_factor,
	 commission_fact.currency_code, commission_fact.currency_rate, commission_fact.commission_type_skey, commission_fact.commission_percent, commission_fact.commission_amount, commission_fact.commission_fare, commission_fact.commission_payout_date, commission_fact.base_commission_voyagefare, commission_fact.base_commission_taxesandfees, commission_fact.base_commission_insurance, commission_fact.base_commission_shorex, commission_fact.base_commission_sailorloot, commission_fact.bonus_commission_voyagefare, commission_fact.bonus_commission_taxesandfees, commission_fact.gsa_commission_voyagefare, commission_fact.gsa_commission_taxesandfees, commission_fact.gsa_commission_insurance, commission_fact.gsa_commission_shorex, commission_fact.gsa_commission_sailorloot, commission_fact.protected_commission, commission_fact.base_commission_voyagefare_usd, commission_fact.base_commission_taxesandfees_usd, commission_fact.base_commission_insurance_usd, commission_fact.base_commission_shorex_usd, commission_fact.base_commission_sailorloot_usd, commission_fact.bonus_commission_voyagefare_usd, commission_fact.bonus_commission_taxesandfees_usd, commission_fact.gsa_commission_voyagefare_usd, commission_fact.gsa_commission_taxesandfees_usd, commission_fact.gsa_commission_insurance_usd, commission_fact.gsa_commission_shorex_usd, commission_fact.gsa_commission_sailorloot_usd, commission_fact.protected_commission_usd from
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
              , max(revn.agency_id) agency_id --Using agent & agency ids from reservation_dim in main query 
			  , max(revn.agent_id) agent_id --Using agent & agency ids from reservation_dim in main query 
			,sum(revn.amount) amount
 			,sum(revn.voyage_amount) voyage_amount
			,sum(revn.taxesandfees) taxesandfees
			,sum(revn.manual_adj) manual_adj
			,sum(revn.insurance) insurance
			,sum(revn.aon_insurace_payment) aon_insurace_payment
			,sum(revn.voyage_commission_amt) voyage_commission_amt
			,sum(revn.tax_commision_amt) tax_commision_amt
			,sum(revn.voyage_credit_card_fee) voyage_credit_card_fee
			,sum(revn.insurance_credit_card_fee) insurance_credit_card_fee
			,sum(revn.taxesandfees_credit_card_fee) taxesandfees_credit_card_fee
			,sum(revn.actual_gross_ticket_revenue) actual_gross_ticket_revenue
			,sum(revn.actual_gross_other_revenue) actual_gross_other_revenue
			,sum(revn.actual_gross_onboard_revenue) actual_gross_onboard_revenue
			,sum(revn.actual_gross_total_revenue) actual_gross_total_revenue
			,max(net_due) net_due
			,sum(revn.amount_usd) amount_usd
			,sum(revn.voyage_amount_usd) voyage_amount_usd
			,sum(revn.taxesandfees_usd) taxesandfees_usd
			,sum(revn.manual_adj_usd) manual_adj_usd
			,sum(revn.insurance_usd) insurance_usd
			,sum(revn.aon_insurace_payment_usd) aon_insurace_payment_usd
			,sum(revn.voyage_commission_amt_usd) voyage_commission_amt_usd
			,sum(revn.tax_commision_amt_usd) tax_commision_amt_usd
			,sum(revn.voyage_credit_card_fee_usd) voyage_credit_card_fee_usd
			,sum(revn.insurance_credit_card_fee_usd) insurance_credit_card_fee_usd
			,sum(revn.taxesandfees_credit_card_fee_usd) taxesandfees_credit_card_fee_usd
			,sum(revn.actual_gross_ticket_revenue_usd) actual_gross_ticket_revenue_usd
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
			,sum(revn.actual_gross_ticket_revenue)/max(sd.sail_days) actual_ticket_gpd
			,(sum(revn.actual_gross_ticket_revenue)/max(sd.sail_days))*revn.currency_rate as actual_ticket_gpd_usd
			,sum(revn.actual_gross_onboard_revenue)/max(sd.sail_days) actual_onboard_gpd 
			,(sum(revn.actual_gross_onboard_revenue)/max(sd.sail_days))*revn.currency_rate as actual_onboard_gpd_usd 
			,sum(revn.actual_gross_other_revenue)/max(sd.sail_days) actual_other_gpd 
			,(sum(revn.actual_gross_other_revenue)/max(sd.sail_days))*revn.currency_rate as actual_other_gpd_usd 
			,sum(revn.actual_gross_total_revenue)/max(sd.sail_days) actual_total_gpd
			,(sum(revn.actual_gross_total_revenue)/max(sd.sail_days))*revn.currency_rate as actual_total_gpd_usd
			,2770 * max(sd.sail_days) as APCDs
			,sum(sd.sail_days) as total_sail_days 
			--,sum(sd.sail_days)/(2770 * max(sd.sail_days)) as load_factor 	
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
			  , booking_currency
			  , booking_currency_rate 
			  , promotion_id
			  , sail_days  
        )
        revenue_fact 
	full join
        (
		select cf.snapshot_date, cf.res_id, cf.guest_id, cf.package_id, cf.invoice_item_type_id, cf.ship_id, cf.agency_id, cf.agent_id, cf.sail_id, cf.currency_code, cf.currency_rate, cf.commission_type_skey, cf.commission_percent, cf.commission_amount, cf.commission_fare, cf.commission_payout_date, cf.base_commission_voyagefare, cf.base_commission_taxesandfees, cf.base_commission_insurance, cf.base_commission_shorex, cf.base_commission_sailorloot, cf.bonus_commission_voyagefare, cf.bonus_commission_taxesandfees, cf.gsa_commission_voyagefare, cf.gsa_commission_taxesandfees, cf.gsa_commission_insurance, cf.gsa_commission_shorex, cf.gsa_commission_sailorloot, cf.protected_commission, cf.base_commission_voyagefare_usd, cf.base_commission_taxesandfees_usd, cf.base_commission_insurance_usd, cf.base_commission_shorex_usd, cf.base_commission_sailorloot_usd, cf.bonus_commission_voyagefare_usd, cf.bonus_commission_taxesandfees_usd, cf.gsa_commission_voyagefare_usd, cf.gsa_commission_taxesandfees_usd, cf.gsa_commission_insurance_usd, cf.gsa_commission_shorex_usd, cf.gsa_commission_sailorloot_usd, cf.protected_commission_usd,seaware_invoice_item_type_lkp.invoice_item_type invoice_item_type from hive_schema_stg.seaware_commission_fact cf   
		join s2 on s2.max_snap_date = cf.snapshot_date 
		left join hive_schema_stg.seaware_invoice_item_type_lkp seaware_invoice_item_type_lkp on cf.invoice_item_type_id = seaware_invoice_item_type_lkp.invoice_item_type_id 
		) 
		commission_fact 
		on
            revenue_fact.res_id                = commission_fact.res_id
            and revenue_fact.guest_id          = commission_fact.guest_id
            and revenue_fact.package_id        = commission_fact.package_id
            and revenue_fact.invoice_item_type = commission_fact.invoice_item_type
	) revenue_comm 
	full join (select * from hive_schema_stg.seaware_promotion_dim where rec_end_dttm ='9999-12-31') as seaware_promotion_dim ON (revenue_comm.revenue_comm_res_id=seaware_promotion_dim.res_id and revenue_comm.revenue_comm_guest_id=seaware_promotion_dim.guest_id and revenue_comm.revenue_comm_package_id=seaware_promotion_dim.package_id and revenue_comm.revenue_comm_promotion_id=seaware_promotion_dim.promotion_id)--Per data in seaware_promotion_dim, it maintains all latest IDs. If not will have to replace this with above sql	
	full join (select rd.res_id, gd.guest_id, pd.package_id, rp_rel.* from hive_schema_stg.seaware_res_package_rel rp_rel 
	join hive_schema_stg.seaware_reservation_dim rd on rp_rel.src_res_id=rd.src_res_id and rd.rec_end_dttm ='9999-12-31' 
	join hive_schema_stg.seaware_guest_dim gd on rp_rel.src_guest_id = gd.src_guest_id and gd.rec_end_dttm ='9999-12-31' 
	join hive_schema_stg.seaware_package_dim pd on rp_rel.src_package_id = pd.src_package_id and pd.rec_end_dttm ='9999-12-31') seaware_res_package_rel 
	on (seaware_res_package_rel.res_id=revenue_comm.revenue_comm_res_id and seaware_res_package_rel.guest_id=revenue_comm.revenue_comm_guest_id and seaware_res_package_rel.package_id=revenue_comm.revenue_comm_package_id) 
	full join (select * from hive_schema_stg.seaware_addon_fact where snapshot_date=(select max(snapshot_date) from hive_schema_stg.seaware_addon_fact)) seaware_addon_fact 
		on (revenue_comm.revenue_comm_res_id = seaware_addon_fact.res_id and revenue_comm.revenue_comm_guest_id = seaware_addon_fact.guest_id and revenue_comm.revenue_comm_addon_id = seaware_addon_fact.addon_id) 
	) revenue_comm_etc 		
    left join
        hive_schema_stg.seaware_commission_type_lkp seaware_commission_type_lkp
        on
            revenue_comm_etc.commission_type_skey = seaware_commission_type_lkp.commission_type_id 
	left join hive_schema_stg.seaware_price_area_lkp seaware_price_area_lkp 
		on seaware_price_area_lkp.price_area_id = revenue_comm_etc.price_area_id
	left join hive_schema_stg.seaware_promotion_lkp seaware_promotion_lkp 
		on seaware_promotion_lkp.promotion_id = revenue_comm_etc.promotion_id 
	full join hive_schema_stg.seaware_res_guest_rel seaware_res_guest_rel 
		on 
		revenue_comm_etc.res_id = seaware_res_guest_rel.res_id and revenue_comm_etc.guest_id = seaware_res_guest_rel.guest_id 	
	full join
        (select * from seaware.seaware_reservation_dim where rec_end_dttm='9999-12-31') seaware_reservation_dim 
        on
          seaware_res_guest_rel.res_id = seaware_reservation_dim.res_id  
	full join 
		(select * from hive_schema_stg.seaware_guest_dim where rec_end_dttm='9999-12-31') seaware_guest_dim  
		on 
		seaware_res_guest_rel.guest_id = seaware_guest_dim.guest_id	 
	full join
        (select * from hive_schema_stg.seaware_package_dim where rec_end_dttm='9999-12-31') seaware_package_dim
        on
            revenue_comm_etc.package_id = seaware_package_dim.package_id 
	--full join for sail_dim is failing hence taking left join with reservation_dim and package_dim conditionally. This will list sailings as long as it has either reservations or has packages associated to it. 
	left join
        (select * from hive_schema_stg.seaware_sail_dim where rec_end_dttm='9999-12-31') seaware_sail_dim
        on
            --seaware_reservation_dim.sail_id = seaware_sail_dim.sail_id  		
			case when seaware_reservation_dim.res_id is null then (seaware_sail_dim.src_sail_id = seaware_package_dim.src_sail_id) else (seaware_reservation_dim.sail_id = seaware_sail_dim.sail_id) end --Modify the join seaware_reservation_dim.sail_id = seaware_sail_dim.sail_id to seaware_reservation_dim.src_sail_id = seaware_sail_dim.src_sail_id  
	left join
        seaware.seaware_ship_dim seaware_ship_dim
        on
            seaware_sail_dim.ship_id = seaware_ship_dim.ship_id
	left join hive_schema_stg.seaware_package_dim p_voy on seaware_sail_dim.src_sail_id=p_voy.src_sail_id and  p_voy.package_class='VOYAGE'	and p_voy.rec_end_dttm = '9999-12-31'
 	left join seaware.seaware_group_res_dim seaware_group_res_dim  
		on 
		    seaware_reservation_dim.res_id = seaware_group_res_dim.res_id and seaware_group_res_dim.rec_end_dttm='9999-12-31 00:00:00'
	left join seaware.seaware_group_dim seaware_group_dim  
		on 
		    seaware_group_res_dim.group_id = seaware_group_dim.group_id 
	LEFT JOIN 
       (select f.*,rd.src_res_id, gd.src_guest_id from seaware.seaware_booked_cabin_reservation_dim f  
       INNER JOIN seaware.seaware_reservation_dim rd ON (rd.res_id = f.res_id) 
       INNER JOIN seaware.seaware_guest_dim gd ON (gd.guest_id = f.guest_id)  
       where f.rec_end_dttm='9999-12-31 00:00:00' 
       ) seaware_booked_cabin_reservation_dim   
       on seaware_reservation_dim.src_res_id = seaware_booked_cabin_reservation_dim.src_res_id and seaware_guest_dim.src_guest_id=seaware_booked_cabin_reservation_dim.src_guest_id 
	LEFT JOIN seaware.seaware_cabin_master seaware_cabin_master ON (seaware_booked_cabin_reservation_dim.cabin_id = seaware_cabin_master.cabin_id) 
	left join hive_schema_stg.seaware_addon_lkp seaware_addon_lkp on (revenue_comm_etc.addon_id=seaware_addon_lkp.addon_id)	
	/*left join
        hive_schema_stg.seaware_agency_dim seaware_agency_dim
        on
            case when revenue_fact.agency_id is null then seaware_commission_fact.agency_id else revenue_fact.agency_id end = seaware_agency_dim.agency_id*/ --replace the join with reservation_dim.agency_id
    left join
        hive_schema_stg.crm_agency_master crm_agency_master
        on
            seaware_reservation_dim.seaware_agency_id = crm_agency_master.seaware_agency_id__c and crm_agency_master.rec_end_dttm='9999-12-31 00:00:00' 
    left join
        hive_schema_stg.crm_agent_master crm_agent_master
        on
            seaware_reservation_dim.seaware_agent_id = crm_agent_master.seaware_agent_id__c and crm_agent_master.rec_end_dttm='9999-12-31 00:00:00' 
	left join seaware.seaware_hotel_res_req_dim seaware_hotel_res_req_dim ON (revenue_comm_etc.hotel_res_req_id = seaware_hotel_res_req_dim.hotel_res_req_id and seaware_hotel_res_req_dim.rec_end_dttm='9999-12-31 00:00:00') 
	left join seaware.seaware_hotel_lkp seaware_hotel_lkp ON (seaware_hotel_res_req_dim.hotel_id = seaware_hotel_lkp.hotel_id)	
	left join seaware.seaware_ship_room_request_dim seaware_ship_room_request_dim ON (revenue_comm_etc.ship_room_req_id = seaware_ship_room_request_dim.ship_room_req_id)
	left join seaware.seaware_ship_room_dim seaware_ship_room_dim ON (seaware_ship_room_request_dim.ship_room_id = seaware_ship_room_dim.ship_room_id and seaware_ship_room_dim.rec_end_dttm='9999-12-31 00:00:00') 
    left join seaware.seaware_ship_facility_lkp seaware_ship_facility_lkp ON (seaware_ship_room_dim.facility_id = seaware_ship_facility_lkp.facility_id) 
	left join hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type pt on seaware_package_dim.package_type = pt.package_type
	left join hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_itinerary pi on seaware_package_dim.package_type = pi.package_type
	left join hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type_vendor_link vl on seaware_package_dim.package_type = vl.package_type
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
	left join seaware.hbtb_nbx_sailor_tribes_vw	hbtb_nbx_sailor_tribes_vw 
		on hbtb_nbx_sailor_tribes_vw.seaware_id = seaware_guest_dim.client_id and hbtb_nbx_sailor_tribes_vw.isactive = 'Y' 
	) main
with no schema binding
;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;