-- Function: pt_intg.load_acbs_facility_loan()

-- DROP FUNCTION pt_intg.load_acbs_facility_loan();

CREATE OR REPLACE FUNCTION pt_intg.load_acbs_facility_loan()
  RETURNS text AS
$BODY$
DECLARE
load_status character varying;
stg_count_check_1 int;
stg_count_check_2 int;
stg_count_check_3 int;
stg_count_check_4 int;
stg_count_check_5 int;
stg_count_check_6 int;
stg_count_check_7 int;

BEGIN

/*      Function                :   load_acbs_facility_loan.sql
*	Modified By		:   Naveen Kancham  13 Mar 2017 - B-1781729 Payoff Amount not updated after extension processed - Logic Fix
*	Modified By		:   Naveen Kancham \ Ravi Teja Voleti 20 Mar 2017 - B-1794367 Payoff Amount not updated after extension processed - Logic Fix (DEV Only)   
*   Modified By		:   Ravi Teja Voleti 24 Mar 2017 - B-1826720 : Interest/Principal payments are duplicated for ACBS History records for a loan
*   Modified By		:   Ravi Teja Voleti 17 Apr 2017 - CCPM-10481 : Rebooked tranche showing on UI and included in calculations - Add Loan Status
*   Modified By		:   Ravi Teja Voleti 25 Apr 2017 - CCPM-11065 : Updates not flowing for Inactive tranches - the amounts remain blank
*   Modified By		:   Ravi Teja Voleti 28 Apr 2017 - CCPM-11156 : last_principal_payment_dt and last_interest_payment_dt columns are not getting updated for the tranches in loan table
*   Modified By		:   Ravi Teja Voleti 24 May 2017 - CCPM-11292 : Reduce COF share by Participation Sold - Tranche/loan level
*   Modified By		:   Ravi Teja Voleti 13 Jun 2017 - CCPM - 11732 : Reduce COF share by Participation Sold - Tranche/loan level
*/

stg_count_check_1 := (select count(*) from pt_intg.pt_ca_limits_record);
stg_count_check_2 := (select count(*) from pt_intg.pt_accrual_schedule_seg);
stg_count_check_3 := (select count(*) from pt_intg.pt_cl_funds_due_seg);
stg_count_check_4 := (select count(*) from pt_intg.pt_cash_event_transactions);
stg_count_check_5 := (select count(*) from pt_intg.pt_cl_secondary_billing_seg);
stg_count_check_6 := (select count(*) from pt_intg.pt_cl_prim_billing_repayment_schd);
stg_count_check_7 := (select count(*) from pt_intg.pt_cl_account_balance);

if (stg_count_check_1 <> 0 and stg_count_check_2 <> 0 and stg_count_check_3 <> 0 and stg_count_check_4 <> 0 and stg_count_check_5 <> 0 and stg_count_check_6 <> 0 and stg_count_check_7 <> 0) THEN


/*----------------------- UPDATE TABLE -----------------------*/

update pt_intg.acbs_facility_loan afl
set
last_capone_principal_billed_amt = null,
 last_global_principal_billed_amt = null,
 last_capone_interest_billed_amt = null,
 last_global_interest_billed_amt = null,
 last_interest_billing_dt = null,
 last_principal_billing_dt = null,
 previous_interest_amt_due = null,
 previous_principal_amt_due = null,
modified_dt = current_timestamp
 from (select afl.* from pt_intg.acbs_facility_loan afl 
join pt_intg.acbs_facility af on af.acbs_facility_num = afl.acbs_facility_num and acbs_facility_status_cd = 'A'
AND ge_legacy_loan_num not like '07%') afl_a
where afl.acbs_facility_num = afl_a.acbs_facility_num and afl.obligation_num = afl_a.obligation_num and afl.limit_type_cd = afl_a.limit_type_cd and afl.limit_key_val = afl_a.limit_key_val 
and afl.as_of_dt = afl_a.as_of_dt;

/*----------------------- TEMP TABLE -----------------------*/

create temp table tranche_facility_temp as
select b.status_cd,b.standard_reference_id_type,a.acbs_facility_num,b.obligation_num,a.limit_type_cd,a.limit_key_val,b.obligor_num,a.portfolio_id,b.legal_maturity_dt,b.effective_dt,a.account_owner_num,b.standard_reference_id
from pt_intg.pt_ca_limits_record a 
inner join pt_intg.acbs_facility af on to_number(a.acbs_facility_num, '9999999999')=af.acbs_facility_num
inner join pt_intg.pt_cl_master_record b on b.acbs_facility_num = a.acbs_facility_num 
and a.limit_type_cd = '31'
and b.pricing_template_id = a.limit_key_val
and a.account_owner_num = '00000000'
union
select b.status_cd,b.standard_reference_id_type,a.acbs_facility_num,b.obligation_num,a.limit_type_cd,a.limit_key_val,b.obligor_num,a.portfolio_id,b.legal_maturity_dt,b.effective_dt,a.account_owner_num,b.standard_reference_id
from pt_intg.pt_ca_limits_record a 
inner join pt_intg.acbs_facility af on to_number(a.acbs_facility_num, '9999999999')=af.acbs_facility_num
inner join pt_intg.pt_cl_master_record b on b.acbs_facility_num = a.acbs_facility_num 
inner join pt_intg.pt_cl_misc_cds_info c on b.obligation_num = c.obligation_num
and a.limit_type_cd = '32'
and a.limit_key_val=c.product_group
and a.account_owner_num = '00000000'
union
select b.status_cd,b.standard_reference_id_type,a.acbs_facility_num,b.obligation_num,a.limit_type_cd,a.limit_key_val,b.obligor_num,a.portfolio_id,b.legal_maturity_dt,b.effective_dt,a.account_owner_num,b.standard_reference_id
from pt_intg.pt_ca_limits_record a 
inner join pt_intg.acbs_facility af on to_number(a.acbs_facility_num, '9999999999')=af.acbs_facility_num
inner join pt_intg.pt_cl_master_record b on b.acbs_facility_num = a.acbs_facility_num 
inner join pt_intg.pt_cl_misc_cds_info c on b.obligation_num = c.obligation_num
and a.limit_type_cd = '33'
and a.limit_key_val=c.product_type_code
and a.account_owner_num = '00000000'
union 
select b.status_cd,b.standard_reference_id_type,a.acbs_facility_num,b.obligation_num,a.limit_type_cd,a.limit_key_val,b.obligor_num,a.portfolio_id,b.legal_maturity_dt,b.effective_dt,a.account_owner_num,b.standard_reference_id
from pt_intg.pt_ca_limits_record a 
inner join pt_intg.acbs_facility af on to_number(a.acbs_facility_num, '9999999999')=af.acbs_facility_num
inner join pt_intg.pt_cl_master_record b on b.acbs_facility_num = a.acbs_facility_num 
inner join pt_intg.pt_cl_misc_cds_info c on b.obligation_num = c.obligation_num
and a.limit_type_cd = '00'
and b.pricing_template_id not like '%SWING%'
and b.pricing_template_id not like '%LC%'
and c.product_type_code != '700'
and a.account_owner_num = '00000000'
and not exists (select * from (select a.acbs_facility_num,b.obligation_num,a.limit_type_cd,a.limit_key_val,b.obligor_num,a.portfolio_id,b.legal_maturity_dt,b.effective_dt,a.account_owner_num,b.standard_reference_id
from pt_intg.pt_ca_limits_record a 
inner join pt_intg.acbs_facility af on to_number(a.acbs_facility_num, '9999999999')=af.acbs_facility_num
inner join pt_intg.pt_cl_master_record b on b.acbs_facility_num = a.acbs_facility_num 
and a.limit_type_cd = '31'
and b.pricing_template_id = a.limit_key_val
and a.account_owner_num = '00000000'
union
select a.acbs_facility_num,b.obligation_num,a.limit_type_cd,a.limit_key_val,b.obligor_num,a.portfolio_id,b.legal_maturity_dt,b.effective_dt,a.account_owner_num,b.standard_reference_id
from pt_intg.pt_ca_limits_record a 
inner join pt_intg.acbs_facility af on to_number(a.acbs_facility_num, '9999999999')=af.acbs_facility_num
inner join pt_intg.pt_cl_master_record b on b.acbs_facility_num = a.acbs_facility_num 
inner join pt_intg.pt_cl_misc_cds_info c on b.obligation_num = c.obligation_num
and a.limit_type_cd = '32'
and a.limit_key_val=c.product_group
and a.account_owner_num = '00000000'
union
select a.acbs_facility_num,b.obligation_num,a.limit_type_cd,a.limit_key_val,b.obligor_num,a.portfolio_id,b.legal_maturity_dt,b.effective_dt,a.account_owner_num,b.standard_reference_id
from pt_intg.pt_ca_limits_record a 
inner join pt_intg.acbs_facility af on to_number(a.acbs_facility_num, '9999999999')=af.acbs_facility_num
inner join pt_intg.pt_cl_master_record b on b.acbs_facility_num = a.acbs_facility_num 
inner join pt_intg.pt_cl_misc_cds_info c on b.obligation_num = c.obligation_num
and a.limit_type_cd = '33'
and a.limit_key_val=c.product_type_code
and a.account_owner_num = '00000000') tab where tab.acbs_facility_num=a.acbs_facility_num and tab.obligation_num=b.obligation_num );
	

/*-***********************HISTORY (PRIN)  **********************-*/

create temp table tranche_history_temp as
select distinct
q.status_cd,
to_number(q.acbs_facility_num, '9999999999') acbs_facility_num,
coalesce(q.obligation_num,'99999999') obligation_num,
to_number(q.limit_type_cd, '999999999') limit_type_cd,
q.limit_key_val,
 cast((case when d.last_pricipal_billed_date = '99999999' then '1900-01-01' else coalesce(d.last_pricipal_billed_date,'1900-01-01') end) as date)  as_of_dt,
case when q.standard_reference_id_type='HFSRE' then 
(select min(first_payment_dt) from pt_intg.acbs_facility_loan a
where a.acbs_facility_num=to_number(q.acbs_facility_num, '9999999999')
and a.obligation_num=coalesce(q.obligation_num,'99999999')
and a.limit_type_cd=to_number(q.limit_type_cd, '999999999')
group by acbs_facility_num,obligation_num,limit_type_cd)
else z.first_payment_dt end,
d.last_capone_principal_billed_amt,
d.last_global_principal_billed_amt,
cast((case when d.last_pricipal_billed_date = '99999999' then '1900-01-01' else d.last_pricipal_billed_date end) as date) last_principal_billing_dt,
d.Previous_Principal_Amount_Due previous_principal_amt_due,
q.standard_reference_id ge_legacy_loan_num
from tranche_facility_temp q
/*left outer*/ join (
select obligation_id,portfolio_cd,max(last_global_principal_billed_amt) last_global_principal_billed_amt ,max(last_capone_principal_billed_amt) last_capone_principal_billed_amt,
max(last_pricipal_billed_date) last_pricipal_billed_date ,max(Previous_Principal_Amount_Due) Previous_Principal_Amount_Due
 from (select distinct a.obligation_id,a.portfolio_cd,
((case when a.lender_type_cd='100' then a.amount_billed_this_period else null end)) last_global_principal_billed_amt,
((case when a.lender_type_cd in ('500','600') then a.amount_billed_this_period - coalesce(s.amount_billed_this_period,0.00) else null end)) last_capone_principal_billed_amt,
 a.due_dt  last_pricipal_billed_date,
 a.previous_amt_due  Previous_Principal_Amount_Due
from pt_intg.pt_cl_funds_due_seg a
left join (select tab.obligation_id,tab.portfolio_cd,tab.due_dt,sum(amount_billed_this_period) amount_billed_this_period from (
select distinct a.obligation_id,a.portfolio_cd,a.due_dt,a.account_owner_id ,a.sequence_num, coalesce(amount_billed_this_period,0.00) amount_billed_this_period
from pt_intg.pt_cl_funds_due_seg a 
where a.lender_type_cd in ('700','702') and account_sequence='1' and balance_category_cd = 'PRIN1' 
)tab
group by tab.obligation_id,tab.portfolio_cd,tab.due_dt ) s on s.obligation_id = a.obligation_id and s.portfolio_cd = a.portfolio_cd and s.due_dt = a.due_dt
where
a.account_owner_id='00000000' and
a.account_sequence='1' and
a.lender_type_cd in ('100','500','600')  
and
 a.balance_category_cd = 'PRIN1')a
group by obligation_id,portfolio_cd,last_pricipal_billed_date) d on d.obligation_id = q.obligation_num and d.portfolio_cd = q.portfolio_id
left outer join(select obligation_id,first_payment_dt from (select obligation_id,cast(first_due_dt as date) first_payment_dt, rank() over (partition by obligation_id order by lender_type_cd asc) as rnk
		from pt_intg.pt_cl_prim_billing_repayment_schd a
		where  a.lender_type_cd in ('100','600'))a where  rnk=1) z on z.obligation_id = q.obligation_num
		where
q.account_owner_num = '00000000';
		
		
		/*-***********************HISTORY(PRIN)  UPDATE**********************-*/
		create temp table tranche_history_update_temp as 
select a.* from tranche_history_temp a left outer join pt_intg.acbs_facility_loan b 
on a.acbs_facility_num = b.acbs_facility_num 
and a.obligation_num = b.obligation_num
and a.limit_type_cd = b.limit_type_cd
and a.limit_key_val = b.limit_key_val
and a.as_of_dt = b.as_of_dt
where b.acbs_facility_num is not null;



update pt_intg.acbs_facility_loan
set
first_payment_dt=b.first_payment_dt,
last_capone_principal_billed_amt = b.last_capone_principal_billed_amt,
last_global_principal_billed_amt = b.last_global_principal_billed_amt,
last_principal_billing_dt = b.last_principal_billing_dt,
previous_principal_amt_due = b.previous_principal_amt_due,
ge_legacy_loan_num = b.ge_legacy_loan_num,
modified_dt = current_timestamp
from tranche_history_update_temp b
where acbs_facility_loan.acbs_facility_num = b.acbs_facility_num 
and acbs_facility_loan.obligation_num = b.obligation_num
and acbs_facility_loan.limit_type_cd = b.limit_type_cd
and acbs_facility_loan.limit_key_val = b.limit_key_val
and acbs_facility_loan.as_of_dt = b.as_of_dt;
		
		/*-***********************HISTORY (PRIN) INSERT**********************-*/
		
		create temp table tranche_history_insert_temp as 
select a.* from tranche_history_temp a left outer join pt_intg.acbs_facility_loan b 
on a.acbs_facility_num = b.acbs_facility_num 
and a.obligation_num = b.obligation_num
and a.limit_type_cd = b.limit_type_cd
and a.limit_key_val = b.limit_key_val
and a.as_of_dt = b.as_of_dt
where b.acbs_facility_num is null and a.status_cd in ('1','2','3');

insert into pt_intg.acbs_facility_loan(acbs_facility_num,
obligation_num,
limit_type_cd,
limit_key_val,
as_of_dt,
first_payment_dt,
last_capone_principal_billed_amt,
last_global_principal_billed_amt,
last_principal_billing_dt,
previous_principal_amt_due,
ge_legacy_loan_num,created_dt,modified_dt) 
select acbs_facility_num,
obligation_num,
limit_type_cd,
limit_key_val,
as_of_dt,
first_payment_dt,
last_capone_principal_billed_amt,
last_global_principal_billed_amt,
last_principal_billing_dt,
previous_principal_amt_due,
ge_legacy_loan_num,
current_timestamp,current_timestamp from tranche_history_insert_temp;

        /* ***********************HISTORY (INT) ****************** */

create temp table tranche_history_temp_int as
select distinct
q.status_cd,
to_number(q.acbs_facility_num, '9999999999') acbs_facility_num,
coalesce(q.obligation_num,'99999999') obligation_num,
to_number(q.limit_type_cd, '999999999') limit_type_cd,
q.limit_key_val,
 cast((case when d.last_interest_billed_date = '99999999' then '1900-01-01' else coalesce(d.last_interest_billed_date,'1900-01-01') end) as date)  as_of_dt,
case when q.standard_reference_id_type='HFSRE' then 
(select min(first_payment_dt) from pt_intg.acbs_facility_loan a
where a.acbs_facility_num=to_number(q.acbs_facility_num, '9999999999')
and a.obligation_num=coalesce(q.obligation_num,'99999999')
and a.limit_type_cd=to_number(q.limit_type_cd, '999999999')
group by acbs_facility_num,obligation_num,limit_type_cd/*,limit_key_val*/)
else z.first_payment_dt end,
d.last_capone_interest_billed_amt,
d.last_global_interest_billed_amt,
cast((case when d.last_interest_billed_date = '99999999' then '1900-01-01' else  d.last_interest_billed_date end) as date) last_interest_billing_dt,
d.Previous_Interest_Amount_Due previous_interest_amt_due,
q.standard_reference_id ge_legacy_loan_num
from tranche_facility_temp q
/*left outer*/ join (select obligation_id,portfolio_cd,max(last_global_interest_billed_amt) last_global_interest_billed_amt ,max(last_capone_interest_billed_amt) last_capone_interest_billed_amt,
max(last_interest_billed_date) last_interest_billed_date ,max(Previous_Interest_Amount_Due) Previous_Interest_Amount_Due
 from (select distinct a.obligation_id,a.portfolio_cd,
((case when a.lender_type_cd='100' then a.amount_billed_this_period else null end)) last_global_interest_billed_amt,
((case when a.lender_type_cd in ('500','600') then a.amount_billed_this_period - coalesce(s.amount_billed_this_period,0.00) else null end)) last_capone_interest_billed_amt,
 a.due_dt  last_interest_billed_date,
 a.previous_amt_due  Previous_Interest_Amount_Due
from pt_intg.pt_cl_funds_due_seg a
left join (select tab.obligation_id,tab.portfolio_cd,tab.due_dt,sum(amount_billed_this_period) amount_billed_this_period from (
select distinct a.obligation_id,a.portfolio_cd,a.due_dt,a.account_owner_id ,a.sequence_num, coalesce(amount_billed_this_period,0.00) amount_billed_this_period
from pt_intg.pt_cl_funds_due_seg a
where a.lender_type_cd in ('700','702') and account_sequence='1' and balance_category_cd = 'INT' 
)tab
group by tab.obligation_id,tab.portfolio_cd,tab.due_dt ) s on s.obligation_id = a.obligation_id and s.portfolio_cd = a.portfolio_cd and s.due_dt = a.due_dt
where
a.account_owner_id='00000000' and
a.account_sequence='1' and
a.lender_type_cd in ('100','500','600')  
and
a.balance_category_cd = 'INT')a
group by a.obligation_id,a.portfolio_cd,a.last_interest_billed_date) d on d.obligation_id = q.obligation_num and d.portfolio_cd = q.portfolio_id
left outer join(select obligation_id,first_payment_dt from (select obligation_id,cast(first_due_dt as date) first_payment_dt, rank() over (partition by obligation_id order by lender_type_cd asc) as rnk
		from pt_intg.pt_cl_prim_billing_repayment_schd a
		where  a.lender_type_cd in ('100','600'))a where  rnk=1) z on z.obligation_id = q.obligation_num
		where
q.account_owner_num = '00000000';


		/*-***********************HISTORY  (INT) UPDATE**********************-*/
		create temp table tranche_history_update_temp_int as 
select a.* from tranche_history_temp_int a left outer join pt_intg.acbs_facility_loan b 
on a.acbs_facility_num = b.acbs_facility_num 
and a.obligation_num = b.obligation_num
and a.limit_type_cd = b.limit_type_cd
and a.limit_key_val = b.limit_key_val
and a.as_of_dt = b.as_of_dt
where b.acbs_facility_num is not null;



update pt_intg.acbs_facility_loan
set
first_payment_dt=b.first_payment_dt,
last_capone_interest_billed_amt = b.last_capone_interest_billed_amt,
last_global_interest_billed_amt = b.last_global_interest_billed_amt,
last_interest_billing_dt = b.last_interest_billing_dt,
previous_interest_amt_due = b.previous_interest_amt_due,
ge_legacy_loan_num = b.ge_legacy_loan_num,
modified_dt = current_timestamp
from tranche_history_update_temp_int b
where acbs_facility_loan.acbs_facility_num = b.acbs_facility_num 
and acbs_facility_loan.obligation_num = b.obligation_num
and acbs_facility_loan.limit_type_cd = b.limit_type_cd
and acbs_facility_loan.limit_key_val = b.limit_key_val
and acbs_facility_loan.as_of_dt = b.as_of_dt;
		
		/*-***********************HISTORY  (INT) INSERT**********************-*/
		
		create temp table tranche_history_insert_temp_int as 
select a.* from tranche_history_temp_int a left outer join pt_intg.acbs_facility_loan b 
on a.acbs_facility_num = b.acbs_facility_num 
and a.obligation_num = b.obligation_num
and a.limit_type_cd = b.limit_type_cd
and a.limit_key_val = b.limit_key_val
and a.as_of_dt = b.as_of_dt
where b.acbs_facility_num is null and a.status_cd in ('1','2','3');

insert into pt_intg.acbs_facility_loan(acbs_facility_num,
obligation_num,
limit_type_cd,
limit_key_val,
as_of_dt,
first_payment_dt,
last_capone_interest_billed_amt,
last_global_interest_billed_amt,
last_interest_billing_dt,
previous_interest_amt_due,
ge_legacy_loan_num,created_dt,modified_dt) 
select acbs_facility_num,
obligation_num,
limit_type_cd,
limit_key_val,
as_of_dt,
first_payment_dt,
last_capone_interest_billed_amt,
last_global_interest_billed_amt,
last_interest_billing_dt,
previous_interest_amt_due,
ge_legacy_loan_num,
current_timestamp,current_timestamp from tranche_history_insert_temp_int;


/*-****************************GO FORWARD DATA***********************************-*/
create temp table tranche_temp as
select distinct
to_number(q.acbs_facility_num, '9999999999') acbs_facility_num,
coalesce(q.obligation_num,'99999999') obligation_num,
to_number(q.limit_type_cd, '999999999') limit_type_cd,
q.limit_key_val,
(case 
	when coalesce(d.last_pricipal_billed_date,'1900-01-01') > coalesce(d.last_interest_billed_date,'1900-01-01') then cast((case when d.last_pricipal_billed_date = '99999999' then '1900-01-01' else d.last_pricipal_billed_date end) as date)
	when coalesce(d.last_interest_billed_date,'1900-01-01') > coalesce(d.last_pricipal_billed_date,'1900-01-01') then cast((case when d.last_interest_billed_date = '99999999' then '1900-01-01' else  d.last_interest_billed_date end) as date)
	when d.last_pricipal_billed_date = d.last_interest_billed_date  then cast((case when d.last_pricipal_billed_date = '99999999' then '1900-01-01' else d.last_pricipal_billed_date end) as date)
	else current_date
end) as_of_dt,
case when q.standard_reference_id_type='HFSRE' then 
(select min(first_payment_dt) from pt_intg.acbs_facility_loan a
where a.acbs_facility_num=to_number(q.acbs_facility_num, '9999999999')
and a.obligation_num=coalesce(q.obligation_num,'99999999')
and a.limit_type_cd=to_number(q.limit_type_cd, '999999999')
--and a.limit_key_val=q.limit_key_val
 group by acbs_facility_num,obligation_num,limit_type_cd/*,limit_key_val*/)
else z.first_payment_dt end,
c.current_accruing_rate all_in_rate,
c.base_rate base_rate,
d.last_capone_principal_billed_amt,
d.last_global_principal_billed_amt,
c.minimum_amt_of_accruals_to_generate floor,
c.index_cd,
d.last_capone_interest_billed_amt,
d.last_global_interest_billed_amt,
cast((case when d.last_interest_billed_date = '99999999' then '1900-01-01' else  d.last_interest_billed_date end) as date) last_interest_billing_dt,
cast((case when d.last_pricipal_billed_date = '99999999' then '1900-01-01' else d.last_pricipal_billed_date end) as date) last_principal_billing_dt,
cast((case when e.process_dt_int = '99999999' then '1900-01-01' else e.process_dt_int end) as date) last_interest_payment_dt,
cast((case when e.process_dt_null = '99999999' then '1900-01-01' else e.process_dt_null end) as date) last_principal_payment_dt,
cast((case when q.legal_maturity_dt = '99999999' then '1900-01-01' else q.legal_maturity_dt end) as date) maturity_dt,
cast ((case 
	when  g.first_due_dt = h.next_due_dt_prim then g.next_due_dt_scndry
	else  h.next_due_dt_prim
end) as date) next_payment_due_dt,
cast((case when q.effective_dt = '99999999' then '1900-01-01' else q.effective_dt end) as date) origination_dt,
j.outstanding_capone_re_debt,
j.outstanding_global_re_debt,
(case when g.frequency_cd is null then h.schedule_freq_cd else g.frequency_cd end) payment_frequency,
d.Previous_Interest_Amount_Due previous_interest_amt_due,
d.Previous_Principal_Amount_Due previous_principal_amt_due,
c.spread_rate spread,
q.standard_reference_id ge_legacy_loan_num
from tranche_facility_temp q
left outer join (select distinct x.obligation_num,x.current_accruing_rate,x.base_rate,x.minimum_amt_of_accruals_to_generate,x.spread_rate,
(case when x.accrual_class_cd = 'F' then 'Fixed'
     when x.accrual_class_cd = 'I' then x.floating_rate_index_cd
     else null
end) index_cd
from pt_intg.pt_accrual_schedule_seg x,
(select obligation_num,max(current_accruing_rate) max_current_accruing_rate 
from pt_intg.pt_accrual_schedule_seg 
where account_owner_num = '00000000'
and lender_type_cd in ('100','600')
and income_class_cd = 'IN'
and balance_category_cd = 'INT'
and accrual_Schedule_status_cd = '1'
and schedule_type_cd = 'A'
and accrual_class_cd in ( 'I','F')
group by 1) y
where x.obligation_num = y.obligation_num
and x.current_accruing_rate = y.max_current_accruing_rate
and account_owner_num = '00000000'
and lender_type_cd in ('100','600')
and income_class_cd = 'IN'
and balance_category_cd = 'INT'
and accrual_Schedule_status_cd = '1'
and schedule_type_cd = 'A'
and accrual_class_cd in ( 'I','F')) c on q.obligation_num = c.obligation_num
left outer join (select a.obligation_id,a.portfolio_cd,
max((case when a.balance_category_cd = 'INT' and a.lender_type_cd='100' then a.amount_billed_this_period else null end)) last_global_interest_billed_amt,
max((case when a.balance_category_cd =  'INT' and a.lender_type_cd in ('500','600') then a.amount_billed_this_period else null end)) last_capone_interest_billed_amt,
max((case when a.balance_category_cd = 'PRIN1' and a.lender_type_cd='100' then a.amount_billed_this_period else null end)) last_global_principal_billed_amt,
max((case when a.balance_category_cd =  'PRIN1' and a.lender_type_cd in ('500','600') then a.amount_billed_this_period else null end)) last_capone_principal_billed_amt,
max((case when a.balance_category_cd = 'INT' then b.max_due_dt else null end)) last_interest_billed_date,
max((case when a.balance_category_cd = 'PRIN1' then b.max_due_dt else null end)) last_pricipal_billed_date,
max((case when a.balance_category_cd = 'INT' then  previous_amt_due else null end )) Previous_Interest_Amount_Due,
max((case when a.balance_category_cd = 'PRIN1' then  previous_amt_due else null end )) Previous_Principal_Amount_Due
from pt_intg.pt_cl_funds_due_seg a,
(select obligation_id,portfolio_cd,balance_category_cd,max(due_dt) max_due_dt from pt_intg.pt_cl_funds_due_seg where
account_owner_id='00000000' and
account_sequence='1' and
lender_type_cd in ('100','500','600') group by 1,2,3) b
where a.obligation_id = b.obligation_id
and a.portfolio_cd = b.portfolio_cd
and a.balance_category_cd = b.balance_category_cd
and a.due_dt = b.max_due_dt
and a.account_owner_id = '00000000'
and a.account_sequence = '1'
and a.lender_type_cd in ('100','500','600') group by a.obligation_id,a.portfolio_cd) d on d.obligation_id = q.obligation_num and d.portfolio_cd = q.portfolio_id
left outer join (select obligation_num,
max(case when balance_category_cd = 'INT' then process_dt else null end) process_dt_int,
max(case when balance_category_cd = '' then process_dt else null end) process_dt_null
from pt_intg.pt_cash_event_transactions 
where sequence_num = '1'
and lender_type_cd in ('100','600')
group by 1) e on e.obligation_num = q.obligation_num 
left outer join (select obligation_id,next_due_dt_scndry,first_due_dt, max(frequency_cd) frequency_cd from (
		select distinct obligation_id,frequency_cd,next_due_dt_scndry,first_due_dt,lender_type_cd from (
		select distinct a.obligation_id,a.frequency_cd,a.next_due_dt next_due_dt_scndry,a.first_due_dt,a.lender_type_cd,
		rank() OVER(partition by a.obligation_id order by a.lender_type_cd asc, a.next_due_dt desc) AS rank  from pt_intg.pt_cl_secondary_billing_seg a,
		(select obligation_id,max(next_due_dt) next_due_dt  from pt_intg.pt_cl_secondary_billing_seg where primary_bill_replacement = 'Y' and lender_type_cd in ('100','500','600')
		group by 1) b
		where a.obligation_id = b.obligation_id
			and a.next_due_dt = b.next_due_dt
			and a.primary_bill_replacement = 'Y'
			and a.lender_type_cd in ('100','500','600')
		group by a.obligation_id,a.frequency_cd,a.next_due_dt,a.first_due_dt,a.lender_type_cd) tab 
			where rank=1 ) tab1 group by obligation_id,next_due_dt_scndry,first_due_dt
		)g on g.obligation_id = q.obligation_num
left outer join (select obligation_id,next_due_dt_prim, max(schedule_freq_cd) schedule_freq_cd from (
		select distinct obligation_id,schedule_freq_cd,next_due_dt_prim from (
		select distinct a.obligation_id,a.schedule_freq_cd,a.next_due_dt next_due_dt_prim,
		rank() OVER(partition by a.obligation_id order by a.lender_type_cd asc, a.next_due_dt desc) from pt_intg.pt_cl_prim_billing_repayment_schd a,
		(select obligation_id,max(next_due_dt) max_next_due_dt from pt_intg.pt_cl_prim_billing_repayment_schd group by 1) b
		where a.obligation_id = b.obligation_id
			and a.next_due_dt = b.max_next_due_dt 
			and a.lender_type_cd in ('100','500','600')
			group by a.obligation_id,a.schedule_freq_cd,a.next_due_dt,a.lender_type_cd ) tab
			where rank=1) tab1 group by obligation_id,next_due_dt_prim
		)  h on h.obligation_id = q.obligation_num
left outer join (select a.obligation_num,a.portfolio_id,a.obligor_num,
max((case when lender_type_cd in ('500','600') then a.current_general_ledger_balance - coalesce(s.current_general_ledger_balance,0.00) else null end)) outstanding_capone_re_debt,
max((case when a.lender_type_cd in ('100') then a.current_general_ledger_balance else null end)) outstanding_global_re_debt
from pt_intg.pt_cl_account_balance a
left join (select tab.obligation_num,tab.portfolio_id,tab.obligor_num,tab.effective_dt,sum(current_general_ledger_balance) current_general_ledger_balance
from (
select distinct a.obligation_num,a.portfolio_id,a.obligor_num,a.effective_dt, coalesce(current_general_ledger_balance,0.00) current_general_ledger_balance
from pt_intg.pt_cl_account_balance a
left join pt_intg.pt_cl_master_record cmr on a.obligation_num = cmr.obligation_num
left join pt_intg.pt_ca_limits_record clr on clr.acbs_facility_num = cmr.acbs_facility_num 
where a.lender_type_cd in ('700','702') and sequence_num = '1' and clr.limit_type_cd = '50')tab
group by tab.obligation_num,tab.portfolio_id,tab.obligor_num,tab.effective_dt) s on s.obligation_num = a.obligation_num and s.portfolio_id = a.portfolio_id 
and s.obligor_num = a.obligor_num and a.effective_dt = s.effective_dt
where a.sequence_num = '1' 
and a.lender_type_cd in ('100','500','600')
group by 1,2,3) j on j.obligation_num = q.obligation_num  and j.obligor_num = q.obligor_num
left outer join(select obligation_id,first_payment_dt from (select obligation_id,cast(first_due_dt as date) first_payment_dt, rank() over (partition by obligation_id order by lender_type_cd asc) as rnk
		from pt_intg.pt_cl_prim_billing_repayment_schd a
		where  a.lender_type_cd in ('100','600'))a where  rnk=1) z on z.obligation_id = q.obligation_num
where
q.account_owner_num = '00000000';



/*-***********************GO-FORWARD UPDATE **********************-*/	

create temp table tranche_update_temp as 
select a.* from tranche_temp a left outer join pt_intg.acbs_facility_loan b 
on a.acbs_facility_num = b.acbs_facility_num 
and a.obligation_num = b.obligation_num
and a.limit_type_cd = b.limit_type_cd
and a.limit_key_val = b.limit_key_val
and a.as_of_dt = b.as_of_dt
where b.acbs_facility_num is not null;


update pt_intg.acbs_facility_loan
set
first_payment_dt=b.first_payment_dt,
all_in_rate = b.all_in_rate,
base_rate = b.base_rate,
floor = b.floor,
index_cd = b.index_cd,
maturity_dt = b.maturity_dt,
next_payment_due_dt = b.next_payment_due_dt,
origination_dt = b.origination_dt,
outstanding_capone_re_debt = b.outstanding_capone_re_debt,
outstanding_global_re_debt = b.outstanding_global_re_debt,
payment_frequency = b.payment_frequency,
spread = b.spread,
ge_legacy_loan_num = b.ge_legacy_loan_num,
modified_dt = current_timestamp,
last_interest_payment_dt = CASE WHEN b.last_interest_payment_dt IS NULL OR b.last_interest_payment_dt::text = ''::text THEN acbs_facility_loan.last_interest_payment_dt ELSE b.last_interest_payment_dt END,
last_principal_payment_dt = CASE WHEN b.last_principal_payment_dt IS NULL OR b.last_principal_payment_dt::text = ''::text THEN acbs_facility_loan.last_principal_payment_dt ELSE b.last_principal_payment_dt END
from tranche_update_temp b
where acbs_facility_loan.acbs_facility_num = b.acbs_facility_num 
and acbs_facility_loan.obligation_num = b.obligation_num
and acbs_facility_loan.limit_type_cd = b.limit_type_cd
and acbs_facility_loan.limit_key_val = b.limit_key_val
and acbs_facility_loan.as_of_dt  = b.as_of_dt;



		
	
--drop temp tables

drop table tranche_temp;
drop table tranche_update_temp;
drop table tranche_facility_temp;
drop table tranche_history_temp;
drop table tranche_history_insert_temp;
drop table tranche_history_update_temp;
drop table tranche_history_update_temp_int;
drop table tranche_history_temp_int;
drop table tranche_history_insert_temp_int;


----Update status_cd

update pt_intg.acbs_facility_loan afl
set
status_cd = tab.status_cd,
modified_dt = current_timestamp
 from (select cmr.status_cd,afl.acbs_facility_num,afl.obligation_num,afl.limit_type_cd,
afl.limit_key_val,afl.as_of_dt  from pt_intg.acbs_facility_loan afl
join pt_intg.pt_cl_master_record cmr on afl.obligation_num = cmr.obligation_num and afl.acbs_facility_num = cast(cmr.acbs_facility_num  as bigint)
and coalesce(afl.status_cd,'N/A') <> coalesce(cmr.status_cd,'N/A')) tab
where tab.acbs_facility_num = afl.acbs_facility_num and tab.obligation_num = afl.obligation_num and tab.limit_type_cd = afl.limit_type_cd and 
tab.limit_key_val = afl.limit_key_val and tab.as_of_dt = afl.as_of_dt;

--Analyze Tables

analyze pt_intg.acbs_facility_loan;
	
load_status := 'Load Completed';
return  load_status;

else load_status := 'Load Incomplete due to missing data in staging table';
return load_status;

END IF;

EXCEPTION WHEN OTHERS THEN
	RAISE EXCEPTION 'Error while adding acbs tranche. SQL Error Message: %, %', SQLSTATE, SQLERRM;
	RETURN FALSE;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION pt_intg.load_acbs_facility_loan()
  OWNER TO pt_owner;