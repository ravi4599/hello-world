create or replace
view shipdw.ship_entertainment_sailor_summary_fact as
select
	voyage_skey,
	voyage_number,
	ship_code,
	voyage_start_date,
	voyage_length,
	voyage_description,
	cabin_category,
	guest_type,
	sailor_tribe,
	sailor_subtribe,
	bain_segment,
	person_age_range,
	person_gender,
	person_country_of_residence,
	address_country,
	address_state,
	person_household_income,
	sum(person_count) as person_count
from
	shipdw.ship_sailor_summary_fact main
where
	main.revenue_group = 'Entertainment'
group by
	voyage_skey,
	voyage_number,
	ship_code,
	voyage_start_date,
	voyage_length,
	voyage_description,
	cabin_category,
	guest_type,
	sailor_tribe,
	sailor_subtribe,
	bain_segment,
	person_age_range,
	person_gender,
	person_country_of_residence,
	address_country,
	address_state,
	person_household_income with no schema binding;