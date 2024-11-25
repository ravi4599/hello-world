create or replace view UDP_AUDIT_BV(
	RDS_TABLE,
	SRC_SYSTEM,
	LOADID,
	SRC_COUNT,
	TGT_COUNT,
	AUDIT_SUMMARY
) as

select *, case
when (SRC_COUNT - TGT_COUNT) <> 0 then 'Not Matching'
when (SRC_COUNT is null) or (TGT_COUNT is null) then 'Record not complete'
else 'Matching'
end as AUDIT_SUMMARY
from(
select UPPER(TABLE_NAME) as INPUT_TABLE, UPPER(SRC_SYSTEM) as SRC_SYSTEM, LOADID, 
        to_number(sum(ADLS_COUNT)) as SRC_COUNT,
        to_number(sum(RDS_COUNT)) as TGT_COUNT
        from ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS
        group by 1,2,3
        order by 3 desc
)a;