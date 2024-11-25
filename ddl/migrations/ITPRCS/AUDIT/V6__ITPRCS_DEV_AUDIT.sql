---Alter table add columns Domain and UDP Version
alter table if exists UDP_AUDIT_TRANS_BKP
add 
	DOMAIN VARCHAR,
	UDP_VERSION VARCHAR;
	