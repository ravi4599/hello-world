create TABLE IF NOT EXISTS CUST_EMAIL_PLR cluster by (BrandId, CdmLoadDate)(
	MEMBERID VARCHAR(16777216),
	EMAILID VARCHAR(16777216),
	BRANDID VARCHAR(16777216),
	EMAILOPTOUTFLAG VARCHAR(16777216),
	LOADEDDATE VARCHAR(16777216),
	MODIFIEDDATE VARCHAR(16777216),
	PRIVACY VARCHAR(16777216),
	CDMLOADDATE VARCHAR(16777216),
	INSPIREID VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS CUST_MOBILE_PLR cluster by (BrandId, CdmLoadDate)(
	MEMBERID VARCHAR(16777216),
	MOBILENUMBER VARCHAR(16777216),
	MOBILEDEVICEID VARCHAR(16777216),
	PUSHNOTIFICATIONOPTIN VARCHAR(16777216),
	SMSOPTIN VARCHAR(16777216),
	LOADEDDATE VARCHAR(16777216),
	MODIFIEDDATE VARCHAR(16777216),
	PRIVACY VARCHAR(16777216),
	BRANDID VARCHAR(16777216),
	CDMLOADDATE VARCHAR(16777216),
	INSPIREID VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS CUST_PROFILE_PLR cluster by (BrandId, CdmLoadDate)(
	MEMBERID VARCHAR(16777216),
	FIRSTNAME VARCHAR(16777216),
	LASTNAME VARCHAR(16777216),
	DOB VARCHAR(16777216),
	CUSTOMERSTATUS VARCHAR(16777216),
	MARITALSTATUS VARCHAR(16777216),
	CHILDRENNUMBER VARCHAR(16777216),
	INCOME VARCHAR(16777216),
	HOUSEHOLDID VARCHAR(16777216),
	DELIVERABILITYSTATUS VARCHAR(16777216),
	IGNOREFRAUDSUSPENDFLAG VARCHAR(16777216),
	LOADEDDATE VARCHAR(16777216),
	MODIFIEDDATE VARCHAR(16777216),
	BIRTHMONTH VARCHAR(16777216),
	BIRTHYEAR VARCHAR(16777216),
	ISBIRTHDATEIMPLIED VARCHAR(16777216),
	PRIVACY VARCHAR(16777216),
	BRANDID VARCHAR(16777216),
	CDMLOADDATE VARCHAR(16777216),
	INSPIREID VARCHAR(16777216),
	AGENCYID VARCHAR(16777216),
	AGENCYSTATUS VARCHAR(16777216),
	INSPIRECUSTOMERTYPE VARCHAR(16777216),
	MDMID VARCHAR(16777216),
	ISMDMIDDELETED VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
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
create TABLE IF NOT EXISTS TRAN_LOYALTY_DISCOUNT_PLR cluster by (BrandId, CdmLoadDate)(
	TRANSACTIONID VARCHAR(16777216),
	TRANDISCOUNTID VARCHAR(16777216),
	OFFERCODE VARCHAR(16777216),
	SOURCEITEMID VARCHAR(16777216),
	DISCOUNTAMOUNT VARCHAR(16777216),
	DISCOUNTDESC VARCHAR(16777216),
	BRANDID VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	CDMLOADDATE VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TRAN_ORDERLINE_PLR cluster by (BrandId, CdmLoadDate)(
	ORDERID VARCHAR(16777216),
	ORDERLINEID VARCHAR(16777216),
	CHANNELID VARCHAR(16777216),
	MDMITEMID VARCHAR(16777216),
	MDMPARENTITEMID VARCHAR(16777216),
	BUSINESSDATE VARCHAR(16777216),
	TIMEKEY VARCHAR(16777216),
	STOREID VARCHAR(16777216),
	EMPLOYEEID VARCHAR(16777216),
	REVENUECENTER VARCHAR(16777216),
	REGISTERID VARCHAR(16777216),
	TAXID VARCHAR(16777216),
	SEATNUMBER VARCHAR(16777216),
	GROSSQUANTITY VARCHAR(16777216),
	PRICE FLOAT,
	DISCOUNTPRICE VARCHAR(16777216),
	GROSSAMOUNT FLOAT,
	NETAMOUNT FLOAT,
	TAXAMOUNT FLOAT,
	INCLUSIVETAX FLOAT,
	ISCLEARED VARCHAR(16777216),
	ISDELETED VARCHAR(16777216),
	ISVOIDED VARCHAR(16777216),
	ISINVENTORY VARCHAR(16777216),
	ISDISCOUNTED VARCHAR(16777216),
	MODIFIERID VARCHAR(16777216),
	TAXEXEMPTID VARCHAR(16777216),
	MANAGERID VARCHAR(16777216),
	BRANDID VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	CDMLOADDATE VARCHAR(16777216),
	LOADTYPE VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TRAN_ORDER_PLR cluster by (BrandId, CdmLoadDate)(
	ORDERID VARCHAR(16777216),
	EMPLOYEEID VARCHAR(16777216),
	BUSINESSDATE VARCHAR(16777216),
	STOREID VARCHAR(16777216),
	CHECKNUMBER VARCHAR(16777216),
	OPENEDTIME VARCHAR(16777216),
	CLOSEDTIME VARCHAR(16777216),
	ORDERNAME VARCHAR(16777216),
	GROSSQUANTITY VARCHAR(16777216),
	GROSSAMOUNT FLOAT,
	DISCOUNTAMOUNT FLOAT,
	NETAMOUNT FLOAT,
	SURCHARGEAMOUNT FLOAT,
	TAXAMOUNT FLOAT,
	PAYMENTAMOUNT FLOAT,
	GRATUITY VARCHAR(16777216),
	CUSTOMERNAME VARCHAR(16777216),
	CUSTOMERID VARCHAR(16777216),
	LOYALTYNUMBER VARCHAR(16777216),
	FIRSTSENDTIME VARCHAR(16777216),
	ISCLOSED VARCHAR(16777216),
	ISFUTUREORDER VARCHAR(16777216),
	ISVOID VARCHAR(16777216),
	ISREFUND VARCHAR(16777216),
	ISTAXEXEMPT VARCHAR(16777216),
	GUESTCOUNT VARCHAR(16777216),
	CDMLOADDATE VARCHAR(16777216),
	BRANDID VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	TIMEKEY VARCHAR(16777216),
	RESTAURANTKEY VARCHAR(16777216),
	LOADTYPE VARCHAR(16777216),
	INSPIREID VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TRG_MKTG_MEMBER_PLR cluster by (BrandId, CdmLoadDate)(
	MEMBERID VARCHAR(16777216),
	MEMBERCARDNUMBER VARCHAR(16777216),
	LOYALTYPROGRAMID VARCHAR(16777216),
	CLOSESTSTOREID VARCHAR(16777216),
	ENROLLSTARTDATE VARCHAR(16777216),
	ENROLLMENTCHANNEL VARCHAR(16777216),
	POINTBALANCE VARCHAR(16777216),
	MEMBERSHIPSTATUS VARCHAR(16777216),
	POINTSEXPIREDATE VARCHAR(16777216),
	UNSUBSCRIBEDATE VARCHAR(16777216),
	LASTLOGINDATE VARCHAR(16777216),
	LASTSTATUSCHANGEDATE VARCHAR(16777216),
	PROFILECOMPLETEDSTATUS VARCHAR(16777216),
	PROFILECOMPLETIONDATE VARCHAR(16777216),
	LOADEDDATE VARCHAR(16777216),
	MODIFIEDDATE VARCHAR(16777216),
	PRIVACY VARCHAR(16777216),
	SUBSCRIBERKEY VARCHAR(16777216),
	SUBSCRIBERSOURCENAME VARCHAR(16777216),
	BRANDID VARCHAR(16777216),
	CDMLOADDATE VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
CREATE PROCEDURE IF NOT EXISTS CUST_EMAIL_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES("DB_PARAM" VARCHAR(16777216), "SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_email_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_email_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_email_plr(
                                        MemberId,
                                        EmailId,
                                        BrandId,
                                        EmailOptoutFlag,
                                        LoadedDate,
                                        ModifiedDate,
                                        Privacy,
                                        CdmLoadDate,
                                        InspireId,
                                        Source,
                                        Filename

                              )
                          SELECT
                                delta_table.MemberId,
                                delta_table.EmailId,
                                delta_table.BrandId,
                                delta_table.EmailOptoutFlag,
                                delta_table.LoadedDate,
                                delta_table.ModifiedDate,
                                delta_table.Privacy,
                                delta_table.CdmLoadDate,
                                delta_table.InspireId,
                                delta_table.Source,
                                delta_table.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `
                                                    + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1
                                    )`;

    var insert_stream_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_email_plr(
                                MemberId,
                                EmailId,
                                BrandId,
                                EmailOptoutFlag,
                                LoadedDate,
                                ModifiedDate,
                                Privacy,
                                CdmLoadDate,
                                InspireId,
                                Source,
                                Filename
                              )
                          SELECT
                                delta_stream.MemberId,
                                delta_stream.EmailId,
                                delta_stream.BrandId,
                                delta_stream.EmailOptoutFlag,
                                delta_stream.LoadedDate,
                                delta_stream.ModifiedDate,
                                delta_stream.Privacy,
                                delta_stream.CdmLoadDate,
                                delta_stream.InspireId,
                                delta_stream.Source,
                                delta_stream.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr_delta_stream delta_stream
                          WHERE EXISTS(
                                        select
                                           log_stream.add_file
                                        from
                                           (
                                              select
                                                 add_file
                                              from `
                                                 + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr_delta_log_insert_stream
                                              where
                                                 add_file is not null and filename > :1
                                           )
                                           log_stream
                                        where
                                           log_stream.add_file = delta_stream.filename )`;

    var delete_json_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_email_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var stream_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
       var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_stream_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_stream_data,
                binds: [stream_search]
                }
        );
        insert_stream_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [stream_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS CUST_MOBILE_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES("DB_PARAM" VARCHAR(16777216), "SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_mobile_plr(
                                      MemberId,
                                      MobileNumber,
                                      MobileDeviceId,
                                      PushNotificationOptIn,
                                      SMSOptIn,
                                      LoadedDate,
                                      ModifiedDate ,
                                      Privacy,
                                      BrandId,
                                      CdmLoadDate,
                                      InspireId,
                                      Filename
                              )
                          SELECT
                              delta_table.MemberId,
                              delta_table.MobileNumber,
                              delta_table.MobileDeviceId,
                              delta_table.PushNotificationOptIn,
                              delta_table.SMSOptIn,
                              delta_table.LoadedDate,
                              delta_table.ModifiedDate ,
                              delta_table.Privacy,
                              delta_table.BrandId,
                              delta_table.CdmLoadDate,
                              delta_table.InspireId,
                              delta_table.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `
                                                    + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1

                                    )`;

    var insert_stream_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_mobile_plr(
                                  MemberId,
                                  MobileNumber,
                                  MobileDeviceId,
                                  PushNotificationOptIn,
                                  SMSOptIn,
                                  LoadedDate,
                                  ModifiedDate ,
                                  Privacy,
                                  BrandId,
                                  CdmLoadDate,
                                  InspireId,
                                  Filename
                              )
                          SELECT
                                  delta_stream.MemberId,
                                  delta_stream.MobileNumber,
                                  delta_stream.MobileDeviceId,
                                  delta_stream.PushNotificationOptIn,
                                  delta_stream.SMSOptIn,
                                  delta_stream.LoadedDate,
                                  delta_stream.ModifiedDate ,
                                  delta_stream.Privacy,
                                  delta_stream.BrandId,
                                  delta_stream.CdmLoadDate,
                                  delta_stream.InspireId,
                                  delta_stream.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta_stream delta_stream
                          WHERE EXISTS(
                                        select
                                           log_stream.add_file
                                        from
                                           (
                                              select
                                                 add_file
                                              from `
                                                 + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta_log_insert_stream
                                              where
                                                 add_file is not null and filename > :1
                                           )
                                           log_stream
                                        where
                                           log_stream.add_file = delta_stream.filename )`;

    var delete_json_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_mobile_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var stream_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
        var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_stream_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_stream_data,
                binds: [stream_search]
                }
        );
        insert_stream_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [stream_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS CUST_PROFILE_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES("DB_PARAM" VARCHAR(16777216), "SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_profile_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_profile_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_profile_plr(
                                      MemberId,
                                      FirstName,
                                      LastName,
                                      DOB,
                                      CustomerStatus,
                                      MaritalStatus ,
                                      ChildrenNumber,
                                      Income,
                                      HouseHoldId,
                                      DeliverabilityStatus,
                                      IgnoreFraudSuspendFlag,
                                      LoadedDate,
                                      ModifiedDate,
                                      BirthMonth,
                                      BirthYear,
                                      IsBirthDateImplied,
                                      Privacy,
                                      BrandId,
                                      CdmLoadDate,
                                      InspireId,
                                      AgencyId,
                                      AgencyStatus,
                                      InspireCustomerType,
                                      MDMId,
                                      IsMDMIDDeleted,
                                      Source,
                                      Filename
                              )
                          SELECT
                              delta_table.MemberId,
                              delta_table.FirstName,
                              delta_table.LastName,
                              delta_table.DOB,
                              delta_table.CustomerStatus,
                              delta_table.MaritalStatus ,
                              delta_table.ChildrenNumber,
                              delta_table.Income,
                              delta_table.HouseHoldId,
                              delta_table.DeliverabilityStatus,
                              delta_table.IgnoreFraudSuspendFlag,
                              delta_table.LoadedDate,
                              delta_table.ModifiedDate,
                              delta_table.BirthMonth,
                              delta_table.BirthYear,
                              delta_table.IsBirthDateImplied,
                              delta_table.Privacy,
                              delta_table.BrandId,
                              delta_table.CdmLoadDate,
                              delta_table.InspireId,
                              delta_table.AgencyId,
                              delta_table.AgencyStatus,
                              delta_table.InspireCustomerType,
                              delta_table.MDMId,
                              delta_table.IsMDMIDDeleted,
                              delta_table.Source,
                              delta_table.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `
                                                    + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1

                                    )`;

    var insert_stream_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.cust_profile_plr(
                                      MemberId,
                                      FirstName,
                                      LastName,
                                      DOB,
                                      CustomerStatus,
                                      MaritalStatus ,
                                      ChildrenNumber,
                                      Income,
                                      HouseHoldId,
                                      DeliverabilityStatus,
                                      IgnoreFraudSuspendFlag,
                                      LoadedDate,
                                      ModifiedDate,
                                      BirthMonth,
                                      BirthYear,
                                      IsBirthDateImplied,
                                      Privacy,
                                      BrandId,
                                      CdmLoadDate,
                                      InspireId,
                                      AgencyId,
                                      AgencyStatus,
                                      InspireCustomerType,
                                      MDMId,
                                      IsMDMIDDeleted,
                                      Source,
                                      Filename
                              )
                          SELECT
                              delta_stream.MemberId,
                              delta_stream.FirstName,
                              delta_stream.LastName,
                              delta_stream.DOB,
                              delta_stream.CustomerStatus,
                              delta_stream.MaritalStatus ,
                              delta_stream.ChildrenNumber,
                              delta_stream.Income,
                              delta_stream.HouseHoldId,
                              delta_stream.DeliverabilityStatus,
                              delta_stream.IgnoreFraudSuspendFlag,
                              delta_stream.LoadedDate,
                              delta_stream.ModifiedDate,
                              delta_stream.BirthMonth,
                              delta_stream.BirthYear,
                              delta_stream.IsBirthDateImplied,
                              delta_stream.Privacy,
                              delta_stream.BrandId,
                              delta_stream.CdmLoadDate,
                              delta_stream.InspireId,
                              delta_stream.AgencyId,
                              delta_stream.AgencyStatus,
                              delta_stream.InspireCustomerType,
                              delta_stream.MDMId,
                              delta_stream.IsMDMIDDeleted,
                              delta_stream.Source,
                              delta_stream.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr_delta_stream delta_stream
                          WHERE EXISTS(
                                        select
                                           log_stream.add_file
                                        from
                                           (
                                              select
                                                 add_file
                                              from `
                                                 + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr_delta_log_insert_stream
                                              where
                                                 add_file is not null and filename > :1
                                           )
                                           log_stream
                                        where
                                           log_stream.add_file = delta_stream.filename )`;

    var delete_json_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + DB_PARAM+`.`+SCHEMA_PARAM+`.cust_profile_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var stream_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
        var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_stream_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_stream_data,
                binds: [stream_search]
                }
        );
        insert_stream_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [stream_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS TRAN_LOYALTY_DISCOUNT_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES_DELTA("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log`
        });

    var insert_stream_data = `INSERT INTO `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr(
                                TRANSACTIONID, 
TRANDISCOUNTID, 
OFFERCODE, 
SOURCEITEMID, 
DISCOUNTAMOUNT, 
DISCOUNTDESC, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
FILENAME
                              )
                          SELECT
                                delta_stream.TRANSACTIONID, 
delta_stream.TRANDISCOUNTID, 
delta_stream.OFFERCODE, 
delta_stream.SOURCEITEMID, 
delta_stream.DISCOUNTAMOUNT, 
delta_stream.DISCOUNTDESC, 
delta_stream.BRANDID, 
delta_stream.SOURCE, 
delta_stream.CDMLOADDATE, 
delta_stream.FILENAME
                          FROM `
                              + SRC_DB_PARAM+`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_stream delta_stream
                          WHERE EXISTS(
                                        select
                                           log_stream.add_file
                                        from
                                           (
                                              select
                                                 add_file
                                              from `
                                                 + SRC_DB_PARAM+`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log_insert_stream
                                              where
                                                 add_file is not null and filename >= :1
                                           )
                                           log_stream
                                        where
                                           log_stream.add_file = delta_stream.filename )`;

    var delete_json_data = `DELETE FROM ` + DST_DB_PARAM+`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + SRC_DB_PARAM+`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename >= :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var stream_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_stream_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_stream_data,
                binds: [stream_search]
                }
        );
        insert_stream_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [stream_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS TRAN_LOYALTY_DISCOUNT_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES_HISTORICAL("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr(
TRANSACTIONID, 
TRANDISCOUNTID, 
OFFERCODE, 
SOURCEITEMID, 
DISCOUNTAMOUNT, 
DISCOUNTDESC, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
FILENAME

                              )
                          SELECT
                                delta_table.TRANSACTIONID, 
delta_table.TRANDISCOUNTID, 
delta_table.OFFERCODE, 
delta_table.SOURCEITEMID, 
delta_table.DISCOUNTAMOUNT, 
delta_table.DISCOUNTDESC, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.CDMLOADDATE, 
delta_table.FILENAME
                          FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1
                                    )`;

    var insert_json_data = `INSERT INTO `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr(
TRANSACTIONID, 
TRANDISCOUNTID, 
OFFERCODE, 
SOURCEITEMID, 
DISCOUNTAMOUNT, 
DISCOUNTDESC, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
FILENAME
                              )
                          SELECT
                                delta_table.TRANSACTIONID, 
delta_table.TRANDISCOUNTID, 
delta_table.OFFERCODE, 
delta_table.SOURCEITEMID, 
delta_table.DISCOUNTAMOUNT, 
delta_table.DISCOUNTDESC, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.CDMLOADDATE, 
delta_table.FILENAME
                          FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta delta_table
                          WHERE EXISTS(
                                      select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_json.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log
                                                   where
                                                      filename > :1
                                                      and add_file is not null
                                                )
                                                metadata_json
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr target_table
                                                   where
                                                      target_table.filename = metadata_json.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_json_data = `DELETE FROM `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.tran_loyalty_discount_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.tran_loyalty_discount_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var json_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
       var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_json_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_json_data,
                binds: [json_search]
                }
        );
        insert_json_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [json_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS TRAN_ORDERLINE_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES_HISTORICAL("DB_PARAM" VARCHAR(16777216), "SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_orderline_plr(
ORDERID, 
ORDERLINEID, 
CHANNELID, 
MDMITEMID, 
MDMPARENTITEMID, 
BUSINESSDATE, 
TIMEKEY, 
STOREID, 
EMPLOYEEID, 
REVENUECENTER, 
REGISTERID, 
TAXID, 
SEATNUMBER, 
GROSSQUANTITY, 
PRICE, 
DISCOUNTPRICE, 
GROSSAMOUNT, 
NETAMOUNT, 
TAXAMOUNT, 
INCLUSIVETAX, 
ISCLEARED, 
ISDELETED, 
ISVOIDED, 
ISINVENTORY, 
ISDISCOUNTED, 
MODIFIERID, 
TAXEXEMPTID, 
MANAGERID, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
LOADTYPE, 
FILENAME

                              )
                          SELECT
                                delta_table.ORDERID, 
delta_table.ORDERLINEID, 
delta_table.CHANNELID, 
delta_table.MDMITEMID, 
delta_table.MDMPARENTITEMID, 
delta_table.BUSINESSDATE, 
delta_table.TIMEKEY, 
delta_table.STOREID, 
delta_table.EMPLOYEEID, 
delta_table.REVENUECENTER, 
delta_table.REGISTERID, 
delta_table.TAXID, 
delta_table.SEATNUMBER, 
delta_table.GROSSQUANTITY, 
delta_table.PRICE, 
delta_table.DISCOUNTPRICE, 
delta_table.GROSSAMOUNT, 
delta_table.NETAMOUNT, 
delta_table.TAXAMOUNT, 
delta_table.INCLUSIVETAX, 
delta_table.ISCLEARED, 
delta_table.ISDELETED, 
delta_table.ISVOIDED, 
delta_table.ISINVENTORY, 
delta_table.ISDISCOUNTED, 
delta_table.MODIFIERID, 
delta_table.TAXEXEMPTID, 
delta_table.MANAGERID, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.CDMLOADDATE, 
delta_table.LOADTYPE, 
delta_table.FILENAME
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `
                                                    + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1
                                    )`;

    var insert_json_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_orderline_plr(
ORDERID, 
ORDERLINEID, 
CHANNELID, 
MDMITEMID, 
MDMPARENTITEMID, 
BUSINESSDATE, 
TIMEKEY, 
STOREID, 
EMPLOYEEID, 
REVENUECENTER, 
REGISTERID, 
TAXID, 
SEATNUMBER, 
GROSSQUANTITY, 
PRICE, 
DISCOUNTPRICE, 
GROSSAMOUNT, 
NETAMOUNT, 
TAXAMOUNT, 
INCLUSIVETAX, 
ISCLEARED, 
ISDELETED, 
ISVOIDED, 
ISINVENTORY, 
ISDISCOUNTED, 
MODIFIERID, 
TAXEXEMPTID, 
MANAGERID, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
LOADTYPE, 
FILENAME
                              )
                          SELECT
                                delta_table.ORDERID, 
delta_table.ORDERLINEID, 
delta_table.CHANNELID, 
delta_table.MDMITEMID, 
delta_table.MDMPARENTITEMID, 
delta_table.BUSINESSDATE, 
delta_table.TIMEKEY, 
delta_table.STOREID, 
delta_table.EMPLOYEEID, 
delta_table.REVENUECENTER, 
delta_table.REGISTERID, 
delta_table.TAXID, 
delta_table.SEATNUMBER, 
delta_table.GROSSQUANTITY, 
delta_table.PRICE, 
delta_table.DISCOUNTPRICE, 
delta_table.GROSSAMOUNT, 
delta_table.NETAMOUNT, 
delta_table.TAXAMOUNT, 
delta_table.INCLUSIVETAX, 
delta_table.ISCLEARED, 
delta_table.ISDELETED, 
delta_table.ISVOIDED, 
delta_table.ISINVENTORY, 
delta_table.ISDISCOUNTED, 
delta_table.MODIFIERID, 
delta_table.TAXEXEMPTID, 
delta_table.MANAGERID, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.CDMLOADDATE, 
delta_table.LOADTYPE, 
delta_table.FILENAME
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta delta_table
                          WHERE EXISTS(
                                      select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_json.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta_log
                                                   where
                                                      filename > :1
                                                      and add_file is not null
                                                )
                                                metadata_json
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr target_table
                                                   where
                                                      target_table.filename = metadata_json.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_json_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_orderline_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var json_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
       var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_json_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_json_data,
                binds: [json_search]
                }
        );
        insert_json_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [json_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS TRAN_ORDER_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES("DB_PARAM" VARCHAR(16777216), "SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr(
                              OrderId,
                              EmployeeId,
                              BusinessDate,
                              StoreId,
                              CheckNumber,
                              OpenedTime,
                              ClosedTime ,
                              OrderName,
                              GrossQuantity,
                              GrossAmount,
                              DiscountAmount,
                              NetAmount,
                              SurchargeAmount,
                              TaxAmount,
                              PaymentAmount,
                              Gratuity,
                              CustomerName,
                              CustomerID,
                              LoyaltyNumber,
                              FirstSendTime,
                              IsClosed,
                              IsFutureOrder,
                              IsVoid,
                              IsRefund,
                              IsTaxExempt,
                              GuestCount,
                              CdmLoadDate,
                              BrandId,
                              Source,
                              TimeKey,
                              RestaurantKey,
                              LoadType,
                              InspireId,
                              Filename
                              )
                          SELECT
                                delta_table.OrderId,
                                delta_table.EmployeeId,
                                delta_table.BusinessDate,
                                delta_table.StoreId,
                                delta_table.CheckNumber,
                                delta_table.OpenedTime,
                                delta_table.ClosedTime ,
                                delta_table.OrderName,
                                delta_table.GrossQuantity,
                                delta_table.GrossAmount,
                                delta_table.DiscountAmount,
                                delta_table.NetAmount,
                                delta_table.SurchargeAmount,
                                delta_table.TaxAmount,
                                delta_table.PaymentAmount,
                                delta_table.Gratuity,
                                delta_table.CustomerName,
                                delta_table.CustomerID,
                                delta_table.LoyaltyNumber,
                                delta_table.FirstSendTime,
                                delta_table.IsClosed,
                                delta_table.IsFutureOrder,
                                delta_table.IsVoid,
                                delta_table.IsRefund,
                                delta_table.IsTaxExempt,
                                delta_table.GuestCount,
                                delta_table.CdmLoadDate,
                                delta_table.BrandId,
                                delta_table.Source,
                                delta_table.TimeKey,
                                delta_table.RestaurantKey,
                                delta_table.LoadType,
                                delta_table.InspireId,
                                delta_table.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `
                                                    + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1

                                    )`;

    var insert_stream_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr(
                              OrderId,
                              EmployeeId,
                              BusinessDate,
                              StoreId,
                              CheckNumber,
                              OpenedTime,
                              ClosedTime ,
                              OrderName,
                              GrossQuantity,
                              GrossAmount,
                              DiscountAmount,
                              NetAmount,
                              SurchargeAmount,
                              TaxAmount,
                              PaymentAmount,
                              Gratuity,
                              CustomerName,
                              CustomerID,
                              LoyaltyNumber,
                              FirstSendTime,
                              IsClosed,
                              IsFutureOrder,
                              IsVoid,
                              IsRefund,
                              IsTaxExempt,
                              GuestCount,
                              CdmLoadDate,
                              BrandId,
                              Source,
                              TimeKey,
                              RestaurantKey,
                              LoadType,
                              InspireId,
                              Filename
                              )
                          SELECT
                    delta_stream.OrderId,
                                delta_stream.EmployeeId,
                                delta_stream.BusinessDate,
                                delta_stream.StoreId,
                                delta_stream.CheckNumber,
                                delta_stream.OpenedTime,
                                delta_stream.ClosedTime ,
                                delta_stream.OrderName,
                                delta_stream.GrossQuantity,
                                delta_stream.GrossAmount,
                                delta_stream.DiscountAmount,
                                delta_stream.NetAmount,
                                delta_stream.SurchargeAmount,
                                delta_stream.TaxAmount,
                                delta_stream.PaymentAmount,
                                delta_stream.Gratuity,
                                delta_stream.CustomerName,
                                delta_stream.CustomerID,
                                delta_stream.LoyaltyNumber,
                                delta_stream.FirstSendTime,
                                delta_stream.IsClosed,
                                delta_stream.IsFutureOrder,
                                delta_stream.IsVoid,
                                delta_stream.IsRefund,
                                delta_stream.IsTaxExempt,
                                delta_stream.GuestCount,
                                delta_stream.CdmLoadDate,
                                delta_stream.BrandId,
                                delta_stream.Source,
                                delta_stream.TimeKey,
                                delta_stream.RestaurantKey,
                                delta_stream.LoadType,
                                delta_stream.InspireId,
                                delta_stream.Filename
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_stream delta_stream
                          WHERE EXISTS(
                                        select
                                           log_stream.add_file
                                        from
                                           (
                                              select
                                                 add_file
                                              from `
                                                 + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log_insert_stream
                                              where
                                                 add_file is not null and filename > :1
                                           )
                                           log_stream
                                        where
                                           log_stream.add_file = delta_stream.filename )`;

    var delete_json_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var stream_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
        var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_stream_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_stream_data,
                binds: [stream_search]
                }
        );
        insert_stream_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [stream_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS TRAN_ORDER_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES_HISTORICAL("DB_PARAM" VARCHAR(16777216), "SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr(
ORDERID, 
EMPLOYEEID, 
BUSINESSDATE, 
STOREID, 
CHECKNUMBER, 
OPENEDTIME, 
CLOSEDTIME, 
ORDERNAME, 
GROSSQUANTITY, 
GROSSAMOUNT, 
DISCOUNTAMOUNT, 
NETAMOUNT, 
SURCHARGEAMOUNT, 
TAXAMOUNT, 
PAYMENTAMOUNT, 
GRATUITY, 
CUSTOMERNAME, 
CUSTOMERID, 
LOYALTYNUMBER, 
FIRSTSENDTIME, 
ISCLOSED, 
ISFUTUREORDER, 
ISVOID, 
ISREFUND, 
ISTAXEXEMPT, 
GUESTCOUNT, 
CDMLOADDATE, 
BRANDID, 
SOURCE, 
TIMEKEY, 
RESTAURANTKEY, 
LOADTYPE, 
INSPIREID, 
FILENAME

                              )
                          SELECT
                                delta_table.ORDERID, 
delta_table.EMPLOYEEID, 
delta_table.BUSINESSDATE, 
delta_table.STOREID, 
delta_table.CHECKNUMBER, 
delta_table.OPENEDTIME, 
delta_table.CLOSEDTIME, 
delta_table.ORDERNAME, 
delta_table.GROSSQUANTITY, 
delta_table.GROSSAMOUNT, 
delta_table.DISCOUNTAMOUNT, 
delta_table.NETAMOUNT, 
delta_table.SURCHARGEAMOUNT, 
delta_table.TAXAMOUNT, 
delta_table.PAYMENTAMOUNT, 
delta_table.GRATUITY, 
delta_table.CUSTOMERNAME, 
delta_table.CUSTOMERID, 
delta_table.LOYALTYNUMBER, 
delta_table.FIRSTSENDTIME, 
delta_table.ISCLOSED, 
delta_table.ISFUTUREORDER, 
delta_table.ISVOID, 
delta_table.ISREFUND, 
delta_table.ISTAXEXEMPT, 
delta_table.GUESTCOUNT, 
delta_table.CDMLOADDATE, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.TIMEKEY, 
delta_table.RESTAURANTKEY, 
delta_table.LOADTYPE, 
delta_table.INSPIREID, 
delta_table.FILENAME
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `
                                                    + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1
                                    )`;

    var insert_json_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.tran_order_plr(
ORDERID, 
EMPLOYEEID, 
BUSINESSDATE, 
STOREID, 
CHECKNUMBER, 
OPENEDTIME, 
CLOSEDTIME, 
ORDERNAME, 
GROSSQUANTITY, 
GROSSAMOUNT, 
DISCOUNTAMOUNT, 
NETAMOUNT, 
SURCHARGEAMOUNT, 
TAXAMOUNT, 
PAYMENTAMOUNT, 
GRATUITY, 
CUSTOMERNAME, 
CUSTOMERID, 
LOYALTYNUMBER, 
FIRSTSENDTIME, 
ISCLOSED, 
ISFUTUREORDER, 
ISVOID, 
ISREFUND, 
ISTAXEXEMPT, 
GUESTCOUNT, 
CDMLOADDATE, 
BRANDID, 
SOURCE, 
TIMEKEY, 
RESTAURANTKEY, 
LOADTYPE, 
INSPIREID, 
FILENAME
                              )
                          SELECT
                                delta_table.ORDERID, 
delta_table.EMPLOYEEID, 
delta_table.BUSINESSDATE, 
delta_table.STOREID, 
delta_table.CHECKNUMBER, 
delta_table.OPENEDTIME, 
delta_table.CLOSEDTIME, 
delta_table.ORDERNAME, 
delta_table.GROSSQUANTITY, 
delta_table.GROSSAMOUNT, 
delta_table.DISCOUNTAMOUNT, 
delta_table.NETAMOUNT, 
delta_table.SURCHARGEAMOUNT, 
delta_table.TAXAMOUNT, 
delta_table.PAYMENTAMOUNT, 
delta_table.GRATUITY, 
delta_table.CUSTOMERNAME, 
delta_table.CUSTOMERID, 
delta_table.LOYALTYNUMBER, 
delta_table.FIRSTSENDTIME, 
delta_table.ISCLOSED, 
delta_table.ISFUTUREORDER, 
delta_table.ISVOID, 
delta_table.ISREFUND, 
delta_table.ISTAXEXEMPT, 
delta_table.GUESTCOUNT, 
delta_table.CDMLOADDATE, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.TIMEKEY, 
delta_table.RESTAURANTKEY, 
delta_table.LOADTYPE, 
delta_table.INSPIREID, 
delta_table.FILENAME
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta delta_table
                          WHERE EXISTS(
                                      select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_json.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log
                                                   where
                                                      filename > :1
                                                      and add_file is not null
                                                )
                                                metadata_json
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr target_table
                                                   where
                                                      target_table.filename = metadata_json.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_json_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + DB_PARAM+`.`+SCHEMA_PARAM+`.tran_order_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var json_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
       var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_json_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_json_data,
                binds: [json_search]
                }
        );
        insert_json_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [json_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS TRG_MKTG_MEMBER_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES("DB_PARAM" VARCHAR(16777216), "SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ DB_PARAM +`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.trg_mktg_member_plr(
                                      MemberId,
                                      MemberCardNumber,
                                      LoyaltyProgramId,
                                      ClosestStoreId,
                                      EnrollStartDate,
                                      EnrollmentChannel,
                                      PointBalance,
                                      MembershipStatus,
                                      PointsExpireDate,
                                      UnsubscribeDate,
                                      LastLoginDate,
                                      LastStatusChangeDate,
                                      ProfileCompletedStatus,
                                      ProfileCompletionDate,
                                      LoadedDate,
                                      ModifiedDate,
                                      Privacy,
                                      SubscriberKey,
                                      SubscriberSourceName,
                                      BrandId,
                                      CdmLoadDate,
                                      Source,
                                      FileName
                              )
                          SELECT
                                delta_table.MemberId,
                                delta_table.MemberCardNumber,
                                delta_table.LoyaltyProgramId,
                                delta_table.ClosestStoreId,
                                delta_table.EnrollStartDate,
                                delta_table.EnrollmentChannel,
                                delta_table.PointBalance,
                                delta_table.MembershipStatus,
                                delta_table.PointsExpireDate,
                                delta_table.UnsubscribeDate,
                                delta_table.LastLoginDate,
                                delta_table.LastStatusChangeDate,
                                delta_table.ProfileCompletedStatus,
                                delta_table.ProfileCompletionDate,
                                delta_table.LoadedDate,
                                delta_table.ModifiedDate,
                                delta_table.Privacy,
                                delta_table.SubscriberKey,
                                delta_table.SubscriberSourceName,
                                delta_table.BrandId,
                                delta_table.CdmLoadDate,
                                delta_table.Source,
                                delta_table.FileName
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `
                                                      + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `
                                                    + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1

                                    )`;

    var insert_stream_data = `INSERT INTO `+ DB_PARAM +`.`+SCHEMA_PARAM+`.trg_mktg_member_plr(
                                      MemberId,
                                      MemberCardNumber,
                                      LoyaltyProgramId,
                                      ClosestStoreId,
                                      EnrollStartDate,
                                      EnrollmentChannel,
                                      PointBalance,
                                      MembershipStatus,
                                      PointsExpireDate,
                                      UnsubscribeDate,
                                      LastLoginDate,
                                      LastStatusChangeDate,
                                      ProfileCompletedStatus,
                                      ProfileCompletionDate,
                                      LoadedDate,
                                      ModifiedDate,
                                      Privacy,
                                      SubscriberKey,
                                      SubscriberSourceName,
                                      BrandId,
                                      CdmLoadDate,
                                      Source,
                                      FileName
                              )
                          SELECT
                                delta_stream.MemberId,
                                delta_stream.MemberCardNumber,
                                delta_stream.LoyaltyProgramId,
                                delta_stream.ClosestStoreId,
                                delta_stream.EnrollStartDate,
                                delta_stream.EnrollmentChannel,
                                delta_stream.PointBalance,
                                delta_stream.MembershipStatus,
                                delta_stream.PointsExpireDate,
                                delta_stream.UnsubscribeDate,
                                delta_stream.LastLoginDate,
                                delta_stream.LastStatusChangeDate,
                                delta_stream.ProfileCompletedStatus,
                                delta_stream.ProfileCompletionDate,
                                delta_stream.LoadedDate,
                                delta_stream.ModifiedDate,
                                delta_stream.Privacy,
                                delta_stream.SubscriberKey,
                                delta_stream.SubscriberSourceName,
                                delta_stream.BrandId,
                                delta_stream.CdmLoadDate,
                                delta_stream.Source,
                                delta_stream.FileName
                          FROM `
                              + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta_stream delta_stream
                          WHERE EXISTS(
                                        select
                                           log_stream.add_file
                                        from
                                           (
                                              select
                                                 add_file
                                              from `
                                                 + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta_log_insert_stream
                                              where
                                                 add_file is not null and filename > :1
                                           )
                                           log_stream
                                        where
                                           log_stream.add_file = delta_stream.filename )`;

    var delete_json_data = `DELETE FROM ` + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + DB_PARAM+`.`+SCHEMA_PARAM+`.trg_mktg_member_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var stream_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
        var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_stream_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_stream_data,
                binds: [stream_search]
                }
        );
        insert_stream_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [stream_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
create or replace stream CUST_EMAIL_PLR_DELTA_LOG_INSERT_STREAM on external table CUST_EMAIL_PLR_DELTA_LOG insert_only = true;
create or replace stream CUST_EMAIL_PLR_DELTA_STREAM on external table CUST_EMAIL_PLR_DELTA insert_only = true;
create or replace stream CUST_MOBILE_PLR_DELTA_LOG_INSERT_STREAM on external table CUST_MOBILE_PLR_DELTA_LOG insert_only = true;
create or replace stream CUST_MOBILE_PLR_DELTA_STREAM on external table CUST_MOBILE_PLR_DELTA insert_only = true;
create or replace stream CUST_PROFILE_PLR_DELTA_LOG_INSERT_STREAM on external table CUST_PROFILE_PLR_DELTA_LOG insert_only = true;
create or replace stream CUST_PROFILE_PLR_DELTA_STREAM on external table CUST_PROFILE_PLR_DELTA insert_only = true;
create or replace stream TRAN_LOYALTY_DISCOUNT_PLR_DELTA_LOG_INSERT_STREAM on external table TRAN_LOYALTY_DISCOUNT_PLR_DELTA_LOG insert_only = true;
create or replace stream TRAN_LOYALTY_DISCOUNT_PLR_DELTA_STREAM on external table TRAN_LOYALTY_DISCOUNT_PLR_DELTA insert_only = true;
create or replace stream TRAN_ORDERLINE_PLR_DELTA_LOG_INSERT_STREAM on external table TRAN_ORDERLINE_PLR_DELTA_LOG insert_only = true;
create or replace stream TRAN_ORDERLINE_PLR_DELTA_STREAM on external table "TRAN_ORDERLINE_PLR_DELTA" insert_only = true;
create or replace stream TRAN_ORDER_PLR_DELTA_LOG_INSERT_STREAM on external table TRAN_ORDER_PLR_DELTA_LOG insert_only = true;
create or replace stream TRAN_ORDER_PLR_DELTA_STREAM on external table "TRAN_ORDER_PLR_DELTA" insert_only = true;
create or replace stream TRG_MKTG_MEMBER_PLR_DELTA_LOG_INSERT_STREAM on external table TRG_MKTG_MEMBER_PLR_DELTA_LOG insert_only = true;
create or replace stream TRG_MKTG_MEMBER_PLR_DELTA_STREAM on external table "TRG_MKTG_MEMBER_PLR_DELTA" insert_only = true;
create or replace task CUST_EMAIL_PLR_CDM_TO_SNOWFLAKE
	warehouse=POLARIS_DATAVENGERS_WH
	schedule='10 minute'
	USER_TASK_TIMEOUT_MS=86400000
	when (SYSTEM$STREAM_HAS_DATA(
    'cust_email_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'cust_email_plr_delta_log_insert_stream'
)) OR
(NOT SYSTEM$STREAM_HAS_DATA(
    'cust_email_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'cust_email_plr_delta_log_insert_stream'
))
	as call cust_email_plr_stp_proc_cdm_to_snowflake_tables('RDS_DEV', 'CDM');
create or replace task CUST_MOBILE_PLR_CDM_TO_SNOWFLAKE
	warehouse=POLARIS_DATAVENGERS_WH
	schedule='10 minute'
	USER_TASK_TIMEOUT_MS=86400000
	when (SYSTEM$STREAM_HAS_DATA(
    'cust_mobile_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'cust_mobile_plr_delta_log_insert_stream'
)) OR
(NOT SYSTEM$STREAM_HAS_DATA(
    'cust_mobile_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'cust_mobile_plr_delta_log_insert_stream'
))
	as call cust_mobile_plr_stp_proc_cdm_to_snowflake_tables('RDS_DEV', 'CDM');
create or replace task CUST_PROFILE_PLR_CDM_TO_SNOWFLAKE
	warehouse=POLARIS_DATAVENGERS_WH
	schedule='10 minute'
	USER_TASK_TIMEOUT_MS=86400000
	when (SYSTEM$STREAM_HAS_DATA(
    'cust_profile_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'cust_profile_plr_delta_log_insert_stream'
)) OR
(NOT SYSTEM$STREAM_HAS_DATA(
    'cust_profile_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'cust_profile_plr_delta_log_insert_stream'
))
	as call cust_profile_plr_stp_proc_cdm_to_snowflake_tables('RDS_DEV', 'CDM');
create or replace task TRAN_LOYALTY_DISCOUNT_PLR_CDM_TO_SNOWFLAKE
	warehouse=POLARIS_DATAVENGERS_WH
	schedule='10 minute'
	USER_TASK_TIMEOUT_MS=86400000
	when (SYSTEM$STREAM_HAS_DATA(
    'tran_loyalty_discount_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'tran_loyalty_discount_plr_delta_log_insert_stream'
)) OR
(NOT SYSTEM$STREAM_HAS_DATA(
    'tran_loyalty_discount_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'tran_loyalty_discount_plr_delta_log_insert_stream'
))
	as call tran_loyalty_discount_plr_stp_proc_cdm_to_snowflake_tables_delta('RDS_DEV', 'CDM', 'RDS_DEV', 'CDM');
create or replace task TRAN_ORDERLINE_PLR_CDM_TO_SNOWFLAKE
	warehouse=POLARIS_DATAVENGERS_WH
	schedule='10 minute'
	USER_TASK_TIMEOUT_MS=86400000
	when (SYSTEM$STREAM_HAS_DATA(
    'tran_orderline_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'tran_orderline_plr_delta_log_insert_stream'
)) OR
(NOT SYSTEM$STREAM_HAS_DATA(
    'tran_orderline_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'tran_orderline_plr_delta_log_insert_stream'
))
	as call tran_orderline_plr_stp_proc_cdm_to_snowflake_tables_delta('RDS_DEV', 'CDM');
create or replace task TRG_MKTG_MEMBER_PLR_CDM_TO_SNOWFLAKE
	warehouse=POLARIS_DATAVENGERS_WH
	schedule='10 minute'
	USER_TASK_TIMEOUT_MS=86400000
	when (SYSTEM$STREAM_HAS_DATA(
    'trg_mktg_member_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'trg_mktg_member_plr_delta_log_insert_stream'
)) OR
(NOT SYSTEM$STREAM_HAS_DATA(
    'trg_mktg_member_plr_delta_stream'
)
AND SYSTEM$STREAM_HAS_DATA(
    'trg_mktg_member_plr_delta_log_insert_stream'
))
	as call trg_mktg_member_plr_stp_proc_cdm_to_snowflake_tables('RDS_DEV', 'CDM');