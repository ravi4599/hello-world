package com.virginvoyages.dimension
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.expressions.Window

import java.sql.SQLException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import com.virginvoyages.scd.GetXrefData.srcReferenceTypeTotgtReferenceType
import org.apache.spark.broadcast.Broadcast
import java.net.UnknownHostException
import scala.util.parsing.json._

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode

object PersonDimLoad {
  // Creating logger
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  // Defining the main method

  def main(args: Array[String]): Unit = {

    //creating spark session
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    // creating spark context
    val sc = spark.sparkContext
    val tribeTbale = sc.broadcast(sc.getConf.get("spark.tribesubtribe.table"))
    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)

    //Calling Metadataframework to get the batchtime and partdate which is used to  get incremental data from source database(Hive table)
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_tme = metadata._3
    val batch_end_tme = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6
    val start_execution_time = metadata._7
    val part_write_date = metadata._8

    try {

      import spark.implicits._
      val frameworkEnv = spark.sparkContext.getConf.get("spark.frameworkEnv").trim()
      if (frameworkEnv.trim().toUpperCase().equals("SHORE")) {
        import spark.implicits._

        var debug_flag = "False";
        try {
          debug_flag = spark.sparkContext.getConf.get("spark.debug.flag").trim()
        } catch {
          case e: NoSuchElementException => { debug_flag = "False"; log.info("#--------------------No Debug Flag No debug  -------------------#") }

        }

        // Reading person detail from hive table MXP Persons
        val personDetail = spark.sql(spark.sparkContext.getConf.get("spark.person.sql"))

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------PersonDetailSQl-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
          //personDetail.show(2,false)
        }

        // udf defined to calculate age from given date of birth
        val currentAge = udf { (dob: java.sql.Date) =>
          import java.time.{ LocalDate, Period }
          //Period.between(dob.toLocalDate, LocalDate.now).getYears
          val age = Period.between(dob.toLocalDate, LocalDate.now).getYears * 12 + (Period.between(dob.toLocalDate, LocalDate.now).getMonths)
          val x = (age.toDouble / 12.toDouble).round.toInt
          x
        }

        val personDetailAge = personDetail.withColumn("person_age", when(col("person_dob").isNotNull, currentAge(col("person_dob"))).otherwise(lit(0)))
        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------personDetailAge-----------------#")
          //personDetailAge.show(2,false)
        }

        val personDetailtribe = personDetailAge.alias("pbarRGPSeawre")
		personDetailtribe.createOrReplaceTempView("personDetailtribeView")
        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          //personDetailtribe.show(2,false)
        }

        // reading booking detail from Hive Table
        var bookingDetailSrcDF = spark.sql(spark.sparkContext.getConf.get("spark.booking.sql"))
        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------bookingDetailSrcDF-----------------#" + spark.sql(spark.sparkContext.getConf.get("spark.booking.sql")))
          //bookingDetailSrcDF.show(2,false)
        }

        bookingDetailSrcDF = bookingDetailSrcDF.filter($"booking_id" >= lit(0))

        val bookingDetailcitiesembark = spark.sql("select city_id as city_id_embark,city_name as booking_embarkation_city from %s".format(sc.getConf.get("spark.cities.table"))) //,whereClause))
        val bookingDetailcitiesdebark = spark.sql("select city_id as city_id_debark,city_name as booking_debarkation_city from %s".format(sc.getConf.get("spark.cities.table"))) //,whereClause))

        val bookingDetail = bookingDetailSrcDF.alias("pbacity")
          .join(bookingDetailcitiesembark.alias("embark"), col("pbacity.booking_embarkation_city_id") === col("embark.city_id_embark"), "left")
          .join(bookingDetailcitiesdebark.alias("debark"), col("pbacity.booking_debarkation_city_id") === col("debark.city_id_debark"), "left")

        val window = Window.partitionBy("person_id_in_booking", "booking_id").orderBy($"booking_lastchanged".desc)
        val bookingdf = bookingDetail.withColumn("booking_rank", row_number().over(window))
        val booking = bookingdf.where(col("booking_rank") === 1)

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------booking filter -----------------#")
          //booking.show(2,false)
          //println("==============Booking count after filtering ================="+ booking.count)
        }

        // Reading data from MXP  tables
        val account_person = spark.sql(spark.sparkContext.getConf.get("spark.accountcharge.sql"))

        val martialstatusDF = spark.sql(spark.sparkContext.getConf.get("spark.martialstatus.sql"))

        val typepersonDF = spark.sql(spark.sparkContext.getConf.get("spark.persontypename.sql"))

        val lookup_itemsDF = spark.sql(spark.sparkContext.getConf.get("spark.lookupitemsMega.sql"))

        // Reading country detail from Hive table
        var countryDetailBirth = spark.sql("select country_id,country_name,last_changed,batchtime,part_date from %s".format(sc.getConf.get("spark.account.table"))) //,whereClause))
          .withColumnRenamed("country_id", "country_id_in_birth")
          .withColumnRenamed("country_name", "person_country_of_birth").drop("part_date").drop("batchtime")
        val windowAccount = Window.partitionBy("country_id_in_birth").orderBy($"last_changed".desc)
        countryDetailBirth = countryDetailBirth.withColumn("countrybirth_rank", row_number().over(windowAccount))
        countryDetailBirth = countryDetailBirth.where(col("countrybirth_rank") === 1).drop("last_changed")

        var countryDetailresidence = spark.sql("select country_id,country_name,last_changed,batchtime,part_date from %s".format(sc.getConf.get("spark.account.table"))) //,whereClause))
          .withColumnRenamed("country_id", "country_id_in_reidence")
          .withColumnRenamed("country_name", "person_country_of_residence").drop("part_date").drop("batchtime")
        val windowAccountres = Window.partitionBy("country_id_in_reidence").orderBy($"last_changed".desc)
        countryDetailresidence = countryDetailresidence.withColumn("countryresidence_rank", row_number().over(windowAccountres))
        countryDetailresidence = countryDetailresidence.where(col("countryresidence_rank") === 1).drop("last_changed")

        var countryDetailnationality = spark.sql("select country_id,country_name,last_changed,batchtime,part_date from %s".format(sc.getConf.get("spark.account.table"))) //,whereClause))
          .withColumnRenamed("country_id", "country_id_in_nationality")
          .withColumnRenamed("country_name", "person_nationality").drop("part_date").drop("batchtime")
        val windowAccountnat = Window.partitionBy("country_id_in_nationality").orderBy($"last_changed".desc)
        countryDetailnationality = countryDetailnationality.withColumn("countrynationality_rank", row_number().over(windowAccountnat))
        countryDetailnationality = countryDetailnationality.where(col("countrynationality_rank") === 1).drop("last_changed")
		
		var countryDetailcode= spark.sql("select country_id,country_code,country_name,last_changed,batchtime,part_date from %s".format(sc.getConf.get("spark.account.table"))) //,whereClause))
		.withColumnRenamed("country_id", "country_id_in_code")
		val windowAccountcode = Window.partitionBy("country_id_in_code").orderBy($"last_changed".desc)
        countryDetailcode = countryDetailcode.withColumn("countrycode_rank", row_number().over(windowAccountcode))
        countryDetailcode = countryDetailcode.where(col("countrycode_rank") === 1).drop("last_changed")
		
		countryDetailcode.createOrReplaceTempView("countrycodetempview")
		 
		 var countryDetailcodeDf = spark.sql("""select country_id_in_code,country_code,country_name,last_changed,batchtime,part_date,person_id,country_of_residence_id from personDetailtribeView left join countrycodetempview on personDetailtribeView.country_of_residence_id=countrycodetempview.country_id_in_code""")
		
		 
		 //var countryDetailcodeDf = personDetailtribe.join(countryDetailcode, col("personDetailtribe.country_of_residence_id") === col("countryDetailcode.country_id"), "left").select("country_id","country_code","country_name","last_changed","batchtime","part_date","person_id")
		 
		 countryDetailcodeDf.createOrReplaceTempView("crewcountrycodeView")




        var persontitle = spark.sql("select sys_lookup_item_name,sys_lookup_id,sys_lookup_category_id,batchtime,part_date from %s".format(sc.getConf.get("spark.syslookup.table"))) //,whereClause))
        persontitle = persontitle.withColumnRenamed("sys_lookup_category_id", "sys_lookup_category_id_pt")
        persontitle = persontitle.withColumnRenamed("sys_lookup_id", "sys_lookup_id_pt") //.drop("part_date").drop("batchtime")
        persontitle = persontitle.where(persontitle.col("sys_lookup_category_id_pt") === 8)
        persontitle = persontitle.withColumnRenamed("sys_lookup_item_name", "person_title")

        //Reading reservation  from Hive Table
        var reservationdxp = spark.sql("select reservationnumber,reservationid,lastmodifieddate,reservationstatuscode from %s".format(sc.getConf.get("spark.dxpreservation.table")))

        reservationdxp = reservationdxp.withColumnRenamed("reservationid", "reservation_guid")
        val windowreservation = Window.partitionBy("reservationnumber").orderBy($"lastmodifieddate".desc)
        reservationdxp = reservationdxp.withColumn("reservation_rank", row_number().over(windowreservation))
        reservationdxp = reservationdxp.where(col("reservation_rank") === 1)
        reservationdxp.createOrReplaceTempView("reservationdfView")

        // Reading from reservation guest

        var reservationguestdxp = spark.sql("select guestcheckindetailid,reservationid,reservationguestid,guestid,guestemergencycontactid,lastmodifieddate from %s".format(sc.getConf.get("spark.dxpreservationguest.table")))

        reservationguestdxp = reservationguestdxp.withColumnRenamed("reservationguestid", "reservationguest_guid_reservation")
        reservationguestdxp = reservationguestdxp.withColumnRenamed("guestid", "reservationguest_guestid")
        reservationguestdxp = reservationguestdxp.withColumnRenamed("reservationid", "reservationguest_reservationid")
          .drop("person_id")
          .drop("voyageid")
        val windowreservationguest = Window.partitionBy("reservationguest_reservationid", "reservationguest_guestid").orderBy($"lastmodifieddate".desc)
        reservationguestdxp = reservationguestdxp.withColumn("reservationguest_rank", row_number().over(windowreservationguest))
        reservationguestdxp = reservationguestdxp.where(col("reservationguest_rank") === 1)

        reservationguestdxp.createOrReplaceTempView("reservationguestdfView")

        spark.conf.set("spark.sql.crossJoin.enabled", "true")
        var personDetailJoinedBookingDetail = personDetailtribe.alias("p").join(booking.alias("b"), col("p.person_id") === col("b.person_id_in_booking"), "left")
        log.info("booking and person")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb1").join(account_person.alias("acctb"), col("ptb1.person_id") === col("acctb.person_id_fromaccount"), "left")
        log.info("booking and person and account")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb2").join(lookup_itemsDF.alias("lookuptb"), col("ptb2.booking_vip_status_id") === col("lookuptb.lookup_item_id_mega"), "left")
        log.info("booking and person and lookupitems_mega_star")

        // personDetailJoinedBookingDetail.where(col("booking_vip_status_id").isin(10252989, 10252992, 10252990, 10252993, 10252988, 10252991, 10226216)).count

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb3").join(martialstatusDF.alias("statustb"), col("ptb3.marital_status_id") === col("statustb.sys_lookup_id_martialstatus"), "left")
        log.info("booking and person and person_martial_status")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb4").join(typepersonDF.alias("typepersontb"), col("ptb4.person_type_id") === col("typepersontb.sys_lookup_id_typename"), "left")
        log.info("booking and person and person_type_name")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb1").join(countryDetailBirth.alias("con1"), col("pb1.person_country_of_birth_id") === col("con1.country_id_in_birth"), "left")
        log.info("booking and person and country1")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb2").join(countryDetailresidence.alias("con2"), col("pb2.country_of_residence_id") === col("con2.country_id_in_reidence"), "left")
        log.info("booking and person and country2")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb3").join(countryDetailnationality.alias("con3"), col("pb3.person_nationality_country_id") === col("con3.country_id_in_nationality"), "left")
        log.info("booking and person and country3")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb4").join(persontitle.alias("con4"), col("pb4.person_title_check") === col("con4.sys_lookup_id_pt"), "left")
        log.info("booking and person and person_title")

        //										personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pba").join(address.alias("add"), col("pba.person_id") === col("add.person_id_in_address"), "left")
        //										log.info("booking and person account address")
        //										personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb4").join(countryadress.alias("con4"), col("pb4.person_country_id") === col("con4.country_id_in_countries"), "left")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.where(personDetailJoinedBookingDetail.col("person_guid").isNotNull)

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pbar").join(reservationdxp.alias("addreser"), col("pbar.booking_reference") === col("addreser.reservationnumber"), "left")
        log.info("reservation")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pbarRG").join(reservationguestdxp.alias("addresergue"), col("pbarRG.reservation_guid") === col("addresergue.reservationguest_reservationid") && col("pbarRG.person_guid") === upper(col("addresergue.reservationguest_guestid")), "left")
        log.info("reservationguest")

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println(s"""#---------------------------------- after all join personDetailJoinedBookingDetail---------------------------------#""")
          //personDetailJoinedBookingDetail.show(2,false)
        }

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.select("sourceID", "srcReferenceType", "tgtReferenceType", "person_id", "person_guid", "person_charge_id", "person_external_id", "mxp_seaware_id","account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "voyage_id", "batchtime", "part_date", "guestcheckindetailid", "booking_manifest_type", "booking_vip_tier","external_booking_id","position_id","ts_ms","crew_manning_agent_code","crew_salary_currency","departure_status_id","persons_op","personbooking_op","created")
        personDetailJoinedBookingDetail.createOrReplaceTempView("personDetailJoinedBookingDetail")

        val guestcheckdetail = spark.sql(spark.sparkContext.getConf.get("spark.guestcheckindetail.sql"))
        guestcheckdetail.createOrReplaceTempView("guestcheckdetail")

        val healthWellnessDf = spark.sql("select * from personDetailJoinedBookingDetail left join guestcheckdetail on (guestcheckindetailid=guestcheckindetail_id)")
          .select("sourceID", "srcReferenceType", "tgtReferenceType", "person_id", "person_guid", "person_charge_id", "person_external_id","mxp_seaware_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type","external_booking_id", "position_id","ts_ms","crew_manning_agent_code","crew_salary_currency","departure_status_id","persons_op","personbooking_op","created","voyage_id", "batchtime", "part_date")

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {

          println("#---------------------------------------healthWellnessDf----------------#")

          println("#---------------------------------------healthWellnessDf----------------#")
        }

        //		========================================== Before interim check ====================

        if (!healthWellnessDf.head(1).isEmpty) {

          //var interimDF = srcReferenceTypeTotgtReferenceType(spark, healthWellnessDf)
          var interimDF = srcReferenceTypeTotgtReferenceType(spark, healthWellnessDf)
          // var interimDF = healthWellnessDf.withColumn("targetID", lit("null").cast(IntegerType))

          log.info("After Xref Call")

          import java.sql._;

          /*val HbaseSailorTribeDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim()).option("table", spark.sparkContext.getConf.get("spark.tribesubtribe.table").trim()).
            option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load()

          val HbaseSeawareReservationDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim()).option("table", spark.sparkContext.getConf.get("spark.seawarereservation.table").trim()).
            option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load().as("hbseawareDF")

          val HbasebainsegmentationDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim()).option("table", spark.sparkContext.getConf.get("spark.bainsegmentation.table").trim()).
            option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load().as("hbbainDF")*/
			val HbaseSailorTribeDF = spark.sql(spark.sparkContext.getConf.get("spark.tribesubtribe.table").trim())
			val HbaseSeawareReservationDF = spark.sql(spark.sparkContext.getConf.get("spark.seawarereservation.table").trim()).as("hbseawareDF")
			val HbasebainsegmentationDF = spark.sql(spark.sparkContext.getConf.get("spark.bainsegmentation.table").trim())as("hbbainDF")

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------HbaseDF----------------#")
            //HbaseSailorTribeDF.show(false)
            println("#---------------------------------------HbaseDF----------------#")
          }

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------HbaseSeawareReservationDF----HOUSEHOLD_BASICDEMOGRAPHICS_ESTIMATEDINCOMEMAX------------#")
            //HbaseSeawareReservationDF.show(false)
            println("#---------------------------------------HbaseSeawareReservationDF-------HOUSEHOLD_BASICDEMOGRAPHICS_ESTIMATEDINCOMEMAX---------#")
          }

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------HbasebainsegmentationDF-----------PREDICTEDLABEL-----#")
            //HbasebainsegmentationDF.show(false)
            println("#---------------------------------------HbasebainsegmentationDF----------PREDICTEDLABEL------#")
          }

          var finalDf = spark.emptyDataFrame

          HbaseSailorTribeDF.createOrReplaceTempView("SailorTribeSubtribeTable")
          var tribesubDf = spark.sql("select * from (select *,row_number() over (partition by SEAWARE_ID order by timestampp desc,C360ID desc) as rn  from SailorTribeSubtribeTable ) a where a.rn=1 ").as("hbsDF")

          val sailorDf = interimDF.join(tribesubDf, upper(col("targetID")) === upper(col("hbsDF.SEAWARE_ID")), "left")
            .selectExpr("targetID as seaware_id", "sourceID", "cast(person_id as string)as person_id", "person_guid", "person_charge_id", "person_external_id", "mxp_seaware_id","account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "hbsDF.sailor_tribe", "hbsDF.sailor_subtribe", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type","external_booking_id","position_id","ts_ms","crew_manning_agent_code","crew_salary_currency","departure_status_id","persons_op","personbooking_op","created", "voyage_id", "batchtime", "part_date").as("saidf")

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------sailorDf-----------sailorDf-----#")
            //sailorDf.show(false)
            println("#---------------------------------------sailorDf----------sailorDf------#")
          }
          val seawareHBSDF = sailorDf.join(HbaseSeawareReservationDF, upper(col("saidf.seaware_id")) === upper(col("hbseawareDF.SEAWARE_ID")), "left")
            .selectExpr("hbseawareDF.HOUSEHOLD_BASICDEMOGRAPHICS_ESTIMATEDINCOMEMAX as estimated_household_income", "saidf.seaware_id as seaware_id", "sourceID", "person_id", "person_guid", "person_charge_id", "person_external_id","mxp_seaware_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "sailor_tribe", "sailor_subtribe", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type","external_booking_id" ,"position_id","ts_ms","crew_manning_agent_code","crew_salary_currency","departure_status_id","persons_op","personbooking_op","created","voyage_id", "batchtime", "part_date").as("seawDF")

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------seawareHBSDF----------------#")
            //seawareHBSDF.show(false)
            //seawareHBSDF.printSchema
            println("#---------------------------------------seawareHBSDF----------------#")
          }

          try {

			//seawareHBSDF.show
            finalDf = seawareHBSDF.join(HbasebainsegmentationDF, upper(col("seawDF.seaware_id")) === upper(col("hbbainDF.SEAWAREID")), "left")
              .selectExpr("hbbainDF.PREDICTEDLABEL as bain_segmentation_predicted_label", "estimated_household_income", "seawDF.seaware_id as seaware_id", "sourceID", "person_id", "person_guid", "person_charge_id", "person_external_id","mxp_seaware_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "sailor_tribe", "sailor_subtribe", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type","external_booking_id","position_id","ts_ms","crew_manning_agent_code","crew_salary_currency","departure_status_id","persons_op","personbooking_op","created", "voyage_id", "batchtime", "part_date")

            finalDf.createOrReplaceTempView("finalDfView")

            val guestemergency = spark.sql(spark.sparkContext.getConf.get("spark.emergencycontact.sql"))
            guestemergency.createOrReplaceTempView("guestemergencyDfView")
			
			val Lookupitems233= spark.sql("""select * from (select lookup_item_name,display_code,case when lookup_item_name='Mega RockStar' then true else false end as mega_rockstar_flag ,lookup_item_id,lookup_sub_category_id,active,rec_deleted,row_number() over (partition by lookup_item_id order by last_changed desc) as rn from  shipdw.hvtb_parse_mxp_lookup_items) lkpitem where lkpitem.rn =1 """)
			Lookupitems233.createOrReplaceTempView("LookupitemsView")

            val FinalDff = spark.sql("""SELECT finaldf.bain_segmentation_predicted_label ,finaldf.estimated_household_income ,finaldf.seaware_id ,finaldf.sourceID ,finaldf.person_id ,finaldf.person_guid ,finaldf.person_charge_id ,finaldf.person_external_id ,finalDf.mxp_seaware_id,finaldf.account_number ,finaldf.person_type ,finaldf.person_gender ,finaldf.person_title ,finaldf.person_first_name ,finaldf.person_middle_name ,finaldf.person_last_name ,finaldf.person_suffix ,finaldf.person_marital_status ,finaldf.person_primary_email ,finaldf.person_dob ,finaldf.person_age ,finaldf.person_place_of_birth ,finaldf.person_country_of_birth ,finaldf.person_country_of_residence ,finaldf.person_nationality ,finaldf.voyage_skey ,finaldf.booking_arrival_date ,finaldf.booking_departure_date ,finaldf.booking_reference ,finaldf.booking_cruise_number ,finaldf.cabin_number ,finaldf.mega_rockstar_flag ,finaldf.vip_flag ,finaldf.booking_arrival_status ,finaldf.booking_status ,finaldf.booking_embarkation_city ,finaldf.booking_debarkation_city ,finaldf.booking_id ,finaldf.reservationguest_guid ,finaldf.reservation_guid ,finaldf.sailor_tribe ,finaldf.sailor_subtribe ,finaldf.contract_signed_version ,finaldf.voyage_well_accepted_version ,finaldf.pre_voyage_health_contract_version ,finaldf.person_booking_charge_id ,finaldf.person_pin_code ,finaldf.person_booking_ship_code ,finaldf.crew_manning_agent ,finaldf.crew_hire_date ,finaldf.person_type_name ,finaldf.booking_vip_status ,finaldf.booking_vip_tier ,finaldf.booking_manifest_type,finalDf.external_booking_id, finalDf.position_id,finalDf.ts_ms,guestemercontact.emergency_contact_person_name ,guestemercontact.emergency_contact_person_relationship ,guestemercontact.emergency_contact_person_phone_number ,guestemercontact.emergency_contact_person_email,Crewcountrycode.country_code as person_country_code_of_residence,LookupitemsView.lookup_item_name as sign_off_reason,finaldf.crew_manning_agent_code,finaldf.crew_salary_currency,finaldf.persons_op,finaldf.personbooking_op,finaldf.created,finaldf.voyage_id ,finaldf.batchtime ,finaldf.part_date FROM ( ( SELECT * FROM finalDfView ) finaldf LEFT JOIN ( SELECT * FROM reservationguestdfView ) resguest ON finaldf.reservation_guid = resguest.reservationguest_reservationid AND finaldf.reservationguest_guid = resguest.reservationguest_guid_reservation LEFT JOIN ( SELECT * FROM guestemergencyDfView ) guestemercontact ON resguest.guestemergencycontactid = guestemercontact.guestemergencycontactid LEFT JOIN ( SELECT * FROM reservationdfView WHERE reservationstatuscode <> 'CN' ) resdf ON resguest.reservationguest_reservationid = resdf.reservation_guid left join (select * from crewcountrycodeView )Crewcountrycode on finaldf.person_id=Crewcountrycode.person_id ) left join (select * from LookupitemsView) LookupitemsView on (finaldf.departure_status_id=LookupitemsView.lookup_item_id and LookupitemsView.lookup_sub_category_id = 233) """)

            if (debug_flag.trim().toUpperCase().equals("True")) {
              println("#--------------------------------------Final--------- before SCD-------#")

            }
            println("before")
            if (!FinalDff.head(1).isEmpty) {
              /*
              //FinalDff.printSchema
              //FinalDff.show(100,false)
              //val FinalNoNullDff = FinalDff.filter(row => !row.anyNull);
     val ConvertDf = FinalDff.withColumn("vip_flag", col("vip_flag").cast(BooleanType)).withColumn("person_dob", col("person_dob").cast(DateType))
              ConvertDf.createOrReplaceTempView("person_tmp")
             spark.sql("SELECT count(1),person_id,booking_id FROM person_tmp group by person_id,booking_id having count(1)>1").show(false)
            val finalPersondf=ConvertDf.distinct
              //println("notnull")
              //FinalDff.show(100, false)
              loadDimFact(spark, finalPersondf)*/

              import spark.implicits._
              val ConvertDf = FinalDff.withColumn("vip_flag", col("vip_flag").cast(BooleanType)).withColumn("person_dob", col("person_dob").cast(DateType))
              ConvertDf.createOrReplaceTempView("person_tmp")
              spark.sql("SELECT count(1),person_id,booking_id FROM person_tmp group by person_id,booking_id having count(1)>1").show(false)
              val finalPersondf = spark.sql("select * from (select *,row_number() over (partition by person_id,booking_id order by batchtime desc) as rn from person_tmp) WHERE rn = 1").drop(col("rn")).dropDuplicates()
              //  ConvertDf.dropDuplicates()
              // val finalPersondf=ConvertDf.distinct
              finalPersondf.createOrReplaceTempView("person_tmp1")
			  //finalPersondf.show(false)
              spark.sql("SELECT count(1),person_id,booking_id FROM person_tmp1 group by person_id,booking_id having count(1)>1").show(false)

              loadDimFact(spark, finalPersondf)
            }
           // println("#--------------------------------------Quality Check after load-------#")
            /*			val personDimcount = spark.sql("select * from shipdw.hvtb_mart_dim_person").count()
									val srcCount = FinalDff.count
									println("#Source count " + srcCount + "-------#personDimcount" + personDimcount + "---------Checking quality check person dim integrity ----#")
									if (srcCount < personDimcount) {
										println("#Source count " + srcCount + "-------#personDimcount" + personDimcount + "---------Failure in quality check person dim integrity compromised ----#");
									}*/
          } catch {
            case e: Exception => {
              ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
              log.info("******************in the catch of Person Dimension Load ******************");
              e.printStackTrace();
              throw new Exception("SQL Exception..please check the stacktrace", e);
              sys.exit(1)

            }

          }

        } // if dataframe is empty

      } else if (frameworkEnv.trim().toUpperCase().equals("SHIP")) {

        import spark.implicits._

        var debug_flag = "False";
        try {
          debug_flag = spark.sparkContext.getConf.get("spark.debug.flag").trim()
        } catch {
          case e: NoSuchElementException => { debug_flag = "False"; log.info("#--------------------No Debug Flag No debug  -------------------#") }

        }

        // Reading person detail from hive table MXP Persons
        val personDetail = spark.sql(spark.sparkContext.getConf.get("spark.person.sql"))

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------PersonDetailSQl-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
          //personDetail.show(false)
        }

        // udf defined to calculate age from given date of birth
        val currentAge = udf { (dob: java.sql.Date) =>
          import java.time.{ LocalDate, Period }
          //Period.between(dob.toLocalDate, LocalDate.now).getYears
          val age = Period.between(dob.toLocalDate, LocalDate.now).getYears * 12 + (Period.between(dob.toLocalDate, LocalDate.now).getMonths)
          val x = (age.toDouble / 12.toDouble).round.toInt
          x
        }

        val personDetailAge = personDetail.withColumn("person_age", when(col("person_dob").isNotNull, currentAge(col("person_dob"))).otherwise(lit(0)))

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------personDetailAge-----------------#")
          //personDetailAge.show(false)
        }

        val personDetailtribe = personDetailAge.alias("pbarRGPSeawre")
        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          //personDetailtribe.show(false)
        }

        // reading booking detail from Hive Table
        var bookingDetailSrcDF = spark.sql(spark.sparkContext.getConf.get("spark.booking.sql"))
        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------bookingDetailSrcDF-----------------#" + spark.sql(spark.sparkContext.getConf.get("spark.booking.sql")))
          //bookingDetailSrcDF.show(false)
        }

        bookingDetailSrcDF = bookingDetailSrcDF.filter($"booking_id" >= lit(0))

        val bookingDetailcitiesembark = spark.sql("select city_id as city_id_embark,city_name as booking_embarkation_city from %s".format(sc.getConf.get("spark.cities.table"))) //,whereClause))
        val bookingDetailcitiesdebark = spark.sql("select city_id as city_id_debark,city_name as booking_debarkation_city from %s".format(sc.getConf.get("spark.cities.table"))) //,whereClause))

        val bookingDetail = bookingDetailSrcDF.alias("pbacity")
          .join(bookingDetailcitiesembark.alias("embark"), col("pbacity.booking_embarkation_city_id") === col("embark.city_id_embark"), "left")
          .join(bookingDetailcitiesdebark.alias("debark"), col("pbacity.booking_debarkation_city_id") === col("debark.city_id_debark"), "left")

        val window = Window.partitionBy("person_id_in_booking", "booking_id").orderBy($"booking_lastchanged".desc)
        val bookingdf = bookingDetail.withColumn("booking_rank", row_number().over(window))
        val booking = bookingdf.where(col("booking_rank") === 1)

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------booking filter -----------------#")
          //booking.show(2,false)
          println("==============Booking count after filtering =================" + booking.count)
        }

        // Reading data from MXP  tables
        val account_person = spark.sql(spark.sparkContext.getConf.get("spark.accountcharge.sql"))

        val martialstatusDF = spark.sql(spark.sparkContext.getConf.get("spark.martialstatus.sql"))

        val typepersonDF = spark.sql(spark.sparkContext.getConf.get("spark.persontypename.sql"))

        val lookup_itemsDF = spark.sql(spark.sparkContext.getConf.get("spark.lookupitemsMega.sql"))

        // Reading country detail from Hive table
        var countryDetailBirth = spark.sql("select country_id,country_name,last_changed,batchtime,part_date from %s".format(sc.getConf.get("spark.account.table"))) //,whereClause))
          .withColumnRenamed("country_id", "country_id_in_birth")
          .withColumnRenamed("country_name", "person_country_of_birth").drop("part_date").drop("batchtime")
        val windowAccount = Window.partitionBy("country_id_in_birth").orderBy($"last_changed".desc)
        countryDetailBirth = countryDetailBirth.withColumn("countrybirth_rank", row_number().over(windowAccount))
        countryDetailBirth = countryDetailBirth.where(col("countrybirth_rank") === 1).drop("last_changed")

        var countryDetailresidence = spark.sql("select country_id,country_name,last_changed,batchtime,part_date from %s".format(sc.getConf.get("spark.account.table"))) //,whereClause))
          .withColumnRenamed("country_id", "country_id_in_reidence")
          .withColumnRenamed("country_name", "person_country_of_residence").drop("part_date").drop("batchtime")
        val windowAccountres = Window.partitionBy("country_id_in_reidence").orderBy($"last_changed".desc)
        countryDetailresidence = countryDetailresidence.withColumn("countryresidence_rank", row_number().over(windowAccountres))
        countryDetailresidence = countryDetailresidence.where(col("countryresidence_rank") === 1).drop("last_changed")

        var countryDetailnationality = spark.sql("select country_id,country_name,last_changed,batchtime,part_date from %s".format(sc.getConf.get("spark.account.table"))) //,whereClause))
          .withColumnRenamed("country_id", "country_id_in_nationality")
          .withColumnRenamed("country_name", "person_nationality").drop("part_date").drop("batchtime")
        val windowAccountnat = Window.partitionBy("country_id_in_nationality").orderBy($"last_changed".desc)
        countryDetailnationality = countryDetailnationality.withColumn("countrynationality_rank", row_number().over(windowAccountnat))
        countryDetailnationality = countryDetailnationality.where(col("countrynationality_rank") === 1).drop("last_changed")

        var persontitle = spark.sql("select sys_lookup_item_name,sys_lookup_id,sys_lookup_category_id,batchtime,part_date from %s".format(sc.getConf.get("spark.syslookup.table"))) //,whereClause))

        persontitle = persontitle.withColumnRenamed("sys_lookup_category_id", "sys_lookup_category_id_pt")
        persontitle = persontitle.withColumnRenamed("sys_lookup_id", "sys_lookup_id_pt") //.drop("part_date").drop("batchtime")
        persontitle = persontitle.where(persontitle.col("sys_lookup_category_id_pt") === 8)
        persontitle = persontitle.withColumnRenamed("sys_lookup_item_name", "person_title")

        //Reading reservation  from Hive Table
        var reservationdxp = spark.sql("select reservationnumber,reservationid,lastmodifieddate,reservationstatuscode from %s".format(sc.getConf.get("spark.dxpreservation.table")))

        reservationdxp = reservationdxp.withColumnRenamed("reservationid", "reservation_guid")
        val windowreservation = Window.partitionBy("reservationnumber").orderBy($"lastmodifieddate".desc)
        reservationdxp = reservationdxp.withColumn("reservation_rank", row_number().over(windowreservation))
        reservationdxp = reservationdxp.where(col("reservation_rank") === 1)
        reservationdxp.createOrReplaceTempView("reservationdfView")

        // Reading from reservation guest

        var reservationguestdxp = spark.sql("select guestcheckindetailid,reservationid,reservationguestid,guestid,guestemergencycontactid,lastmodifieddate from %s".format(sc.getConf.get("spark.dxpreservationguest.table")))

        reservationguestdxp = reservationguestdxp.withColumnRenamed("reservationguestid", "reservationguest_guid_reservation")
        reservationguestdxp = reservationguestdxp.withColumnRenamed("guestid", "reservationguest_guestid")
        reservationguestdxp = reservationguestdxp.withColumnRenamed("reservationid", "reservationguest_reservationid")
          .drop("person_id")
          .drop("voyageid")
        val windowreservationguest = Window.partitionBy("reservationguest_reservationid", "reservationguest_guestid").orderBy($"lastmodifieddate".desc)
        reservationguestdxp = reservationguestdxp.withColumn("reservationguest_rank", row_number().over(windowreservationguest))
        reservationguestdxp = reservationguestdxp.where(col("reservationguest_rank") === 1)

        reservationguestdxp.createOrReplaceTempView("reservationguestdfView")

        spark.conf.set("spark.sql.crossJoin.enabled", "true")
        var personDetailJoinedBookingDetail = personDetailtribe.alias("p").join(booking.alias("b"), col("p.person_id") === col("b.person_id_in_booking"), "left")
        log.info("booking and person")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb1").join(account_person.alias("acctb"), col("ptb1.person_id") === col("acctb.person_id_fromaccount"), "left")
        log.info("booking and person and account")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb2").join(lookup_itemsDF.alias("lookuptb"), col("ptb2.booking_vip_status_id") === col("lookuptb.lookup_item_id_mega"), "left")
        log.info("booking and person and lookupitems_mega_star")

        // personDetailJoinedBookingDetail.where(col("booking_vip_status_id").isin(10252989, 10252992, 10252990, 10252993, 10252988, 10252991, 10226216)).count

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb3").join(martialstatusDF.alias("statustb"), col("ptb3.marital_status_id") === col("statustb.sys_lookup_id_martialstatus"), "left")
        log.info("booking and person and person_martial_status")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("ptb4").join(typepersonDF.alias("typepersontb"), col("ptb4.person_type_id") === col("typepersontb.sys_lookup_id_typename"), "left")
        log.info("booking and person and person_type_name")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb1").join(countryDetailBirth.alias("con1"), col("pb1.person_country_of_birth_id") === col("con1.country_id_in_birth"), "left")
        log.info("booking and person and country1")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb2").join(countryDetailresidence.alias("con2"), col("pb2.country_of_residence_id") === col("con2.country_id_in_reidence"), "left")
        log.info("booking and person and country2")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb3").join(countryDetailnationality.alias("con3"), col("pb3.person_nationality_country_id") === col("con3.country_id_in_nationality"), "left")
        log.info("booking and person and country3")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb4").join(persontitle.alias("con4"), col("pb4.person_title_check") === col("con4.sys_lookup_id_pt"), "left")
        log.info("booking and person and person_title")

        // personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pba").join(address.alias("add"), col("pba.person_id") === col("add.person_id_in_address"), "left")
        //  log.info("booking and person account address")

        //  personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pb4").join(countryadress.alias("con4"), col("pb4.person_country_id") === col("con4.country_id_in_countries"), "left")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.where(personDetailJoinedBookingDetail.col("person_guid").isNotNull)

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pbar").join(reservationdxp.alias("addreser"), col("pbar.booking_reference") === col("addreser.reservationnumber"), "left")
        log.info("reservation")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.alias("pbarRG").join(reservationguestdxp.alias("addresergue"), col("pbarRG.reservation_guid") === col("addresergue.reservationguest_reservationid") && col("pbarRG.person_guid") === upper(col("addresergue.reservationguest_guestid")), "left")

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println(s"""#---------------------------------- after all join personDetailJoinedBookingDetail---------------------------------#""")
          personDetailJoinedBookingDetail.show(false)
        }

        //personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.select("sourceId", "srcIdType", "srcSrcType", "tgtIdType", "tgtSrcType", "voyage_skey","person_suffix","person_marital_status","person_primary_email", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "address_country", "address_type", "address_mailing", "address_line1", "address_line2", "address_line3", "address_postal_code", "address_city", "address_state", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "pbarRG.cabin_category", "pbarRG.cabin_type", "pbarRG.mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "guestcheckindetailid", "reservationguest_guid", "reservation_guid","person_booking_charge_id","person_account_id","person_pin_code", "voyage_id",  "batchtime", "part_date")

        personDetailJoinedBookingDetail = personDetailJoinedBookingDetail.select("sourceId", "srcIdType", "srcSrcType", "tgtIdType", "tgtSrcType", "voyage_skey", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type", "voyage_id", "batchtime", "part_date", "guestcheckindetailid")
        personDetailJoinedBookingDetail.createOrReplaceTempView("personDetailJoinedBookingDetail")

        val guestcheckdetail = spark.sql(spark.sparkContext.getConf.get("spark.guestcheckindetail.sql"))
        guestcheckdetail.createOrReplaceTempView("guestcheckdetail")

        val healthWellnessDf = spark.sql("select * from personDetailJoinedBookingDetail left join guestcheckdetail on (guestcheckindetailid=guestcheckindetail_id)")
          .select("sourceId", "srcIdType", "srcSrcType", "tgtIdType", "tgtSrcType", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type", "voyage_id", "batchtime", "part_date")

        //.select("sourceId", "srcIdType", "srcSrcType", "tgtIdType", "tgtSrcType", "voyage_skey","person_suffix","person_marital_status","person_primary_email", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "address_country", "address_type", "address_mailing", "address_line1", "address_line2", "address_line3", "address_postal_code", "address_city", "address_state", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "cabin_category", "cabin_type", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "guestcheckindetailid", "reservationguest_guid", "reservation_guid","person_booking_charge_id","person_account_id","person_pin_code", "voyage_id",  "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "batchtime", "part_date")

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          healthWellnessDf.filter("contract_signed_version is not null").show(false)
          println("#---------------------------------------healthWellnessDf----------------#")
          healthWellnessDf.show(false)
          //healthWellnessDf.printSchema()
          println("#---------------------------------------healthWellnessDf----------------#")
        }

        //		========================================== Before interim check ====================

        if (!healthWellnessDf.head(1).isEmpty) {

          var interimDF = srcReferenceTypeTotgtReferenceType(spark, healthWellnessDf)

          import java.sql._;

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------interimDF----------------#")
            interimDF.show(false)
            interimDF.printSchema()
            println("#---------------------------------------interimDF----------------#")
          }

          //.selectExpr("targetID as seaware_id","sourceId","srcIdType","srcSrcType","tgtIdType","tgtSrcType",  "voyage_skey","person_suffix", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "address_country", "address_type", "address_mailing", "address_line1", "address_line2", "address_line3", "address_postal_code", "address_city", "address_state", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "cabin_category", "cabin_type", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid","person_booking_charge_id","person_account_id","person_pin_code", "sailor_tribe", "sailor_subtribe", "person_card_type", "person_card_status", "person_card_level", "person_card_color", "person_card_color_value", "voyage_id",  "batchtime", "part_date")

          val HbaseSailorTribeDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim()).option("table", spark.sparkContext.getConf.get("spark.tribesubtribe.table").trim()).
            option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load()

          val HbaseSeawareReservationDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim()).option("table", spark.sparkContext.getConf.get("spark.seawarereservation.table").trim()).
            option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load().as("hbseawareDF")

          val HbasebainsegmentationDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim()).option("table", spark.sparkContext.getConf.get("spark.bainsegmentation.table").trim()).
            option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load().as("hbbainDF")

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------HbaseDF----------------#")
            //HbaseSailorTribeDF.show(false)
            HbaseSailorTribeDF.printSchema
            println("#---------------------------------------HbaseDF----------------#")
          }

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------HbaseSeawareReservationDF----HOUSEHOLD_BASICDEMOGRAPHICS_ESTIMATEDINCOMEMAX------------#")
            //HbaseSeawareReservationDF.show(false)
            //HbaseSeawareReservationDF.printSchema
            println("#---------------------------------------HbaseSeawareReservationDF-------HOUSEHOLD_BASICDEMOGRAPHICS_ESTIMATEDINCOMEMAX---------#")
          }

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------HbasebainsegmentationDF-----------PREDICTEDLABEL-----#")
            HbasebainsegmentationDF.show(false)
            //HbasebainsegmentationDF.printSchema
            println("#---------------------------------------HbasebainsegmentationDF----------PREDICTEDLABEL------#")
          }

          var finalDf = spark.emptyDataFrame

          HbaseSailorTribeDF.createOrReplaceTempView("SailorTribeSubtribeTable")
          var tribesubDf = spark.sql("select * from (select *,row_number() over (partition by SEAWARE_ID order by TIMESTAMP desc,C360ID desc) as rn  from SailorTribeSubtribeTable ) a where a.rn=1 ").as("hbsDF")

          val sailorDf = interimDF.join(tribesubDf, upper(col("targetID")) === upper(col("hbsDF.SEAWARE_ID")), "left")
            .selectExpr("targetID as seaware_id", "sourceID", "cast(person_id as string)as person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "hbsDF.sailor_tribe", "hbsDF.sailor_subtribe", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type", "voyage_id", "batchtime", "part_date").as("saidf")

          //.selectExpr("targetID as seaware_id", "sourceID", "voyage_skey","person_suffix","person_marital_status","person_primary_email", "cast(person_id as string)as person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "address_country", "address_type", "address_mailing", "address_line1", "address_line2", "address_line3", "address_postal_code", "address_city", "address_state", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "cabin_category", "cabin_type", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "guestcheckindetailid", "reservationguest_guid", "reservation_guid","person_booking_charge_id","person_account_id","person_pin_code", "hbsDF.sailor_tribe", "hbsDF.sailor_subtribe", "person_card_type", "person_card_status", "person_card_level", "person_card_color", "person_card_color_value", "voyage_id", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "batchtime", "part_date").as("saidf")

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------sailorDf-----------sailorDf-----#")
            sailorDf.show(false)
            println("#---------------------------------------sailorDf----------sailorDf------#")
          }

          val seawareHBSDF = sailorDf.join(HbaseSeawareReservationDF, upper(col("saidf.seaware_id")) === upper(col("hbseawareDF.SEAWARE_ID")), "left")
            .selectExpr("hbseawareDF.HOUSEHOLD_BASICDEMOGRAPHICS_ESTIMATEDINCOMEMAX as estimated_household_income", "saidf.seaware_id as seaware_id", "sourceID", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "sailor_tribe", "sailor_subtribe", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type", "voyage_id", "batchtime", "part_date").as("seawDF")

          //.selectExpr("hbseawareDF.HOUSEHOLD_BASICDEMOGRAPHICS_ESTIMATEDINCOMEMAX as estimated_household_income", "saidf.seaware_id as seaware_id", "sourceID", "voyage_skey","person_suffix","person_marital_status","person_primary_email", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "address_country", "address_type", "address_mailing", "address_line1", "address_line2", "address_line3", "address_postal_code", "address_city", "address_state", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "cabin_category", "cabin_type", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "guestcheckindetailid", "reservationguest_guid", "reservation_guid","person_booking_charge_id","person_account_id","person_pin_code", "sailor_tribe", "sailor_subtribe", "person_card_type", "person_card_status", "person_card_level", "person_card_color", "person_card_color_value", "voyage_id", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "batchtime", "part_date").as("seawDF")

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------seawareHBSDF----------------#")
            seawareHBSDF.show(false)
            seawareHBSDF.printSchema
            println("#---------------------------------------seawareHBSDF----------------#")
          }

          try {

            finalDf = seawareHBSDF.join(HbasebainsegmentationDF, upper(col("seawDF.seaware_id")) === upper(col("hbbainDF.SEAWAREID")), "left")
              .selectExpr("hbbainDF.PREDICTEDLABEL as bain_segmentation_predicted_label", "estimated_household_income", "seawDF.seaware_id as seaware_id", "sourceID", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_suffix", "person_marital_status", "person_primary_email", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "voyage_skey", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "reservationguest_guid", "reservation_guid", "sailor_tribe", "sailor_subtribe", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "person_booking_charge_id", "person_pin_code", "person_booking_ship_code", "crew_manning_agent", "crew_hire_date", "person_type_name", "booking_vip_status", "booking_vip_tier", "booking_manifest_type", "voyage_id", "batchtime", "part_date")
            //.selectExpr("hbbainDF.PREDICTEDLABEL as bain_segmentation_predicted_label", "estimated_household_income", "seawDF.seaware_id as seaware_id", "sourceID", "voyage_skey","person_suffix","person_marital_status","person_primary_email", "person_id", "person_guid", "person_charge_id", "person_external_id", "account_number", "person_type", "person_gender", "person_title", "person_first_name", "person_middle_name", "person_last_name", "person_dob", "person_age", "person_place_of_birth", "person_country_of_birth", "person_country_of_residence", "person_nationality", "address_country", "address_type", "address_mailing", "address_line1", "address_line2", "address_line3", "address_postal_code", "address_city", "address_state", "booking_arrival_date", "booking_departure_date", "booking_reference", "booking_cruise_number", "cabin_number", "cabin_category", "cabin_type", "mega_rockstar_flag", "vip_flag", "booking_arrival_status", "booking_status", "booking_embarkation_city", "booking_debarkation_city", "booking_id", "guestcheckindetailid", "reservationguest_guid", "reservation_guid","person_booking_charge_id","person_account_id","person_pin_code", "sailor_tribe", "sailor_subtribe", "person_card_type", "person_card_status", "person_card_level", "person_card_color", "person_card_color_value", "voyage_id", "contract_signed_version", "voyage_well_accepted_version", "pre_voyage_health_contract_version", "batchtime", "part_date")

            finalDf.createOrReplaceTempView("finalDfView")
            finalDf.createOrReplaceTempView("finalDf")
            spark.sql("SELECT count(1),person_id,booking_id FROM finalDf group by person_id,booking_id having count(1)>1").show(false)

            val guestemergency = spark.sql(spark.sparkContext.getConf.get("spark.emergencycontact.sql"))
            guestemergency.createOrReplaceTempView("guestemergencyDfView")

            val FinalDff = spark.sql("""SELECT finaldf.bain_segmentation_predicted_label ,finaldf.estimated_household_income ,finaldf.seaware_id ,finaldf.sourceID ,finaldf.person_id ,finaldf.person_guid ,finaldf.person_charge_id ,finaldf.person_external_id ,finaldf.account_number ,finaldf.person_type ,finaldf.person_gender ,finaldf.person_title ,finaldf.person_first_name ,finaldf.person_middle_name ,finaldf.person_last_name ,finaldf.person_suffix ,finaldf.person_marital_status ,finaldf.person_primary_email ,finaldf.person_dob ,finaldf.person_age ,finaldf.person_place_of_birth ,finaldf.person_country_of_birth ,finaldf.person_country_of_residence ,finaldf.person_nationality ,finaldf.voyage_skey ,finaldf.booking_arrival_date ,finaldf.booking_departure_date ,finaldf.booking_reference ,finaldf.booking_cruise_number ,finaldf.cabin_number ,finaldf.mega_rockstar_flag ,finaldf.vip_flag ,finaldf.booking_arrival_status ,finaldf.booking_status ,finaldf.booking_embarkation_city ,finaldf.booking_debarkation_city ,finaldf.booking_id ,finaldf.reservationguest_guid ,finaldf.reservation_guid ,finaldf.sailor_tribe ,finaldf.sailor_subtribe ,finaldf.contract_signed_version ,finaldf.voyage_well_accepted_version ,finaldf.pre_voyage_health_contract_version ,finaldf.person_booking_charge_id ,finaldf.person_pin_code ,finaldf.person_booking_ship_code ,finaldf.crew_manning_agent ,finaldf.crew_hire_date ,finaldf.person_type_name ,finaldf.booking_vip_status ,finaldf.booking_vip_tier ,finaldf.booking_manifest_type ,guestemercontact.emergency_contact_person_name ,guestemercontact.emergency_contact_person_relationship ,guestemercontact.emergency_contact_person_phone_number ,guestemercontact.emergency_contact_person_email ,finaldf.voyage_id ,finaldf.batchtime ,finaldf.part_date FROM ( ( SELECT * FROM finalDfView ) finaldf LEFT JOIN ( SELECT * FROM reservationguestdfView ) resguest ON finaldf.reservation_guid = resguest.reservationguest_reservationid AND finaldf.reservationguest_guid = resguest.reservationguest_guid_reservation LEFT JOIN ( SELECT * FROM guestemergencyDfView ) guestemercontact ON resguest.guestemergencycontactid = guestemercontact.guestemergencycontactid LEFT JOIN ( SELECT * FROM reservationdfView WHERE reservationstatuscode <> 'CN' ) resdf ON resguest.reservationguest_reservationid = resdf.reservation_guid ) """)

            if (debug_flag.trim().toUpperCase().equals("True")) {
              println("#--------------------------------------Final--------- before SCD-------#")
              finalDf.printSchema
              finalDf.show(false)
            }
            if (!FinalDff.head(1).isEmpty) {
              import spark.implicits._
              val ConvertDf = FinalDff.withColumn("vip_flag", col("vip_flag").cast(BooleanType)).withColumn("person_dob", col("person_dob").cast(DateType))
              ConvertDf.createOrReplaceTempView("person_tmp")
              spark.sql("SELECT count(1),person_id,booking_id FROM person_tmp group by person_id,booking_id having count(1)>1").show(false)
              val finalPersondf = spark.sql("select * from (select *,row_number() over (partition by person_id,booking_id order by batchtime desc) as rn from person_tmp) WHERE rn = 1").drop(col("rn")).dropDuplicates()
              //  ConvertDf.dropDuplicates()
              // val finalPersondf=ConvertDf.distinct
              finalPersondf.createOrReplaceTempView("person_tmp1")
              spark.sql("SELECT count(1),person_id,booking_id FROM person_tmp1 group by person_id,booking_id having count(1)>1").show(false)

              loadDimFact(spark, finalPersondf)

            }

            println("#--------------------------------------Quality Check after load-------#")

            val personDimcount = spark.sql("select * from shipdw.hvtb_mart_dim_person").count()
            val srcCount = FinalDff.count
            println("#Source count " + srcCount + "-------#personDimcount" + personDimcount + "---------Failure in quality check person dim integrity compromised ----#")
            if (srcCount < personDimcount) {

              println("#Source count " + srcCount + "-------#personDimcount" + personDimcount + "---------Failure in quality check person dim integrity compromised ----#");
            }

          } catch {
            case e: Exception => {
              ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
              log.info("******************in the catch of Person Dimension Load ******************");
              e.printStackTrace();
              throw new Exception("SQL Exception..please check the stacktrace", e);
              sys.exit(1)

            }

          }

        } // if dataframe is empty
      }

      log.info("Updating Metadata framework")
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
      spark.stop()

    } catch {
      case e: Exception => {
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        log.info("******************in the catch of Person Dimension Load ******************");
        e.printStackTrace();
        throw new Exception("SQL Exception..please check the stacktrace", e);
        spark.stop()

      }

    } // end of catch

  }
}