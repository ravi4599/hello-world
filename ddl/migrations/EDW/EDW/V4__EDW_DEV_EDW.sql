ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

/* <sc-view> VW_PNLSUMMARY_PERIOD </sc-view> */
CREATE OR REPLACE VIEW "VW_PNLSUMMARY_PERIOD"
("FISCAL_PERIOD_NUMBER", "FISCAL_YEAR", "REST_NUMBER", "REST_NAME", "REST_STATUS", "ARG_AREA_NUMBER", "ARG_AREA_NAME", "ARG_DISTRICT_NUMBER", "ARG_DISTRICT_NAME", "ARG_SUBREGION_NUMBER", "ARG_SUBREGION_NAME", "ARG_REGION_NUMBER", "ARG_REGION_NAME", "ACCOUNT_SORT_NUMBER", "CHILD_ACCOUNT_NUMBER", "CHILD_ACCOUNT_TYPE", "CHILD_ACCOUNT_DESC", "PARENT_ACCOUNT_NUMBER", "PARENT_ACCOUNT_DESC", "ACTUAL_PTD_AMOUNT", "BUDGET_PTD_AMOUNT", "ISACTIVE_STORE_VIEW", "ISACTIVE_AREA_VIEW", "ISACTIVE_DISTRICT_VIEW")
AS
SELECT
          PS.FISCAL_PERIOD_NUMBER,
          PS.FISCAL_YEAR,
          RES.REST_NUMBER,
          RES.REST_NAME,
          RES.REST_STATUS,
          RES.ARG_AREA_NUMBER,
          RES.ARG_AREA_NAME,
          RES.ARG_DISTRICT_NUMBER,
          RES.ARG_DISTRICT_NAME,
          RES.ARG_SUBREGION_NUMBER,
          RES.ARG_SUBREGION_NAME,
          RES.ARG_REGION_NUMBER,
          RES.ARG_REGION_NAME,
          PS.ACCOUNT_SORT_NUMBER,
          PS.CHILD_ACCOUNT_NUMBER,
          PS.CHILD_ACCOUNT_TYPE,
          PS.CHILD_ACCOUNT_DESC,
          PS.PARENT_ACCOUNT_NUMBER,
          PS.PARENT_ACCOUNT_DESC,
          PS.ACTUAL_PTD_AMOUNT,
          PS.BUDGET_PTD_AMOUNT,
          PS.ISACTIVE_STORE_VIEW,
          PS.ISACTIVE_AREA_VIEW,
          PS.ISACTIVE_DISTRICT_VIEW
     FROM
          EDW_DEV.EDW.FACT_PNLSUMMARY_PERIOD PS
          JOIN
                    EDW_DEV.EDW.DIM_RESTAURANT RES
             ON PS.REST_NUMBER = RES.REST_NUMBER AND RES.ISCURRENT = 1 AND REST_TYPE = 'Arg';


/* <sc-view> VW_COMP_ADJ_2YRS_DAILY </sc-view> */

CREATE OR REPLACE VIEW "VW_COMP_ADJ_2YRS_DAILY" ("STORENUMBER", "DMA_NAME", "DMA_CODE", "OWNERSHIP_TYPE", "Day", "BUSINESSDATE", "WEEK_NUMBER", "WEEK_START_DATE", "WEEK_END_DATE", "PERIOD_NUMBER", "PERIOD_START_DATE", "PERIOD_END_DATE", "QUARTER_NUMBER", "QUARTER_START_DATE", "QUARTER_END_DATE", "YEAR_NUMBER", "YEAR_START_DATE", "YEAR_END_DATE", "COMP_SALES_TY", "COMP_TRANS_TY", "COMP_SALES_PY", "COMP_TRANS_PY") AS WITH 
Min_Start_Date As 
(
--Select 
--  CASE 
--    WHEN Min_Date <= CAST(Getdate() - 10 AS Date) THEN Min_Date
--    ELSE Cast(Getdate() - 10 AS Date)
--  END As Days_Date 
--From (Select Min(Days_Date) As Min_Date From Dim_Date where Fiscal_Year = (Select Fiscal_Year From Dim_Date Where Days_Date = Cast(GetDate() - 1 AS Date))) A
Select Min(Days_Date) As Days_Date From EDW_DEV.EDW.Dim_Date where Fiscal_Year = (Select Fiscal_Year -1 From EDW_DEV.EDW.Dim_Date Where Days_Date = Cast(Current_Date - 1 AS Date))
),

Min_Store_Open_Date As 
(
Select Cast(Concat(Cast(Fiscal_Year - 3 As Char(4)) , '-10-01') As date) AS Min_Open_Date FROM EDW_DEV.EDW.Dim_Date WHERE Days_Date = Current_date
  
  ),

COMP_TY AS 
(SELECT A.Rest_Number AS storenumber,
       B.Rest_DMA_Desc as DMA_Name,
       B.Rest_DMA_Code as DMA_Code,
       CASE WHEN B.Rest_Type = 'Arg' THEN 'Corporate'
			ELSE 'Franchise'
	   END AS  Ownership_Type,
       Upper(Calendar_Day_Name) As "Day",
       C.Days_Date as businessdate,
       C.Fiscal_Week_Number AS Week_Number,
       C.Fiscal_Week_Start_Date AS Week_Start_Date,
       C.Fiscal_Week_End_Date AS Week_End_Date,
       C.Fiscal_Period_Number AS Period_Number,
       C.Fiscal_Period_Start_Date AS Period_Start_Date,
       C.Fiscal_Period_End_Date AS Period_End_Date,
       C.Fiscal_Quarter_Number AS Quarter_Number,
       C.Fiscal_Quarter_Start_Date AS Quarter_Start_Date,
       C.Fiscal_Quarter_End_Date AS Quarter_End_Date,
       C.Fiscal_Year AS Year_Number,
       C.Fiscal_Year_Start_Date AS Year_Start_Date,
       C.Fiscal_Year_End_Date AS Year_End_Date,
       SalesAmt_TY As Comp_Sales_TY,
       TransCnt_TY As Comp_Trans_TY
FROM EDW_DEV.EDW.Fact_Sales_Day_YOY A 
JOIN EDW_DEV.EDW.Dim_Restaurant B ON A.Restaurant_Key = B.Restaurant_key
JOIN EDW_DEV.EDW.Dim_Date C ON A.Date_Key = C.Date_Key
Where Days_Date >= (Select Days_Date From Min_Start_Date) And B.Rest_OpenDate <= (Select Min_Open_Date FROM Min_Store_Open_Date)
)
,

COMP_PY AS 
(SELECT A.Rest_Number AS storenumber,
        C.Days_Date as businessdate,
       TransCnt_TY As Comp_Trans_PY,
       SalesAmt_TY As Comp_Sales_PY
FROM EDW_DEV.EDW.Fact_Sales_Day_YOY A 
JOIN EDW_DEV.EDW.Dim_Restaurant B ON A.Restaurant_Key = B.Restaurant_key
JOIN EDW_DEV.EDW.Dim_Date C ON A.Date_Key = CAST(REPLACE(TO_VARCHAR(DATEADD(DAY, -364, C.Adjusted_Comparison_Date::DATE)),'-','')AS INTEGER)--Cast(Replace(Add_Days(C.Adjusted_Comparison_Date, -364),'-','') AS INT)

Where C.Days_Date >= (Select Days_Date From Min_Start_Date) And B.Rest_OpenDate <= (Select Min_Open_Date FROM Min_Store_Open_Date)
)
--SELECT A.* FROM COMP_TY A JOIN COMP_PY B ON 1=1
Select A.*,B.Comp_Sales_PY,B.Comp_Trans_PY from COMP_TY A JOIN COMP_PY B ON A.storenumber = B.storenumber And A.businessdate = B.businessdate
Where Comp_Sales_TY > 0 AND Comp_Sales_PY > 0; --And A.businessdate > GetDate() - 4;



/* <sc-view> VW_PNL_SUMMARY_ACCOUNTS </sc-view> */

CREATE OR REPLACE VIEW "VW_PNL_SUMMARY_ACCOUNTS" ("REST_NUMBER", "REST_NAME", "FISCAL_PERIOD_NUMBER", "FISCAL_YEAR", "SALES_ACTUAL_AMT", "SALES_BUDGET_AMT", "MCE_ACTUAL_AMT", "MCE_BUDGET_AMT", "MCI_ACTUAL_AMT", "MCI_BUDGET_AMT", "UCI_ACTUAL_AMT", "UCI_BUDGET_AMT", "TCI_ACTUAL_AMT", "TCI_BUDGET_AMT", "MCE_ACTUAL_AMT %", "MCI_ACTUAL_AMT %", "UCI_ACTUAL_AMT %", "TCI_ACTUAL_AMT %", "MCE_BUDGET_AMT %", "MCI_BUDGET_AMT %", "UCI_BUDGET_AMT %", "TCI_BUDGET_AMT %") AS WITH RESTAURANTS  
AS (  
 SELECT   
  REST_NUMBER,  
  REST_NAME,  
  REST_TYPE,  
  ISCURRENT  
 FROM EDW_DEV.EDW.Dim_Restaurant  
 WHERE REST_TYPE = 'Arg'  
  AND ISCURRENT = 1  
 ),  
PNLSUMMARY_PERIOD  
AS (  
SELECT REST_NUMBER,  
              FISCAL_PERIOD_NUMBER,  
              FISCAL_YEAR,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A06000'  
                                  THEN (ACTUAL_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS SALES_ACTUAL_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A06000'  
                                  THEN (BUDGET_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS SALES_BUDGET_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A26996'  
                                  THEN (ACTUAL_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS MCE_ACTUAL_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A26996'  
                                  THEN (BUDGET_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS MCE_BUDGET_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A26997'  
                                  THEN (ACTUAL_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS MCI_ACTUAL_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A26997'  
                                  THEN (BUDGET_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS MCI_BUDGET_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A16997'  
                                  THEN (ACTUAL_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS UCI_ACTUAL_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A16997'  
                                  THEN (BUDGET_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS UCI_BUDGET_AMT,  
  
       SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A16998'  
                                  THEN (ACTUAL_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS TCI_ACTUAL_AMT,  
              SUM(CASE   
                           WHEN PARENT_ACCOUNT_NUMBER = 'A16998'  
                                  THEN (BUDGET_PTD_AMOUNT)  
                           ELSE 0  
                           END) AS TCI_BUDGET_AMT  
       FROM EDW_DEV.EDW.FACT_PNLSUMMARY_PERIOD  
       GROUP BY REST_NUMBER,  
              FISCAL_PERIOD_NUMBER,  
              FISCAL_YEAR   
 )  
  
 SELECT   
  a.REST_NUMBER,  
  a.REST_NAME,  
  b.FISCAL_PERIOD_NUMBER,  
        b.FISCAL_YEAR,  
  b.SALES_ACTUAL_AMT,  
  b.SALES_BUDGET_AMT,  
  b.MCE_ACTUAL_AMT,  
  b.MCE_BUDGET_AMT,  
  b.MCI_ACTUAL_AMT,   
  b.MCI_BUDGET_AMT,    
  b.UCI_ACTUAL_AMT,   
  b.UCI_BUDGET_AMT,  
  b.TCI_ACTUAL_AMT,  
  b.TCI_BUDGET_AMT,  
Concat(CAST(cast(b.MCE_ACTUAL_AMT as decimal(10,2))/nullif(cast(b.SALES_ACTUAL_AMT as decimal(10,2)),0) as char(15) ) , '%') AS "MCE_ACTUAL_AMT %",  
Concat(CAST(cast(b.MCI_ACTUAL_AMT as decimal(10,2))/nullif(cast(b.SALES_ACTUAL_AMT as decimal(10,2)),0) as char(15) )  , '%') AS "MCI_ACTUAL_AMT %",  
Concat(CAST(cast(b.UCI_ACTUAL_AMT as decimal(10,2))/nullif(cast(b.SALES_ACTUAL_AMT as decimal(10,2)),0) as char(15) )  , '%') AS "UCI_ACTUAL_AMT %",  
Concat(CAST(cast(b.TCI_ACTUAL_AMT as decimal(10,2))/nullif(cast(b.SALES_ACTUAL_AMT as decimal(10,2)),0) as char(15) )  , '%') AS "TCI_ACTUAL_AMT %",  
Concat(CAST(cast(b.MCE_BUDGET_AMT as decimal(10,2))/nullif(cast(b.SALES_BUDGET_AMT as decimal(10,2)),0) as char(15) )  , '%') AS "MCE_BUDGET_AMT %",  
Concat(CAST(cast(b.MCI_BUDGET_AMT as decimal(10,2))/nullif(cast(b.SALES_BUDGET_AMT as decimal(10,2)),0) as char(15) )  , '%') AS "MCI_BUDGET_AMT %",  
Concat(CAST(cast(b.UCI_BUDGET_AMT as decimal(10,2))/nullif(cast(b.SALES_BUDGET_AMT as decimal(10,2)),0) as char(15) )  , '%') AS "UCI_BUDGET_AMT %",                          
Concat(CAST(cast(b.TCI_BUDGET_AMT as decimal(10,2))/nullif(cast(b.SALES_BUDGET_AMT as decimal(10,2)),0) as char(15) )  , '%') AS "TCI_BUDGET_AMT %"                          
  
FROM  RESTAURANTS a JOIN PNLSUMMARY_PERIOD b   
ON a.REST_NUMBER=b.Rest_Number  
;


/* <sc-view> VW_REPORT_TLD_TAX_EXEMPT </sc-view> */
CREATE OR REPLACE VIEW "VW_REPORT_TLD_TAX_EXEMPT"
("REST_NUMBER", "DATE_KEY", "REST_NAME", "REST_ADDRCITY", "REST_ADDRSTATE", "POS_NAME", "ORDER_NUMBER", "ORDER_NAME", "EMPLOYEE_NAME", "TAXEXEMPT_ID", "GUEST_COUNT", "GROSS_QUANTITY", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "SURCHARGE_AMOUNT", "TAX_AMOUNT", "PAYMENT_AMOUNT", "IS_TAXEXEMPT", "IS_VOID", "IS_REFUND")
AS
SELECT
	DR.Rest_Number,
	FSO.Date_Key,
	DR.Rest_Name,
	DR.Rest_AddrCity,
	DR.Rest_AddrState,
	   POS.POS_NAME,
	FSO.Order_Number,
	FSO.Order_Name,
		   IFNULL(DE.Employee_Name,'NA') AS Employee_Name,
	FSO.TAXEXEMPT_ID,
	FSO.GUEST_COUNT,
	FSO.GROSS_QUANTITY,
	FSO.GROSS_AMOUNT,
	FSO.DISCOUNT_AMOUNT,
	FSO.NET_AMOUNT,
	FSO.SURCHARGE_AMOUNT,
	FSO.TAX_AMOUNT,
	FSO.PAYMENT_AMOUNT,
	FSO.IS_TAXEXEMPT,
	FSO.IS_VOID,
	FSO.IS_REFUND

FROM
	EDW_DEV.EDW.FACT_SALES_ORDER FSO
INNER JOIN
		EDW_DEV.EDW.DIM_RESTAURANT DR ON DR.Restaurant_key=FSO.Restaurant_Key
LEFT JOIN
		EDW_DEV.EDW.Dim_Employee DE ON DE.Employee_Key = FSO.Employee_Key
INNER JOIN
		EDW_DEV.EDW.DIM_POINTOFSALE POS ON POS.POS_KEY = FSO.POS_KEY
WHERE FSO.IS_TAXEXEMPT = 1;



/* <sc-view> VW_TLD_FACT_SALES_ORDER_PAYMENT </sc-view> */

CREATE OR REPLACE VIEW "VW_TLD_FACT_SALES_ORDER_PAYMENT" ("ORDER_ID", "ORDER_NUMBER", "ORDER_DATETIME", "ORDER_DATE", "ORDER_TIME", "REST_NUMBER", "REST_NAME", "REST_DMA_CODE", "REST_DMA_DESC", "REST_TYPE", "POS_NAME", "PAYMENT_TYPE_ID", "PAYMENT_DESC", "PAYMENT_AUTH_ID", "PAYMENT_LAST4", "PAYMENT_NAME", "PAYMENT_AMOUNT", "IS_VOID") AS SELECT 
		sop.Order_ID,
        sop.Order_Number,
		TIMESTAMPADD(
			SECOND,
			SECOND(DT.Time_Of_Day_Time),
			TIMESTAMPADD(
				MINUTE,
				MINUTE(DT.Time_Of_Day_Time),
				TIMESTAMPADD(
					HOUR,
					HOUR(DT.Time_Of_Day_Time),
					TO_TIMESTAMP(DD.Days_Date)
				)
			)
		) AS ORDER_DATETIME,
        DD.Days_Date Order_Date,
		DT.Time_Of_Day_Time Order_Time,
		IFNULL(DR.Rest_Number,'NA') AS Rest_Number,
		IFNULL(DR.Rest_Name,'NA') AS Rest_Name,
		DR.Rest_DMA_Code,
		DR.Rest_DMA_Desc,
		DR.Rest_Type,
        pos.POS_Name,	
		sop.Payment_Type_ID,
        sop.Payment_Desc,
        sop.Payment_Auth_ID,
        sop.Payment_Last4,
        sop.Payment_Name,
        sop.Payment_Amount,
        sop.Is_Void
FROM EDW_DEV.EDW.Fact_Sales_Order_Payment sop
INNER JOIN EDW_DEV.EDW.Dim_Date DD
	ON DD.Date_key = sop.Date_Key
INNER JOIN EDW_DEV.EDW.Dim_Time DT
	ON DT.Time_key = sop.Time_Key
INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
	ON DR.Restaurant_key = sop.Restaurant_Key
INNER JOIN EDW_DEV.EDW.Dim_PointOfSale pos ON sop.POS_Key = pos.POS_Key
;



/* <sc-view> VW_DATE </sc-view> */
CREATE OR REPLACE VIEW "VW_DATE" ("DATE_KEY", "DATE_ID", "DAYS_COUNT", "DAYS_DATE", "DAYS_TEXT", "DAYS_NUMBER", "DATE_RTI", "FISCAL_YEAR_START_DATE", "FISCAL_YEAR_END_DATE", "FISCAL_YEAR", "FISCAL_PERIOD_NUMBER", "FISCAL_YEAR_NAME", "FISCAL_YEAR_SHORT_NAME", "FISCAL_YY_SHORT_NAME", "FISCAL_QUARTER_NUMBER", "FISCAL_QUARTER_START_DATE", "FISCAL_QUARTER_END_DATE", "FISCAL_QUARTER_NAME", "FISCAL_QUARTER_SHORT_NAME", "FISCAL_PERIOD_START_DATE", "FISCAL_PERIOD_START_DATE_INT", "FISCAL_PERIOD_END_DATE", "FISCAL_PERIOD_NAME", "FISCAL_PERIOD_SHORT_NAME", "FISCAL_WEEK_IN_PERIOD_NUMBER", "FISCAL_WEEK_NUMBER", "FISCAL_WEEK_END_DATE", "FISCAL_WEEK_START_DATE", "FISCAL_WEEK_START_DATE_INT", "FISCAL_COMPARISON_DATE", "CALENDAR_YEAR", "CALENDAR_MONTH_NUMBER", "CALENDAR_MONTH_NAME", "CALENDAR_SHORT_MONTH_NAME", "CALENDAR_YEAR_START_DATE", "CALENDAR_YEAR_END_DATE", "CALENDAR_QUARTER_NUMBER", "CALENDAR_QUARTER_NAME", "CALENDAR_ABBREVIATED_QUARTER_NAME", "CALENDAR_QUARTER_START_DATE", "CALENDAR_QUARTER_END_DATE", "CALENDAR_MONTH_START_DATE", "CALENDAR_MONTH_END_DATE", "WEEKDAY_FLAG", "WEEKEND_FLAG", "CALENDAR_DAY_NAME", "CALENDAR_SHORT_DAY_NAME", "CALENDAR_WEEK_START_DATE", "CALENDAR_WEEKEND_DATE", "CALENDAR_WEEK_NAME", "CALENDAR_SHORT_WEEK_NAME", "ISO_WEEK_NAME", "ISO_ORD_DATE_NAME", "FISCAL_QUARTER_ID", "FISCAL_PERIOD_ID", "CALENDAR_QUARTER_ID", "CALENDAR_MONTH_ID", "FISCAL_WEEKS_IN_PERIOD_QUANTITY", "Last Year", "2 Years Ago", "3 Years Ago", "4 Years Ago", "YTD", "Last YTD", "2 Years Ago YTD", "3 Years Ago YTD", "4 Years Ago YTD", "YESTERDAY", "FWTD", "FWTD-1", "PTD", "PTD-1", "1stDayOfPeriod", "Last 13 Periods", "Last 13 Weeks", "Last 13 Days", "ADJUSTED_COMPARISON_DATE", "Fiscal QTD", "Fiscal YTD", "ADJUSTED_WEEK_START_DATE", "ADJUSTED_WEEK_END_DATE", "ADJUSTED_PERIOD_START_DATE", "ADJUSTED_PERIOD_END_DATE", "ADJUSTED_QUARTER_START_DATE", "ADJUSTED_QUARTER_END_DATE", "ADJUSTED_YEAR_START_DATE", "ADJUSTED_YEAR_END_DATE", "IS_CHRISTMAS_DAY", "IS_FISCAL_COMP_CHRISTMAS_DAY", "IS_CALENDAR_COMP_CHRISTMAS_DAY", "THANKSGIVING_DATE", "IS_THANKSGIVING_DAY", "IS_FISCAL_COMP_THANKSGIVING_DAY", "IS_CALENDAR_COMP_THANKSGIVING_DAY", "IS_NEWYEARS_DAY", "IS_MLKINGS_BIRTHDAY", "IS_VALENTINES_DAY", "IS_ASHWEDNESDAY", "IS_STPATRICKS_DAY", "IS_GOODFRIDAY", "IS_EASTER", "IS_CINCODEMAYO", "IS_MOTHERS_DAY", "IS_FATHERS_DAY", "IS_INDEPENDENCE_DAY", "IS_LABOR_DAY", "IS_CYBERMONDAY", "IS_CHRISTMAS_EVE", "IS_NEWYEARS_EVE", "IS_LENT") AS WITH CTE_Fiscal_Period  
AS  
(  
SELECT DISTINCT Fiscal_Period_Number Fiscal_Period_Nbr, Fiscal_Period_Start_Date Fiscal_Period_Start_Dt, Fiscal_Period_End_Date Fiscal_Period_End_Dt, Fiscal_Year Fiscal_year_Int  
FROM EDW_DEV.EDW.dim_Date  
WHERE Days_Date <=   (DATEADD(DAY, -1, CURRENT_TIMESTAMP()))
)  
,CTE_Fiscal_Period_Rank  
AS  
(  
SELECT Fiscal_Period_Nbr,  Fiscal_Period_Start_Dt,  Fiscal_Period_End_Dt,  Fiscal_year_Int, ROW_NUMBER() OVER (ORDER BY Fiscal_Period_Start_Dt DESC) r  FROM CTE_Fiscal_Period
)  

,CTE_Fiscal_Period_Last2  
AS  
(  
SELECT * FROM CTE_Fiscal_period_Rank  
WHERE r =2  
)  
SELECT
Date_key
,Date_ID
,Days_Count
,Days_Date
,Days_Text
,Days_Number
,Date_RTI
,Fiscal_Year_Start_Date
,Fiscal_Year_End_Date
,Fiscal_Year
,Fiscal_Period_Number
,Fiscal_Year_Name
,Fiscal_Year_Short_Name
,Fiscal_YY_Short_Name
,Fiscal_Quarter_Number
,Fiscal_Quarter_Start_Date
,Fiscal_Quarter_End_Date
,Fiscal_Quarter_Name
,Fiscal_Quarter_Short_Name
,Fiscal_Period_Start_Date
,Fiscal_Period_Start_Date_Int
,Fiscal_Period_End_Date
,Fiscal_Period_Name
,Fiscal_Period_Short_Name
,Fiscal_Week_In_Period_Number
,Fiscal_Week_Number
,Fiscal_Week_End_Date
,Fiscal_Week_Start_Date
,Fiscal_Week_Start_Date_Int
,Fiscal_Comparison_Date
,Calendar_Year
,Calendar_Month_Number
,Calendar_Month_Name
,Calendar_Short_Month_Name
,Calendar_Year_Start_Date
,Calendar_Year_End_Date
,Calendar_Quarter_Number
,Calendar_Quarter_Name
,Calendar_Abbreviated_Quarter_Name
,Calendar_Quarter_Start_Date
,Calendar_Quarter_End_Date
,Calendar_Month_Start_Date
,Calendar_Month_End_Date
,Weekday_Flag
,Weekend_Flag
,Calendar_Day_Name
,Calendar_Short_Day_Name
,Calendar_Week_Start_Date
,Calendar_WeekEnd_Date
,Calendar_Week_Name
,Calendar_Short_Week_Name
,ISO_Week_Name
,ISO_Ord_Date_Name
,Fiscal_Quarter_Id
,Fiscal_Period_Id
,Calendar_Quarter_Id
,Calendar_Month_Id
,Fiscal_weeks_in_Period_quantity
  ,CASE  
      WHEN Days_Date 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-1)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-1)||'-12-31','YYYY-MM-DD')
      THEN 1  
      ELSE 0  
    END
      AS "Last Year"
 ,CASE  
      WHEN Days_Date 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-2)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-2)||'-12-31','YYYY-MM-DD')
      THEN 1  
      ELSE 0  
    END
      AS "2 Years Ago"
  ,CASE  
      WHEN Days_Date 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-3)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-3)||'-12-31','YYYY-MM-DD')
      THEN 1  
      ELSE 0  
    END
      AS "3 Years Ago"
  ,CASE  
      WHEN Days_Date 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-4)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-4)||'-12-31','YYYY-MM-DD')
      THEN 1  
      ELSE 0  
    END
      AS "4 Years Ago" 
    ,CASE  
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE()))||'-01-01','YYYY-MM-DD') and (DATEADD(DAY, -1, CURRENT_DATE()))
      THEN 1 
      ELSE 0  
 END AS YTD 
  ,CASE  
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-1)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-1,(DATEADD(DAY, -1, CURRENT_DATE())))
      THEN 1  
      ELSE 0  
 END AS "Last YTD" 
   ,CASE  
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-2)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-2,(DATEADD(DAY, -1, CURRENT_DATE())))
	  THEN 1  
      ELSE 0  
 END AS "2 Years Ago YTD" 
 ,CASE  
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-3)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-3,(DATEADD(DAY, -1, CURRENT_DATE())))
      THEN 1  
      ELSE 0  
 END  AS "3 Years Ago YTD"  
  ,CASE  
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-4)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-4,(DATEADD(DAY, -1, CURRENT_DATE())))
      THEN 1   
      ELSE 0
  END AS "4 Years Ago YTD"  
  ,CASE
      WHEN TO_DATE(CURRENT_DATE()-1) = Days_Date
	  THEN 1  
      ELSE 0
  END AS Yesterday 
 ,CASE
      WHEN TO_DATE(CURRENT_DATE()-1) between  Fiscal_Week_Start_Date and Fiscal_Week_End_Date and CURRENT_DATE() >= days_date 
      THEN 1  
      ELSE 0  
    END AS FWTD 
  ,CASE  
      when TO_DATE(CURRENT_DATE()-1)  between DATEADD(WEEK, 1, Fiscal_Week_Start_Date) and DATEADD(WEEK, 1, Fiscal_Week_End_Date) 
      THEN 1  
      ELSE 0  
    END AS "FWTD-1" 
,CASE  
    WHEN TO_DATE(CURRENT_DATE()-1) BETWEEN Fiscal_Period_Start_Date AND Fiscal_Period_End_Date AND CURRENT_DATE() >= days_date  
      THEN 1  
      ELSE 0  
    END AS PTD  
,CASE WHEN fpl.fiscal_period_Nbr IS NOT NULL THEN 1 ELSE 0 END  AS "PTD-1"
 ,CASE  
    WHEN CURRENT_DATE() = Fiscal_Period_Start_Date
      THEN 1  
      ELSE 0  
    END AS "1stDayOfPeriod"
 ,CASE  
      WHEN Fiscal_Period_Start_Date >= DATE_TRUNC('MONTH', DATEADD(MONTH, -13, CURRENT_DATE()))AND days_date <= (CURRENT_DATE() -1)
      THEN  1  
      ELSE  0  
  END AS "Last 13 Periods"
  ,CASE  
     WHEN Fiscal_Week_Start_Date >= DATEADD(WEEK, -13, Fiscal_Week_Start_Date)  AND  days_date <= (CURRENT_DATE() -1)
      THEN 1 
      ELSE 0 
    END AS "Last 13 Weeks"  
 ,CASE  
      WHEN days_date BETWEEN DATEADD(DAY, -13, Fiscal_Week_Start_Date) AND (CURRENT_DATE() -1)
      THEN  1  
      ELSE  0  
    END  AS "Last 13 Days" 
,Adjusted_Comparison_Date
 ,CASE  
   WHEN (CURRENT_DATE()) BETWEEN Fiscal_Quarter_Start_Date AND Fiscal_Quarter_End_Date AND days_date <= (CURRENT_DATE() -1)
      THEN 1 
      ELSE 0  
   END AS "Fiscal QTD"  
,CASE  
    WHEN Days_Date BETWEEN Fiscal_Year_Start_Date AND (CURRENT_DATE() -1) AND  YEAR(Fiscal_Year_Start_Date) =  
             (SELECT  
                Fiscal_Year  
              FROM EDW_DEV.EDW.dim_date  
              WHERE  (CURRENT_DATE() -1) BETWEEN fiscal_year_start_date AND Fiscal_year_end_Date  
              GROUP BY fiscal_year)  
      -- OLD LOGIC YEAR(DATEADD(dd,-1,GETDATE()))  
      THEN 1  
      ELSE 0 
    END AS "Fiscal YTD"
,Adjusted_Week_Start_Date
,Adjusted_Week_End_Date
,Adjusted_Period_Start_Date
,Adjusted_Period_End_Date
,Adjusted_Quarter_Start_Date
,Adjusted_Quarter_End_Date
,Adjusted_Year_Start_Date
,Adjusted_Year_End_Date
,Is_Christmas_Day
,Is_Fiscal_Comp_Christmas_Day
,Is_Calendar_Comp_Christmas_Day
,Thanksgiving_Date
,Is_Thanksgiving_Day
,Is_Fiscal_Comp_Thanksgiving_Day
,Is_Calendar_Comp_Thanksgiving_Day
,Is_NewYears_Day
,Is_MLKings_Birthday
,Is_Valentines_Day
,Is_AshWednesday
,Is_StPatricks_Day
,Is_GoodFriday
,Is_Easter
,Is_CincoDeMayo
,Is_Mothers_Day
,Is_Fathers_Day
,Is_Independence_Day
,Is_Labor_Day
,Is_CyberMonday
,Is_Christmas_Eve
,Is_NewYears_Eve
,Is_Lent

FROM EDW_DEV.EDW.Dim_Date DD
LEFT JOIN Cte_Fiscal_Period_Last2 fpl ON dd.Fiscal_Period_Number = fpl.Fiscal_period_Nbr AND dd.Fiscal_Year = fpl.fiscal_year_Int ;



/* <sc-view> VW_REST_TEMPCLOSURE </sc-view> */
CREATE OR REPLACE VIEW "VW_REST_TEMPCLOSURE"
("RESTAURANT_KEY", "CLOSE_DT", "OPEN_DT")
AS
SELECT
Restaurant_key,
Rest_Temp_CloseDate  Close_DT,
Rest_Temp_OpenDate Open_DT
FROM
EDW_DEV.EDW.Dim_Restaurant
WHERE IsCurrent = 1 AND Rest_Temp_CloseDate <> '1753-01-01';



/* <sc-view> VW_TLD_ITEM_PROMOTED_WEEK </sc-view> */
CREATE OR REPLACE VIEW "VW_TLD_ITEM_PROMOTED_WEEK"
("DAYS_DATE", "FISCAL_YEAR", "FISCAL_WEEK_NUMBER", "REST_NUMBER", "REST_DMA_CODE", "REST_DMA_DESC", "REST_TYPE", "PLU_NUMBER", "PLU_DESC", "ITEM_ID", "ITEM_DESC", "ITEM_PRODUCT_ID", "ITEM_PRODUCT_DESC", "COMPONENT_ID", "COMPONENT_NAME", "ITEM_TYPE_CODE", "POS_NAME", "POS_ITEM_DESC", "MENU_GROUP", "MENU_TYPE", "MAIN_GROUP", "PROMO_PRIMARY_TV", "PROMO_SECONDARY_TV", "PROMO_INSTORE", "DAYS_COUNT", "GROSS_QUANTITY", "GROSS_ORDER_COUNT", "GROSS_AMOUNT", "NET_AMOUNT", "NET_ORDER_AMOUNT")
AS
WITH CTE_Order_Amt
AS
(
       SELECT
              CAST(TRUNC(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') ) AS INT)  AS Fiscal_Week_Start_Date_Key,
              DR.Rest_Number,
              COUNT(DISTINCT dd.Date_key) AS Days_Count
       FROM
              EDW_DEV.EDW.Fact_Sales_Order FSO
       INNER JOIN
                     EDW_DEV.EDW.Dim_Date DD
              ON FSO.Date_Key = DD.Date_key
       INNER JOIN
                     EDW_DEV.EDW.Dim_Restaurant DR
              ON DR.Restaurant_key = FSO.Restaurant_Key
       WHERE FSO.Payment_Amount > 0
       GROUP BY CAST(TRUNC(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') ) AS INT) ,
              	 DR.Rest_Number
)
SELECT
       dd.DAYS_DATE,
       dd.Fiscal_Year,
       dd.Fiscal_Week_Number,
       res.Rest_Number,
       res.Rest_DMA_Code,
       res.Rest_DMA_Desc,
          res.Rest_Type,
       mi.PLU_Number,
       mi.PLU_Desc,
       mi.Item_ID,
       mi.Item_Desc,
       mi.Item_Product_ID,
       mi.Item_Product_Desc,
          mi.Component_ID,
          mi.Component_Name,
       mi.Item_Type_Code,
       mi.POS_Name,
       mi.POS_Item_Desc,
       mi.Menu_Group,
       mi.Menu_Type,
       mi.Main_Group,
              IFNULL(p.Primary_TV,'NA') Promo_Primary_TV,
              IFNULL(p.Secondary_TV,'NA') Promo_Secondary_TV,
              IFNULL(p.InStore,'NA') Promo_InStore,
                 IFNULL(OA.Days_Count,0) AS Days_Count,
       iw.Gross_Quantity,
       iw.Gross_Order_Count,
       iw.Gross_Amount,
       iw.Net_Amount - iw.Discount_Amount AS Net_Amount,
       iw.Net_Order_Amount
FROM
       EDW_DEV.EDW.Fact_Sales_Item_Week iw
    INNER JOIN
              EDW_DEV.EDW.Dim_Date dd ON iw.Date_Key = dd.Date_key
     INNER JOIN
              EDW_DEV.EDW.Dim_Restaurant res ON iw.Restaurant_Key = res.Restaurant_key
     INNER JOIN
              EDW_DEV.EDW.Dim_MenuItem mi ON iw.Menu_Key = mi.Menu_key
     LEFT JOIN
              EDW_DEV.EDW.Fact_Promotions_Week p
        ON     mi.Item_Product_ID = p.Item_Product_ID
           AND iw.date_key =p.Date_Key
           AND res.Rest_DMA_Code = p.Rest_DMA_Code
        LEFT JOIN CTE_Order_Amt OA
       	ON res.Rest_Number = OA.Rest_Number
       	AND iw.Date_Key = OA.Fiscal_Week_Start_Date_Key;


/* <sc-view> VW_COMPSALES_DAYPART_REVCTR </sc-view> */

CREATE OR REPLACE VIEW "VW_COMPSALES_DAYPART_REVCTR" 
("DAYS_DATE", "DAYPART_KEY", "DAYPART_DESCRIPTION", "REST_NUMBER", "REST_TYPE", "DATE_KEY", "REMODEL_DATE", "ACQUISITION_DATE", "POP_KEY", "REVENUE_CENTER_DESC", "COMP_SALES_TY_TY", "COMP_SALES_TY_TY_ADJ", "COMP_TRANS_TY_TY", "COMP_TRANS_TY_TY_ADJ", "COMP_SALES_LY_TY", "COMP_SALES_LY_TY_ADJ", "COMP_TRANS_LY_TY", "COMP_TRANS_LY_TY_ADJ", "COMP_SALES_TY_LY", "COMP_SALES_TY_LY_ADJ", "COMP_TRANS_TY_LY", "COMP_TRANS_TY_LY_ADJ", "COMP_SALES_LY_LY", "COMP_SALES_LY_LY_ADJ", "COMP_TRANS_LY_LY", "COMP_TRANS_LY_LY_ADJ", "SALESAMT_TY", "TRANSCNT_TY", "SALESAMT_LY", "SALESAMT_LY_ADJ", "TRANSCNT_LY", "TRANSCNT_LY_ADJ", "SALESAMT_PY", "SALESAMT_PY_ADJ", "TRANSCNT_PY", "TRANSCNT_PY_ADJ", "WEEKDAY_FLAG", "WEEKEND_FLAG") 
AS 
SELECT 
       DD.Days_Date,
	   DP.DayPart_Key,
       DP.DayPart_Description,
       Comp.Rest_Number,
	   DR.Rest_Type,
       DD.Date_Key,
	   TO_DATE(CAST(DR.Rest_RemodelDT AS VARCHAR(30)),'YYYYMMDD') AS Remodel_Date,
	   TO_DATE(CAST(DR.Rest_AcquisitionDt AS VARCHAR(30)),'YYYYMMDD') AS Acquisition_Date,
	   POP.POP_key,
       POP.POP_Desc AS Revenue_Center_Desc,
       Comp.SalesAmt_TY_TY AS Comp_Sales_TY_TY,
	   Comp.SalesAmt_TY_TY_adj AS Comp_Sales_TY_TY_adj,
       Comp.TransCnt_TY_TY AS Comp_Trans_TY_TY,
	   Comp.TransCnt_TY_TY_adj AS Comp_Trans_TY_TY_adj,
       Comp.SalesAmt_LY_TY AS Comp_Sales_LY_TY,
	   Comp.SalesAmt_LY_TY_adj AS Comp_Sales_LY_TY_adj,
       Comp.TransCnt_LY_TY AS Comp_Trans_LY_TY,
	   Comp.TransCnt_LY_TY_adj AS Comp_Trans_LY_TY_adj,
       Comp.SalesAmt_TY_LY AS Comp_Sales_TY_LY,
       Comp.SalesAmt_TY_LY_adj AS Comp_Sales_TY_LY_adj,
       Comp.TransCnt_TY_LY AS Comp_Trans_TY_LY,
       Comp.TransCnt_TY_LY_adj AS Comp_Trans_TY_LY_adj,
       Comp.SalesAmt_LY_LY AS Comp_Sales_LY_LY,
       Comp.SalesAmt_LY_LY_adj AS Comp_Sales_LY_LY_adj,
       Comp.TransCnt_LY_LY AS Comp_Trans_LY_LY,
       Comp.TransCnt_LY_LY_adj AS Comp_Trans_LY_LY_adj,
	   Total.SalesAmt_TY,
	   Total.TransCnt_TY,
	   Total.SalesAmt_LY,
	   Total.SalesAmt_LY_adj,
	   Total.TransCnt_LY,
	   Total.TransCnt_LY_adj,
	   Total.SalesAmt_PY,
	   Total.SalesAmt_PY_adj,
	   Total.TransCnt_PY,
	   Total.TransCnt_PY_adj,
       DD.Weekday_Flag,
       DD.Weekend_Flag 
FROM EDW_DEV.EDW.Fact_CompSales_DayPart Comp
INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
	ON DR.Restaurant_key = Comp.Restaurant_Key
INNER JOIN EDW_DEV.EDW.Dim_Date DD
	ON DD.Date_key = Comp.Date_Key
INNER JOIN EDW_DEV.EDW.Dim_PointOfPurchase POP
	ON POP.POP_key = Comp.POP_Key
INNER JOIN EDW_DEV.EDW.Dim_DayPart DP
	ON DP.DayPart_Key = Comp.DayPart_Key
LEFT JOIN EDW_DEV.EDW.Fact_Sales_DayPart_YOY Total
	ON Total.Date_Key = Comp.Date_Key
	AND Total.DayPart_Key = Comp.DayPart_Key
	AND Total.POP_Key = Comp.POP_Key
	AND Total.Rest_Number = Comp.Rest_Number


;


/* <sc-view> ZVW_COMPSALES_DAYPART </sc-view> */
CREATE OR REPLACE VIEW "ZVW_COMPSALES_DAYPART"
("DAYS_DATE", "DAYPART_KEY", "DAYPART_DESCRIPTION", "REST_NUMBER", "REST_TYPE", "DATE_KEY", "REMODEL_DATE", "ACQUISITION_DATE", "POP_KEY", "REVENUE_CENTER_DESC", "COMP_SALES_TY_TY", "COMP_SALES_TY_TY_ADJ", "COMP_TRANS_TY_TY", "COMP_TRANS_TY_TY_ADJ", "COMP_SALES_LY_TY", "COMP_SALES_LY_TY_ADJ", "COMP_TRANS_LY_TY", "COMP_TRANS_LY_TY_ADJ", "COMP_SALES_TY_LY", "COMP_SALES_TY_LY_ADJ", "COMP_TRANS_TY_LY", "COMP_TRANS_TY_LY_ADJ", "COMP_SALES_LY_LY", "COMP_SALES_LY_LY_ADJ", "COMP_TRANS_LY_LY", "COMP_TRANS_LY_LY_ADJ", "SALESAMT_TY", "TRANSCNT_TY", "SALESAMT_LY", "SALESAMT_LY_ADJ", "TRANSCNT_LY", "TRANSCNT_LY_ADJ", "SALESAMT_PY", "SALESAMT_PY_ADJ", "TRANSCNT_PY", "TRANSCNT_PY_ADJ", "CHECKSIZE_DESC", "AUV", "AUV_DESC", "WEEKDAY_FLAG", "WEEKEND_FLAG")
AS
--Date: 8/14/2018
--Process:  Comp Sales by DayPart, Restaurant, Revenue Center, weekday and weekend.
--Modified Date:6/3/2019
--Modified By: Jack Chen
--Modified Part: Comment out all 2 year ago part since revenue center is not ready and return TY data >= 20180601 8/22/2018 
--               Meger with TLD to find missing revenue center 11/27/18
--				 Create new table Fact_Sales_Hr as base table 11/28/2018 
--	             Update Comp trans and check ifnull <>0 4/19/2019
--				 remodel YOY and Comp  6/3/2019
WITH CTE_AUV
AS
(
	SELECT
		DD.Fiscal_Year,
		DD.Fiscal_Year +1 AS Next_Fiscal_Year,
		YOY.DayPart_Key,
		YOY.Rest_Number,
		YOY.POP_Key,
		CAST(AVG(SalesAmt_TY) AS NUMERIC(18,2)) AS SalesAmt_TY
FROM
		EDW_DEV.EDW.Fact_Sales_DayPart_YOY YOY
INNER JOIN
			EDW_DEV.EDW.Dim_Date DD
	ON DD.DATE_KEY = YOY.Date_Key
WHERE SalesAmt_TY != 0
GROUP BY DD.Fiscal_Year,
		          YOY.DayPart_Key,
		          YOY.Rest_Number,
		          YOY.POP_Key

)
SELECT
       DD.Days_Date,
       DP.DayPart_Key,
          DP.DayPart_Description,
          Comp.Rest_Number,
       DR.Rest_Type,
          DD.DATE_KEY,
       TO_DATE(LEFT(CAST(DR.Rest_RemodelDT AS VARCHAR), 30),'YYYYMMDD') AS Remodel_Date,
       TO_DATE(LEFT(CAST(DR.Rest_AcquisitionDt AS VARCHAR), 30),'YYYYMMDD') AS Acquisition_Date,
       POP.POP_key,
          POP.POP_Desc AS Revenue_Center_Desc,
          Comp.SalesAmt_TY_TY AS Comp_Sales_TY_TY,
       Comp.SalesAmt_TY_TY_adj AS Comp_Sales_TY_TY_adj,
          Comp.TransCnt_TY_TY AS Comp_Trans_TY_TY,
       Comp.TransCnt_TY_TY_adj AS Comp_Trans_TY_TY_adj,
          Comp.SalesAmt_LY_TY AS Comp_Sales_LY_TY,
       Comp.SalesAmt_LY_TY_adj AS Comp_Sales_LY_TY_adj,
          Comp.TransCnt_LY_TY AS Comp_Trans_LY_TY,
       Comp.TransCnt_LY_TY_adj AS Comp_Trans_LY_TY_adj,
          Comp.SalesAmt_TY_LY AS Comp_Sales_TY_LY,
          Comp.SalesAmt_TY_LY_adj AS Comp_Sales_TY_LY_adj,
          Comp.TransCnt_TY_LY AS Comp_Trans_TY_LY,
          Comp.TransCnt_TY_LY_adj AS Comp_Trans_TY_LY_adj,
          Comp.SalesAmt_LY_LY AS Comp_Sales_LY_LY,
          Comp.SalesAmt_LY_LY_adj AS Comp_Sales_LY_LY_adj,
          Comp.TransCnt_LY_LY AS Comp_Trans_LY_LY,
          Comp.TransCnt_LY_LY_adj AS Comp_Trans_LY_LY_adj,
       Total.SalesAmt_TY,
       Total.TransCnt_TY,
       Total.SalesAmt_LY,
       Total.SalesAmt_LY_adj,
       Total.TransCnt_LY,
       Total.TransCnt_LY_adj,
       Total.SalesAmt_PY,
       Total.SalesAmt_PY_adj,
       Total.TransCnt_PY,
       Total.TransCnt_PY_adj,
              IFNULL(DCS.CheckSize_Desc,'NA') AS CheckSize_Desc,
       AUV.SalesAmt_TY  AS AUV,
              IFNULL(DAUV.AUV_Desc,'NA') AS AUV_Desc,
          DD.Weekday_Flag,
          DD.Weekend_Flag
   FROM
       EDW_DEV.EDW.Fact_CompSales_DayPart Comp
   INNER JOIN
		EDW_DEV.EDW.Dim_Restaurant DR
    ON DR.Restaurant_key = Comp.Restaurant_Key
   INNER JOIN
		EDW_DEV.EDW.Dim_Date DD
    ON DD.DATE_KEY = Comp.Date_Key
   INNER JOIN
		EDW_DEV.EDW.Dim_PointOfPurchase POP
    ON POP.POP_key = Comp.POP_Key
   INNER JOIN
		EDW_DEV.EDW.Dim_DayPart DP
    ON DP.DayPart_Key = Comp.DayPart_Key
   LEFT JOIN
		EDW_DEV.EDW.Fact_Sales_DayPart_YOY Total
    ON Total.Date_Key = Comp.Date_Key
    AND Total.DayPart_Key = Comp.DayPart_Key
    AND Total.POP_Key = Comp.POP_Key
    AND Total.Rest_Number = Comp.Rest_Number
   LEFT JOIN
		EDW_DEV.EDW.Dim_CheckSize DCS
    ON CASE
			WHEN Total.TransCnt_TY != 0
				THEN CAST(Total.SalesAmt_TY/ Total.TransCnt_TY AS NUMERIC(18,2))
      ELSE 0
		END BETWEEN DCS.Low_Value AND DCS.High_Value
   LEFT JOIN CTE_AUV AUV
    ON AUV.DayPart_Key = Comp.DayPart_Key
    AND AUV.Next_Fiscal_Year = DD.Fiscal_Year
    AND AUV.Rest_Number = Comp.Rest_Number
    AND AUV.POP_Key = Comp.POP_Key
   LEFT JOIN
		EDW_DEV.EDW.Dim_AUV DAUV
    ON AUV.SalesAmt_TY BETWEEN  DAUV.Low_Value AND DAUV.High_Value --CREATE BY: Jack Chen 

		;




/* <sc-view> VW_COMP_DIVESTITUREDATES </sc-view> */
CREATE OR REPLACE VIEW "VW_COMP_DIVESTITUREDATES"
("REST_SNBR", "DIVESTITURE_DATE")
AS
WITH CTE_All
AS
(
 SELECT
  Rest_Number,
  Rest_AcquisitionDt,
  Rest_Type,
    IFNULL(LAG(Rest_TYPE) OVER (PARTITION BY Rest_Number ORDER BY Effective_Begin_Date), Rest_Type) Rest_Type_LAG,
  Rest_Fran_Number,
  ROW_NUMBER () OVER (PARTITION BY Rest_Number ORDER BY Effective_Begin_Date DESC NULLS LAST) r,
  Effective_Begin_Date,
  Effective_End_Date,
  IsCurrent
  FROM
  EDW_DEV.EDW.Dim_Restaurant
),
CTE_LAG
AS
(
 SELECT
  Rest_Number,
  MAX(Rest_AcquisitionDt) Rest_AcquisitionDt,
  Rest_Type,
  MIN(Rest_Type_LAG) Rest_Type_LAG,
  Rest_Fran_Number,
  MIN(r) r
  --,IsCurrent
  FROM CTE_All
  GROUP BY Rest_Number,Rest_Type, Rest_Fran_Number
)
SELECT
 LEFT(
 CAST(Rest_Number AS VARCHAR), 5) Rest_sNbr,
 CAST(
            IFNULL(DD.DAYS_DATE,'1753-01-01') AS TIMESTAMP)  Divestiture_Date
FROM CTE_LAG CL
LEFT JOIN
  EDW_DEV.EDW.DIM_DATE DD
ON CL.Rest_AcquisitionDt = DD."DATE_KEY"
WHERE r = 1 AND Rest_Type_LAG = 'Arg'
 AND rest_type = 'Fra'
AND Rest_AcquisitionDt >= 20161107;



/* <sc-view> VW_FACT_SOS_BYCAR </sc-view> */

CREATE OR REPLACE VIEW "VW_FACT_SOS_BYCAR" 
("FACT_SOS_BYCAR_KEY", "RESTAURANT_KEY", "DATE_KEY", "SPEAKER_ARR_DATEKEY", "SPEAKER_ARR_TIMEKEY", "SPEAKER_DEPART_DATEKEY", "SPEAKER_DEPART_TIMEKEY", "WINDOW_ARR_DATEKEY", "WINDOW_ARR_TIMEKEY", "WINDOW_DEPART_DATEKEY", "WINDOW_DEPART_TIMEKEY", "LOAD_ID", "LAST_UPDATE_DATE_TIME") 
AS 
  WITH PreviousFiscalMonth    
  AS      
  (      
  SELECT  Fiscal_Period_Id as PeriodID FROM EDW_DEV.EDW.DIM_DATE WHERE Days_Date=Current_Date
  )      
         
  ,FiscalPeriods      
  AS      
  (      
  SELECT   Distinct Fiscal_Period_Id FROM EDW_DEV.EDW.Dim_Date WHERE Fiscal_Period_Id <= (SELECT PeriodID FROM PreviousFiscalMonth)      
  ORDER BY Fiscal_Period_Id Desc  
  Limit 13    
  )  
  ,FiscalDates  
  AS  
  (  
 SELECT Date_key  FROM EDW_DEV.EDW.Dim_Date WHERE Fiscal_Period_Id IN (SELECT Fiscal_Period_Id from FiscalPeriods)  
  )
   
    
select *  from EDW_DEV.EDW.Fact_SpeedOfService_ByCar  WHERE Date_key IN  (SELECT Date_key  from FiscalDates) ;



/* <sc-view> VW_TLD_ITEM_PROMOTED_WEEK_BY_DMA </sc-view> */

CREATE OR REPLACE VIEW "VW_TLD_ITEM_PROMOTED_WEEK_BY_DMA" ("DAYS_DATE", "FISCAL_YEAR", "FISCAL_WEEK_NUMBER", "REST_DMA_CODE", "REST_DMA_DESC", "REST_TYPE", "ITEM_PRODUCT_DESC", "PROMO_PRIMARY_TV", "PROMO_SECONDARY_TV", "PROMO_INSTORE", "DAYS_COUNT", "GROSS_QUANTITY", "GROSS_ORDER_COUNT", "GROSS_AMOUNT", "NET_AMOUNT", "NET_ORDER_AMOUNT", "U/S/D", "Item Mix %") AS WITH CTE_Order_Amt
AS
(
		SELECT 
			CAST(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') AS INT) AS Fiscal_Week_Start_Date_Key,
			DR.Rest_Number,
			DR.Rest_DMA_Code,
			COUNT(DISTINCT dd.Date_key) AS Days_Count 
		FROM EDW_DEV.EDW.Fact_Sales_Order FSO
		INNER JOIN EDW_DEV.EDW.Dim_Date DD
			ON FSO.Date_Key = DD.Date_key
		INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
			ON DR.Restaurant_key = FSO.Restaurant_Key
		WHERE FSO.Payment_Amount > 0
		GROUP BY CAST(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') AS INT),
				 DR.Rest_Number,
				DR.Rest_DMA_Code
)

,CTE_Total_Amount
AS
( 
		SELECT Date_Key,
			   DR.Rest_DMA_Code,
			   SUM(FSI.Net_Amount) - SUM(FSI.Discount_Amount) AS Total_Amount
		FROM EDW_DEV.EDW.Fact_Sales_Item_Week FSI
		INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
			ON DR.Restaurant_key = FSI.Restaurant_Key 

		GROUP BY FSI.Date_Key,
                 DR.Rest_DMA_Code
)

SELECT dd.DAYS_DATE,
       dd.Fiscal_Year,
       dd.Fiscal_Week_Number,
       res.Rest_DMA_Code,
       res.Rest_DMA_Desc,
	   res.Rest_Type,
       mi.Item_Product_Desc,
       IFNULL(p.Primary_TV,'NA') Promo_Primary_TV,
       IFNULL(p.Secondary_TV,'NA') Promo_Secondary_TV,
       IFNULL(p.InStore,'NA') Promo_InStore,
	   AVG(IFNULL(OA.Days_Count,0)) AS Days_Count,
       SUM(iw.Gross_Quantity) AS Gross_Quantity,
	   SUM(iw.Gross_Order_Count) AS Gross_Order_Count,
       SUM(iw.Gross_Amount) AS Gross_Amount,
       SUM(iw.Net_Amount - iw.Discount_Amount) AS Net_Amount,
	   SUM(iw.Net_Order_Amount) AS Net_Order_Amount,
	   CASE WHEN SUM(IFNULL(OA.Days_Count,0))  = 0
				THEN 0
			ELSE SUM(iw.Gross_Quantity)/AVG(IFNULL(OA.Days_Count,0))/COUNT(DISTINCT res.Rest_Number) 
	   END AS "U/S/D",
	
	   CASE WHEN IFNULL(CTA.Total_Amount,0) = 0
				THEN 0
			ELSE (
				 SUM( 
					CASE WHEN LOWER(mi.Item_Product_Desc) LIKE '%meal%'
							THEN (iw.Net_Amount - iw.Discount_Amount)+ 3.3 * iw.Gross_Quantity
						 ELSE (iw.Net_Amount - iw.Discount_Amount)
					END
				  ) /  IFNULL(CTA.Total_Amount,0)
				  )
	   END AS "Item Mix %"
FROM EDW_DEV.EDW.Fact_Sales_Item_Week iw
    INNER JOIN EDW_DEV.EDW.Dim_Date dd ON iw.Date_Key = dd.Date_key
     INNER JOIN EDW_DEV.EDW.Dim_Restaurant res ON iw.Restaurant_Key = res.Restaurant_key
     INNER JOIN EDW_DEV.EDW.Dim_MenuItem mi ON iw.Menu_Key = mi.Menu_key
     LEFT JOIN EDW_DEV.EDW.Fact_Promotions_Week p
        ON     mi.Item_Product_ID = p.Item_Product_ID
           AND iw.date_key =p.Date_Key
           AND res.Rest_DMA_Code = p.Rest_DMA_Code

	 LEFT JOIN CTE_Total_Amount CTA
		ON CTA.Rest_DMA_Code = res.Rest_DMA_Code
		AND CTA.Date_Key = iw.Date_Key  
	 LEFT JOIN CTE_Order_Amt OA
		ON res.Rest_Number = OA.Rest_Number
		AND iw.Date_Key = OA.Fiscal_Week_Start_Date_Key


GROUP BY IFNULL(p.Primary_TV, 'NA'),
         IFNULL(p.Secondary_TV, 'NA'),
         IFNULL(p.InStore, 'NA'),
         dd.Fiscal_Year,
         dd.Fiscal_Week_Number,
         res.Rest_DMA_Code,
         res.Rest_DMA_Desc,
	     dd.DAYS_DATE,
         res.Rest_Type,
         mi.Item_Product_Desc,
         IFNULL(CTA.Total_Amount,0)




;



/* <sc-view> VW_TLD_ITEM_AGG_WEEK </sc-view> */

CREATE OR REPLACE VIEW "VW_TLD_ITEM_AGG_WEEK" ("FISCAL_YEAR", "FISCAL_PERIOD_NUMBER", "FISCAL_WEEK_NUMBER", "FISCAL_WEEK_START_DATE", "REST_NUMBER", "REST_NAME", "REST_DMA_CODE", "REST_DMA", "REST_TYPE", "POS_NAME", "ITEM_NUMBER", "PLU_NUMBER", "PLU_DESC", "PROMO_CODE", "PROMO_DESC", "ITEM_ID", "ITEM_DESC", "ITEM_PRODUCT_ID", "ITEM_PRODUCT_DESC", "ITEM_TYPE_CODE", "COMPONENT_ID", "COMPONENT_NAME", "COUPON_NAME", "COUPON_TYPE", "MENU_GROUP", "MENU_TYPE", "MAIN_GROUP", "MAIN_PROTEIN", "PRIMARY_TV", "SECONDARY_TV", "INSTORE", "DAYS_COUNT", "GROSS_QUANTITY", "GROSS_ORDER_COUNT", "GROSS_AMOUNT", "NET_AMOUNT", "NET_ORDER_AMOUNT") AS WITH CTE_Order_Amt
AS
(
		SELECT 
			CAST(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') AS INT) AS Fiscal_Week_Start_Date_Key,
			DR.Rest_Number,
			COUNT(DISTINCT dd.Date_key) AS Days_Count 
		FROM EDW_DEV.EDW.Fact_Sales_Order FSO
		INNER JOIN EDW_DEV.EDW.Dim_Date DD
			ON FSO.Date_Key = DD.Date_key
		INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
			ON DR.Restaurant_key = FSO.Restaurant_Key
		WHERE FSO.Payment_Amount > 0
		GROUP BY CAST(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') AS INT),
				 DR.Rest_Number
)


SELECT 
       dd.Fiscal_Year,
	   dd.Fiscal_Period_Number,
       dd.Fiscal_Week_Number,
	   dd.Fiscal_Week_Start_Date,
       res.Rest_Number,
	   res.Rest_Name,
       res.Rest_DMA_Code,
       res.Rest_DMA_Desc  AS Rest_DMA,
	   res.Rest_Type,
       mi.POS_Name,
      CASE WHEN MI.POS_Name ='Xpient' THEN TO_Char(MI.Item_ID)
			 WHEN MI.Promo_Code = '0' AND MI.POS_Name <> 'Xpient'  THEN CONCAT('-' , MI.PLU_Number) 
			 WHEN MI.Promo_Code <> '0' AND MI.POS_Name <> 'Xpient' THEN CONCAT('-',mi.Promo_Code,MI.PLU_Number)
		END AS Item_Number,
       mi.PLU_Number,
       mi.PLU_Desc,
	  mi.Promo_Code,
	  mi.Promo_Desc,      
	   mi.Item_ID,
       mi.Item_Desc,

       mi.Item_Product_ID,
       mi.Item_Product_Desc,
	  MI.Item_Type_Code,
	  MI.Component_ID,
	  MI.Component_Name,
	  MI.Coupon_Name,
	  MI.Coupon_Type,
	  MI.Menu_Group,
	  MI.Menu_Type,
	  MI.Main_Group,
	  MI.Main_Protein,
       IFNULL(p.Primary_TV,'NA') Primary_TV,
       IFNULL(p.Secondary_TV,'NA') Secondary_TV,
       IFNULL(p.InStore,'NA') InStore,
	   IFNULL(OA.Days_Count,0) AS Days_Count,
       iw.Gross_Quantity,
       iw.Gross_Order_Count,
       iw.Gross_Amount,
       iw.Net_Amount - iw.Discount_Amount AS Net_Amount,
       iw.Net_Order_Amount
FROM EDW_DEV.EDW.Fact_Sales_Item_Week iw
    INNER JOIN EDW_DEV.EDW.Dim_Date dd ON iw.Date_Key = dd.Date_key
     INNER JOIN EDW_DEV.EDW.Dim_Restaurant res ON iw.Restaurant_Key = res.Restaurant_key
     INNER JOIN EDW_DEV.EDW.Dim_MenuItem mi ON iw.Menu_Key = mi.Menu_key
     LEFT JOIN EDW_DEV.EDW.Fact_Promotions_WEEK p
        ON     mi.Item_Product_ID = p.Item_Product_ID
           AND iw.date_key =p.Date_Key
           AND res.Rest_DMA_Code = p.Rest_DMA_Code
	 LEFT JOIN CTE_Order_Amt OA
		ON res.Rest_Number = OA.Rest_Number
		AND iw.Date_Key = OA.Fiscal_Week_Start_Date_Key;


/* <sc-view> VW_COMP_ADJ_1YR_DAILY </sc-view> */

CREATE OR REPLACE VIEW "VW_COMP_ADJ_1YR_DAILY" ("STORENUMBER", "DMA_NAME", "DMA_CODE", "OWNERSHIP_TYPE", "Day", "BUSINESSDATE", "WEEK_NUMBER", "WEEK_START_DATE", "WEEK_END_DATE", "PERIOD_NUMBER", "PERIOD_START_DATE", "PERIOD_END_DATE", "QUARTER_NUMBER", "QUARTER_START_DATE", "QUARTER_END_DATE", "YEAR_NUMBER", "YEAR_START_DATE", "YEAR_END_DATE", "COMP_SALES_TY", "COMP_TRANS_TY", "COMP_SALES_PY", "COMP_TRANS_PY") AS WITH 
Min_Start_Date As 
(
--Select 
--  CASE 
--    WHEN Min_Date <= CAST(Getdate() - 10 AS Date) THEN Min_Date
--    ELSE Cast(Getdate() - 10 AS Date)
--  END As Days_Date 
--From (Select Min(Days_Date) As Min_Date From Dim_Date where Fiscal_Year = (Select Fiscal_Year From Dim_Date Where Days_Date = Cast(GetDate() - 1 AS Date))) A
Select Min(Days_Date) As Days_Date From EDW_DEV.EDW.Dim_Date where Fiscal_Year = (Select Fiscal_Year -1 From EDW_DEV.EDW.Dim_Date Where Days_Date = Cast(Current_Date - 1 AS Date))
),

Min_Store_Open_Date As 
(
Select Cast(Concat(Cast(Fiscal_Year - 2 As Char(4)) , '-10-01') As date) AS Min_Open_Date FROM EDW_DEV.EDW.Dim_Date WHERE Days_Date = Current_date
  
  ),

COMP_TY AS 
(SELECT A.Rest_Number AS storenumber,
       B.Rest_DMA_Desc as DMA_Name,
       B.Rest_DMA_Code as DMA_Code,
       CASE WHEN B.Rest_Type = 'Arg' THEN 'Corporate'
			ELSE 'Franchise'
	   END AS  Ownership_Type,
       Upper(Calendar_Day_Name) As "Day",
       C.Days_Date as businessdate,
       C.Fiscal_Week_Number AS Week_Number,
       C.Fiscal_Week_Start_Date AS Week_Start_Date,
       C.Fiscal_Week_End_Date AS Week_End_Date,
       C.Fiscal_Period_Number AS Period_Number,
       C.Fiscal_Period_Start_Date AS Period_Start_Date,
       C.Fiscal_Period_End_Date AS Period_End_Date,
       C.Fiscal_Quarter_Number AS Quarter_Number,
       C.Fiscal_Quarter_Start_Date AS Quarter_Start_Date,
       C.Fiscal_Quarter_End_Date AS Quarter_End_Date,
       C.Fiscal_Year AS Year_Number,
       C.Fiscal_Year_Start_Date AS Year_Start_Date,
       C.Fiscal_Year_End_Date AS Year_End_Date,
       SalesAmt_TY As Comp_Sales_TY,
       TransCnt_TY As Comp_Trans_TY
FROM EDW_DEV.EDW.Fact_Sales_Day_YOY A 
JOIN EDW_DEV.EDW.Dim_Restaurant B ON A.Restaurant_Key = B.Restaurant_key
JOIN EDW_DEV.EDW.Dim_Date C ON A.Date_Key = C.Date_Key
Where Days_Date >= (Select Days_Date From Min_Start_Date) And B.Rest_OpenDate <= (Select Min_Open_Date FROM Min_Store_Open_Date)
)
,

COMP_PY AS 
(SELECT A.Rest_Number AS storenumber,
        C.Days_Date as businessdate,
       TransCnt_TY As Comp_Trans_PY,
       SalesAmt_TY As Comp_Sales_PY
FROM EDW_DEV.EDW.Fact_Sales_Day_YOY A 
JOIN EDW_DEV.EDW.Dim_Restaurant B ON A.Restaurant_Key = B.Restaurant_key
JOIN EDW_DEV.EDW.Dim_Date C ON A.Date_Key = Cast(Replace(C.Adjusted_Comparison_Date,'-','') AS INT)
Where C.Days_Date >= (Select Days_Date From Min_Start_Date) And B.Rest_OpenDate <= (Select Min_Open_Date FROM Min_Store_Open_Date)
)
--SELECT A.* FROM COMP_TY A JOIN COMP_PY B ON 1=1
Select A.*,B.Comp_Sales_PY,B.Comp_Trans_PY from COMP_TY A JOIN COMP_PY B ON A.storenumber = B.storenumber And A.businessdate = B.businessdate
Where Comp_Sales_TY > 0 AND Comp_Sales_PY > 0; --And A.businessdate > GetDate() - 4;


/* <sc-view> VW_DT_SPEED_OF_SERVICE </sc-view> */

CREATE OR REPLACE VIEW "VW_DT_SPEED_OF_SERVICE" ("Rest Number", "Hour of Day (24)", "Day Part", "Time of Day", "FISCAL_YEAR", "FISCAL_PERIOD_NUMBER", "FISCAL_WEEK_IN_PERIOD_NUMBER", "FISCAL_WEEK_NUMBER", "Date", "DATE_KEY", "Total Order Time (s)", "ORDER_COUNT", "Daily_Met_Goal", "Hourly_Met_Goal", "Total Window Time (s)", "Window_Time_Goal_Met", "Total Greet Time (s)", "Greet_Time_Goal_Met", "Order_Pad_Time_Met_Goal", "Total Order Pad Time (s)", "REST_DMA_CODE", "REST_DMA_DESC", "REST_LATITUDE", "REST_LONGITUDE", "REST_ADDRSTATE", "ARG_REGION_NAME", "ARG_SVP_NAME", "ARG_SUBREGION_NAME", "ARG_VPO_NAME", "ARG_DISTRICT_NAME", "ARG_DO_NAME", "ARG_AREA_NAME", "ARG_AREA_SUPERVISOR") AS WITH PreviousFiscalMonth    
  AS      
  (      
  SELECT  Case when Days_Date<Fiscal_Period_End_Date THEN Fiscal_Period_Id-1      
 ELSE Fiscal_Period_Id End as PeriodID FROM EDW_DEV.EDW.DIM_DATE WHERE Days_Date=Current_Date
  )      
         
  ,FiscalPeriods      
  AS      
  (      
  SELECT   Distinct Fiscal_Period_Id FROM EDW_DEV.EDW.Dim_Date WHERE Fiscal_Period_Id <= (SELECT PeriodID FROM PreviousFiscalMonth)      
  ORDER BY Fiscal_Period_Id Desc  
  Limit 13    
  )  
  ,FiscalDates  
  AS  
  (  
 SELECT Date_key  FROM EDW_DEV.EDW.Dim_Date WHERE Fiscal_Period_Id IN (SELECT Fiscal_Period_Id from FiscalPeriods)  
  )
     
  SELECT        
    dr.Rest_Number AS  "Rest Number"         
   ,dt.HH24Hrs as "Hour of Day (24)"         
   ,dt.day_part_text  as "Day Part"        
   ,dt.US_Time_Text as "Time of Day"         
   ,dd.fiscal_year        
   ,dd.fiscal_period_number        
   ,dd.fiscal_week_in_period_Number        
   ,dd.fiscal_week_number        
   ,dd.days_date AS "Date"       
   ,sos.Date_key         
   ,sos.Time_Accumulated  AS  "Total Order Time (s)"         
   ,sos.Order_Count         
   ,null  "Daily_Met_Goal"         
   ,null  "Hourly_Met_Goal"         
   ,sos.Window_Time  AS  "Total Window Time (s)"         
   ,null  "Window_Time_Goal_Met"         
   ,null AS  "Total Greet Time (s)"        
   ,null  "Greet_Time_Goal_Met"        
   ,null  "Order_Pad_Time_Met_Goal"         
   ,sos.Order_Pad_Time  AS  "Total Order Pad Time (s)"         
   ,dr.Rest_DMA_Code        
   ,dr.Rest_DMA_Desc        
   ,dr.rest_Latitude        
   ,dr.rest_longitude        
   ,dr.rest_addrState        
   ,dr.Arg_Region_Name        
   ,dr.Arg_SVP_Name        
   ,dr.Arg_SubRegion_Name        
   ,dr.Arg_VPO_Name        
   ,dr.Arg_district_name        
   ,dr.ARG_DO_Name        
   ,dr.Arg_Area_Name        
   ,dr.Arg_Area_Supervisor        
  FROM        
    EDW_DEV.EDW.FACT_SPEEDOFSERVICE sos        
    INNER JOIN EDW_DEV.EDW.dim_date dd ON sos.date_key = dd.date_key        
    INNER JOIN EDW_DEV.EDW.dim_time dt ON sos.time_key = dt.time_key        
    INNER JOIN EDW_DEV.EDW.DIM_RESTAURANT dr        
      ON sos.Restaurant_key = dr.Restaurant_key        
   WHERE sos.date_key IN  (SELECT Date_key  from FiscalDates);



/* <sc-view> VW_DIM_DATE </sc-view> */

CREATE OR REPLACE VIEW "VW_DIM_DATE" ("DATE_KEY", "DATE_ID", "DAYS_COUNT", "DAYS_DATE", "DAYS_TEXT", "DAYS_NUMBER", "DATE_RTI", "FISCAL_YEAR_START_DATE", "FISCAL_YEAR_END_DATE", "FISCAL_YEAR", "FISCAL_PERIOD_NUMBER", "FISCAL_YEAR_NAME", "FISCAL_YEAR_SHORT_NAME", "FISCAL_YY_SHORT_NAME", "FISCAL_QUARTER_NUMBER", "FISCAL_QUARTER_START_DATE", "FISCAL_QUARTER_END_DATE", "FISCAL_QUARTER_NAME", "FISCAL_QUARTER_SHORT_NAME", "FISCAL_PERIOD_START_DATE", "FISCAL_PERIOD_START_DATE_INT", "FISCAL_PERIOD_END_DATE", "FISCAL_PERIOD_NAME", "FISCAL_PERIOD_SHORT_NAME", "FISCAL_WEEK_IN_PERIOD_NUMBER", "FISCAL_WEEK_NUMBER", "FISCAL_WEEK_END_DATE", "FISCAL_WEEK_START_DATE", "FISCAL_WEEK_START_DATE_INT", "FISCAL_COMPARISON_DATE", "CALENDAR_YEAR", "CALENDAR_MONTH_NUMBER", "CALENDAR_MONTH_NAME", "CALENDAR_SHORT_MONTH_NAME", "CALENDAR_YEAR_START_DATE", "CALENDAR_YEAR_END_DATE", "CALENDAR_QUARTER_NUMBER", "CALENDAR_QUARTER_NAME", "CALENDAR_ABBREVIATED_QUARTER_NAME", "CALENDAR_QUARTER_START_DATE", "CALENDAR_QUARTER_END_DATE", "CALENDAR_MONTH_START_DATE", "CALENDAR_MONTH_END_DATE", "WEEKDAY_FLAG", "WEEKEND_FLAG", "CALENDAR_DAY_NAME", "CALENDAR_SHORT_DAY_NAME", "CALENDAR_WEEK_START_DATE", "CALENDAR_WEEKEND_DATE", "CALENDAR_WEEK_NAME", "CALENDAR_SHORT_WEEK_NAME", "ISO_WEEK_NAME", "ISO_ORD_DATE_NAME", "FISCAL_QUARTER_ID", "FISCAL_PERIOD_ID", "CALENDAR_QUARTER_ID", "CALENDAR_MONTH_ID", "FISCAL_WEEKS_IN_PERIOD_QUANTITY", "Last Year", "2 Years Ago", "3 Years Ago", "4 Years Ago", "YTD", "Last YTD", "2 Years Ago YTD", "3 Years Ago YTD", "4 Years Ago YTD", "YESTERDAY", "FWTD", "FWTD-1", "PTD", "PTD-1", "1stDayOfPeriod", "Last 13 Periods", "Last 13 Weeks", "Last 13 Days", "ADJUSTED_COMPARISON_DATE", "Fiscal QTD", "Fiscal YTD", "ADJUSTED_WEEK_START_DATE", "ADJUSTED_WEEK_END_DATE", "ADJUSTED_PERIOD_START_DATE", "ADJUSTED_PERIOD_END_DATE", "ADJUSTED_QUARTER_START_DATE", "ADJUSTED_QUARTER_END_DATE", "ADJUSTED_YEAR_START_DATE", "ADJUSTED_YEAR_END_DATE", "IS_CHRISTMAS_DAY", "IS_FISCAL_COMP_CHRISTMAS_DAY", "IS_CALENDAR_COMP_CHRISTMAS_DAY", "THANKSGIVING_DATE", "IS_THANKSGIVING_DAY", "IS_FISCAL_COMP_THANKSGIVING_DAY", "IS_CALENDAR_COMP_THANKSGIVING_DAY") AS WITH CTE_Fiscal_Period
AS
(
SELECT DISTINCT Fiscal_Period_Number Fiscal_Period_Nbr, Fiscal_Period_Start_Date Fiscal_Period_Start_Dt, Fiscal_Period_End_Date Fiscal_Period_End_Dt, Fiscal_Year Fiscal_year_Int
FROM EDW_DEV.EDW.dim_Date
WHERE Days_Date <=  (DATEADD(DAY, -1, CURRENT_TIMESTAMP()))

)
,CTE_Fiscal_Period_Rank
AS
(
SELECT Fiscal_Period_Nbr,  Fiscal_Period_Start_Dt,  Fiscal_Period_End_Dt,  Fiscal_year_Int, ROW_NUMBER() OVER (ORDER BY Fiscal_Period_Start_Dt DESC) r  FROM CTE_Fiscal_Period
)
,CTE_Fiscal_Period_Last2
AS
(
SELECT Fiscal_Period_Nbr,  Fiscal_Period_Start_Dt,  Fiscal_Period_End_Dt,  Fiscal_year_Int,r FROM CTE_Fiscal_period_Rank
WHERE r =2
)
  SELECT

    Date_key
   ,Date_ID
   ,Days_Count
   ,Days_Date
   ,Days_Text
   ,Days_Number
   ,Date_RTI
   ,Fiscal_Year_Start_Date
   ,Fiscal_Year_End_Date
   ,Fiscal_Year
   ,Fiscal_Period_Number
   ,Fiscal_Year_Name
   ,Fiscal_Year_Short_Name
   ,Fiscal_YY_Short_Name
   ,Fiscal_Quarter_Number
   ,Fiscal_Quarter_Start_Date
   ,Fiscal_Quarter_End_Date
   ,Fiscal_Quarter_Name
   ,Fiscal_Quarter_Short_Name
   ,Fiscal_Period_Start_Date
   ,CAST(to_char(Fiscal_Period_Start_Date,'YYYYMMDD') AS INT) AS Fiscal_Period_Start_Date_Int
   ,Fiscal_Period_End_Date
   ,Fiscal_Period_Name
   ,Fiscal_Period_Short_Name
   ,Fiscal_Week_In_Period_Number
   ,Fiscal_Week_Number
   ,Fiscal_Week_End_Date
   ,Fiscal_Week_Start_Date
   ,CAST(to_char(Fiscal_Week_Start_Date,'YYYYMMDD') AS INT) AS Fiscal_Week_Start_Date_Int
   ,Fiscal_Comparison_Date
   ,Calendar_Year
   ,Calendar_Month_Number
   ,Calendar_Month_Name
   ,Calendar_Short_Month_Name
   ,Calendar_Year_Start_Date
   ,Calendar_Year_End_Date
   ,Calendar_Quarter_Number
   ,Calendar_Quarter_Name
   ,Calendar_Abbreviated_Quarter_Name
   ,Calendar_Quarter_Start_Date
   ,Calendar_Quarter_End_Date
   ,Calendar_Month_Start_Date
   ,Calendar_Month_End_Date
   ,Weekday_Flag
   ,Weekend_Flag
   ,Calendar_Day_Name
   ,Calendar_Short_Day_Name
   ,Calendar_Week_Start_Date
   ,Calendar_WeekEnd_Date
   ,Calendar_Week_Name
   ,Calendar_Short_Week_Name
   ,ISO_Week_Name
   ,ISO_Ord_Date_Name
   ,Fiscal_Quarter_Id
   ,Fiscal_Period_Id
   ,Calendar_Quarter_Id
   ,Calendar_Month_Id
   ,Fiscal_weeks_in_Period_quantity
   ,CASE  
      WHEN Days_Date 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-1)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-1)||'-12-31','YYYY-MM-DD')
      THEN 'Last Year'
      ELSE 'N'  
    END AS "Last Year"  
   ,CASE  
      WHEN Days_Date --BETWEEN concat(year(now())-2,'-','01','-','01') AND concat(year(now())-2,'-','12','-','31') 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-2)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-2)||'-12-31','YYYY-MM-DD')
      THEN '2 Years Ago'        
      ELSE 'N'  
    END AS "2 Years Ago"  
   ,CASE  
      WHEN Days_Date --BETWEEN concat(year(now())-3,'-','01','-','01') AND concat(year(now())-3,'-','12','-','31') 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-3)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-3)||'-12-31','YYYY-MM-DD')
      THEN '3 Years Ago'  
      ELSE 'N'  
    END AS "3 Years Ago"  
   ,CASE  
      WHEN Days_Date --BETWEEN concat(year(now())-4,'-','01','-','01') AND concat(year(now())-4,'-','12','-','31') 
      BETWEEN TO_DATE((YEAR(CURRENT_DATE())-4)||'-01-01','YYYY-MM-DD') AND TO_DATE((YEAR(CURRENT_DATE())-4)||'-12-31','YYYY-MM-DD')
      THEN '4 Years Ago'  
      ELSE 'N'  
    END AS "4 Years Ago"  
   ,CASE  
      --WHEN Days_Date BETWEEN concat(year(now()),'-','01','-','01') and add_days(now(),-1) 
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE()))||'-01-01','YYYY-MM-DD') and (DATEADD(DAY, -1, CURRENT_DATE()))
      THEN 'YTD'  
      ELSE 'N'  
    END AS YTD  
   ,CASE  
      --WHEN Days_Date BETWEEN concat(year(now())-1,'-','01','-','01') and add_days(add_years(now(),-1),-1)
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-1)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-1,(DATEADD(DAY, -1, CURRENT_DATE())))
      THEN 'Last YTD'  
      ELSE 'N'  
    END AS "Last YTD"  
   ,CASE  
      --WHEN Days_Date BETWEEN concat(year(now())-2,'-','01','-','01') and add_days(add_years(now(),-2),-1)
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-2)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-2,(DATEADD(DAY, -1, CURRENT_DATE())))
      THEN '2 Years Ago YTD'  
      ELSE 'N'  
    END AS "2 Years Ago YTD"  
  ,CASE  
      --WHEN Days_Date BETWEEN concat(year(now())-3,'-','01','-','01') and add_days(add_years(now(),-3),-1)
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-3)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-3,(DATEADD(DAY, -1, CURRENT_DATE())))
      THEN '3 Years Ago YTD'  
      ELSE 'N'  
    END AS "3 Years Ago YTD"  
  ,CASE  
      --WHEN Days_Date BETWEEN concat(year(now())-4,'-','01','-','01') and add_days(add_years(now(),-4),-1)
      WHEN Days_Date BETWEEN TO_DATE((YEAR(CURRENT_DATE())-4)||'-01-01','YYYY-MM-DD') and DATEADD(YEAR,-4,(DATEADD(DAY, -1, CURRENT_DATE())))
      THEN '4 Years Ago YTD'  
      ELSE 'N'  
    END AS "4 Years Ago YTD"  
  ,CASE  
      --WHEN to_date(add_days(now(),-1),'YYYY-MM-DD') = Days_Date  
      WHEN TO_DATE(CURRENT_DATE()-1) = Days_Date
      THEN 'Yesterday'  
   ELSE 'N'  
   END AS Yesterday  
  ,CASE  
      --WHEN to_date(add_days(now(),-1),'YYYY-MM-DD') between  Fiscal_Week_Start_Date and Fiscal_Week_End_Date and to_date(now(),'YYYY-MM-DD')>= days_date
      WHEN TO_DATE(CURRENT_DATE()-1) between  Fiscal_Week_Start_Date and Fiscal_Week_End_Date and CURRENT_DATE() >= days_date 
	  then 'FWTD'
	Else 'N'
	END as FWTD
  ,CASE  
      --when to_date(add_days(now(),-1),'YYYY-MM-DD') between  add_weeks(Fiscal_Week_Start_Date,1) and add_weeks(Fiscal_Week_End_Date,1) 
      when TO_DATE(CURRENT_DATE()-1)  between DATEADD(WEEK, 1, Fiscal_Week_Start_Date) and DATEADD(WEEK, 1, Fiscal_Week_End_Date) 
	then 'FWTD'
	Else 'N'
    END AS "FWTD-1"  
  ,CASE  
    --WHEN to_date(add_days(now(),-1),'YYYY-MM-DD') BETWEEN Fiscal_Period_Start_Date AND Fiscal_Period_End_Date AND to_date(now(),'YYYY-MM-DD') >= days_date  
    WHEN TO_DATE(CURRENT_DATE()-1) BETWEEN Fiscal_Period_Start_Date AND Fiscal_Period_End_Date AND CURRENT_DATE() >= days_date  
    THEN 'PTD'  
    ELSE 'N'  
   END AS PTD 
  ,CASE 
	WHEN fpl.fiscal_period_Nbr IS NOT NULL
	THEN 'PTD'
	ELSE 'N'
   END AS "PTD-1"  
 ,CASE  
    WHEN (CURRENT_DATE()) = Fiscal_Period_Start_Date
	Then 'Y'
	Else 'N'  
   END AS "1stDayOfPeriod"  
  ,CASE  
      --WHEN Fiscal_Period_Start_Date >= to_date(concat(year(add_months(now(),-13)),'-',month(add_months(now(),-13)),'-','01'),'yyyy-mm-dd') AND days_date <= to_date(add_days(now(),-1),'YYYY-MM-DD')
      WHEN Fiscal_Period_Start_Date >= DATE_TRUNC('MONTH', DATEADD(MONTH, -13, CURRENT_DATE()))AND days_date <= (DATEADD(DAY,-1,CURRENT_DATE()))
	   THEN 'Last 13 Periods'  
      ELSE 'N' 
   END AS "Last 13 Periods"  
  ,CASE  
     --WHEN Fiscal_Week_Start_Date >= add_weeks(now(),-13) AND  days_date <= to_date(add_days(now(),-1),'YYYY-MM-DD')  
     WHEN Fiscal_Week_Start_Date >= DATEADD(WEEK, -13, Fiscal_Week_Start_Date)  AND  days_date <= (DATEADD(DAY,-1,CURRENT_DATE()))
     THEN 'Last 13 Weeks'  
     ELSE 'N'  
   END AS "Last 13 Weeks"  
 ,CASE  
      --WHEN days_date BETWEEN to_date(add_days(now(),-13),'YYYY-MM-DD') AND to_date(add_days(now()-1,0),'YYYY-MM-DD')  
      WHEN days_date BETWEEN DATEADD(DAY, -13, Fiscal_Week_Start_Date) AND (DATEADD(DAY,-1,CURRENT_DATE()))
      THEN 'Last 13 Days'  
      ELSE 'N'  
  END AS "Last 13 Days" 
  
 ,(DATEADD(DAY,-364,CURRENT_DATE()))  Adjusted_Comparison_Date
 ,CASE  
   WHEN (CURRENT_DATE()) BETWEEN Fiscal_Quarter_Start_Date AND Fiscal_Quarter_End_Date AND days_date <= (DATEADD(DAY,-1,CURRENT_DATE()))
      THEN 'QTD'  
      ELSE 'N'  
   END AS "Fiscal QTD"  
 ,CASE  
    WHEN Days_Date BETWEEN Fiscal_Year_Start_Date AND (DATEADD(DAY,-1,CURRENT_DATE())) AND  Fiscal_Year_Start_Date =  
             (SELECT  
                Fiscal_Year_Start_Date  
              FROM EDW_DEV.EDW.dim_date  
              WHERE  (DATEADD(DAY,-1,CURRENT_DATE())) BETWEEN fiscal_year_start_date AND Fiscal_year_end_Date  
              GROUP BY Fiscal_Year_Start_Date)  
      -- OLD LOGIC YEAR(DATEADD(dd,-1,GETDATE()))  
    THEN 'Fiscal YTD'  
    ELSE 'N'  
    END AS "Fiscal YTD" 
 ,DATEADD(DAY, -364, Fiscal_Week_Start_Date) AS Adjusted_Week_Start_Date
,DATEADD(DAY, -364, Fiscal_Week_End_Date) AS Adjusted_Week_End_Date
,DATEADD(DAY, -364, Fiscal_Period_Start_Date) AS Adjusted_Period_Start_Date
,DATEADD(DAY, -364, Fiscal_Period_End_Date) AS Adjusted_Period_End_Date
,DATEADD(DAY, -364, Fiscal_Quarter_Start_Date) AS Adjusted_Quarter_Start_Date
,DATEADD(DAY, -364, Fiscal_Quarter_End_Date) AS Adjusted_Quarter_End_Date
,DATEADD(DAY, -364, Fiscal_Year_Start_Date) AS Adjusted_Year_Start_Date
,DATEADD(DAY, -364, Fiscal_Year_End_Date) AS Adjusted_Year_End_Date

  ,DD.Is_Christmas_Day
  ,DD.Is_Fiscal_Comp_Christmas_Day
 ,DD.Is_Calendar_Comp_Christmas_Day
 ,DD.Thanksgiving_Date
,DD.Is_Thanksgiving_Day
,DD.Is_Fiscal_Comp_Thanksgiving_Day
,DD.Is_Calendar_Comp_Thanksgiving_Day
  FROM
    EDW_DEV.EDW.Dim_Date dd
  LEFT JOIN Cte_Fiscal_Period_Last2 fpl ON dd.Fiscal_Period_Number = fpl.Fiscal_period_Nbr AND dd.Fiscal_Year = fpl.fiscal_year_Int   
  ORDER BY
    Days_Date;


/* <sc-view> VW_AUDIT_EVENTS_DAY </sc-view> */

CREATE OR REPLACE VIEW "VW_AUDIT_EVENTS_DAY" ("DAYS_DATE", "REST_NUMBER", "REST_NAME", "EMPLOYEE_NAME", "EVENT_ID", "EVENT_NAME", "Value") AS SELECT 
		DD.Days_Date
		,DR.Rest_Number 
		,DR.Rest_Name 
		,CE.Employee_Name AS Employee_Name
		,FA.Event_Key AS Event_ID
		,DAE.Event_Name AS Event_Name
		,"Value"
	FROM EDW_DEV.EDW.Fact_Audit_Day AS FA
	INNER JOIN EDW_DEV.EDW.Dim_Date AS DD
	ON DD.Date_Key = FA.Date_Key
	INNER JOIN EDW_DEV.EDW.Dim_AuditEvent AS DAE
	ON FA.Event_Key = DAE.Event_Key
	INNER JOIN EDW_DEV.EDW.Dim_Restaurant AS DR
	ON FA.Restaurant_Key = DR.Restaurant_Key
	INNER JOIN EDW_DEV.EDW.Dim_Employee AS DE
	ON FA.Employee_Key = DE.Employee_Key
	INNER JOIN EDW_DEV.EDW.Dim_Employee AS CE
	ON DE.Employee_Id = CE.Employee_Id
	AND CE.POS_Name = DE.POS_Name
	AND CE.Is_Current = 1
;


/* <sc-view> VW_TLD_FACT_SALES_ORDER </sc-view> */

CREATE OR REPLACE VIEW "VW_TLD_FACT_SALES_ORDER" ("ORDER_ID", "ORDER_NUMBER", "ORDER_NAME", "ORDER_DATETIME", "ORDER_DATE", "ORDER_TIME", "REST_NUMBER", "REST_NAME", "REST_DMA_CODE", "REST_DMA_DESC", "REST_TYPE", "POS_NAME", "EMPLOYEE_ID", "EMPLOYEE_NAME", "REVENUE_CENTER", "TAXEXEMPT_ID", "GUEST_COUNT", "GROSS_QUANTITY", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "SURCHARGE_AMOUNT", "TAX_AMOUNT", "PAYMENT_AMOUNT", "IS_TAXEXEMPT", "IS_VOID", "IS_REFUND") AS SELECT 
		so.Order_ID,
		so.Order_Number,
		so.Order_Name,
		TIMESTAMPADD(
			SECOND,
			SECOND(DT.Time_Of_Day_Time),
			TIMESTAMPADD(
				MINUTE,
				MINUTE(DT.Time_Of_Day_Time),
				TIMESTAMPADD(
					HOUR,
					HOUR(DT.Time_Of_Day_Time),
					TO_TIMESTAMP(DD.Days_Date)
				)
			)
		) AS ORDER_DATETIME,
		DD.Days_Date Order_Date,
		DT.Time_Of_Day_Time Order_Time,
		IFNULL(DR.Rest_Number,'NA') AS Rest_Number,
		IFNULL(DR.Rest_Name,'NA') AS Rest_Name,
		DR.Rest_DMA_Code,
		DR.Rest_DMA_Desc,
		DR.Rest_Type,
		pos.POS_Name,
		IFNULL(DE.Employee_Id,0) AS Employee_ID,
		IFNULL(DE.Employee_Name,'NA') AS Employee_Name,	
        IFNULL(POP.POP_Desc,'NA') AS Revenue_Center,
		so.TaxExempt_ID,
        so.Guest_Count,
        so.Gross_Quantity,
        so.Gross_Amount,
        so.Discount_Amount,
        so.Net_Amount,
        so.Surcharge_Amount,
        so.Tax_Amount,
        so.Payment_Amount,
		so.IS_TAXEXEMPT,
        so.Is_Void,
        so.Is_Refund
FROM EDW_DEV.EDW.Fact_Sales_Order so
INNER JOIN EDW_DEV.EDW.Dim_Date DD
	ON DD.Date_key = so.Date_Key
INNER JOIN EDW_DEV.EDW.Dim_Time DT
	ON DT.Time_key = so.Time_Key
INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
	ON DR.Restaurant_key = so.Restaurant_Key
LEFT JOIN EDW_DEV.EDW.Dim_PointOfPurchase POP
	ON POP.POP_key = so.POP_Key
INNER JOIN EDW_DEV.EDW.Dim_PointOfSale pos ON so.POS_Key = pos.POS_Key
LEFT JOIN EDW_DEV.EDW.Dim_Employee DE ON so.Employee_Key = DE.Employee_Key
;



/* <sc-view> VW_FACT_SOS_CHECK </sc-view> */
CREATE OR REPLACE VIEW "VW_FACT_SOS_CHECK"
("DATE_KEY", "VOL")
AS
SELECT
 DATE_KEY,
 COUNT(*) Vol
 from
 EDW_DEV.EDW.FACT_SPEEDOFSERVICE
 Group by
 Date_KEY
 Order by
 DATE_Key DESC NULLS LAST

 ;



/* <sc-view> VW_TLD_FACT_SALES_ORDER_LINE </sc-view> */

CREATE OR REPLACE VIEW "VW_TLD_FACT_SALES_ORDER_LINE" ("ORDER_ID", "ORDER_NUMBER", "ORDER_DATETIME", "ORDER_DATE", "ORDER_TIME", "REST_NUMBER", "REST_NAME", "REST_DMA_CODE", "REST_DMA_DESC", "REST_TYPE", "POS_NAME", "REVENUE_CENTER", "ITEM_NUMBER", "PLU_NUMBER", "PLU_DESC", "PROMO_CODE", "PROMO_DESC", "ITEM_PRODUCT_ID", "ITEM_PRODUCT_DESC", "DISCOUNT_NUMBER", "DISCOUNT_DESC", "DISCOUNT_TYPE_DESC", "REGISTER_ID", "LINE_ITEM_ID", "PARENT_LINE_ITEM_ID", "PRICE", "GROSS_QUANTITY", "NET_QUANTITY", "GROSS_AMOUNT", "NET_AMOUNT", "DISCOUNT_AMOUNT", "IS_MODIFIER", "IS_COMBO", "IS_COUPON", "IS_CLEAR", "IS_VOID", "IS_REFUND") AS SELECT 
    SOL.Order_ID,
		SOL.Order_Number,
		TIMESTAMPADD(
			SECOND,
			SECOND(DT.Time_Of_Day_Time),
			TIMESTAMPADD(
				MINUTE,
				MINUTE(DT.Time_Of_Day_Time),
				TIMESTAMPADD(
					HOUR,
					HOUR(DT.Time_Of_Day_Time),
					TO_TIMESTAMP(DD.Days_Date)
				)
			)
		) AS ORDER_DATETIME,
		DD.Days_Date Order_Date,
		DT.Time_Of_Day_Time Order_Time,
		IFNULL(DR.Rest_Number,'NA') AS Rest_Number,
		IFNULL(DR.Rest_Name,'NA') AS Rest_Name,
		DR.Rest_DMA_Code,
		DR.Rest_DMA_Desc,
		DR.Rest_Type,
		pos.POS_Name,
		IFNULL(POP.POP_Desc,'NA') AS Revenue_Center,
		IFNULL(
		CASE WHEN DMI.POS_Name ='Xpient' THEN TO_Char(DMI.Item_ID)
			 WHEN DMI.Promo_Code = '0' AND DMI.POS_Name <> 'Xpient'  THEN CONCAT('-' , DMI.PLU_Number) 
			 WHEN DMI.Promo_Code <> '0' AND DMI.POS_Name <> 'Xpient' THEN CONCAT('-',dmi.Promo_Code,DMI.PLU_Number)
		END
		,'NA') AS Item_Number,  
		IFNULL(DMI.PLU_Number,'NA') AS PLU_Number,
		IFNULL(DMI.PLU_Desc,'NA') AS PLU_Desc,
		IFNULL(DMI.Promo_Code,'NA') AS Promo_Code,
		IFNULL(DMI.Promo_Desc,'NA') AS Promo_Desc,
		IFNULL(DMI.Item_Product_ID,0) AS Item_Product_ID,
		IFNULL(DMI.Item_Product_Desc,'NA') AS Item_Product_Desc,
		IFNULL(Disc.Discount_Number,0) AS Discount_Number,
		IFNULL(Disc.Discount_Desc,'NA') AS Discount_Desc,
		IFNULL(Disc.Discount_Type_Desc,'NA') AS Discount_Type_Desc,
		SOL.Register_ID,
		SOL.Line_Item_ID,
		SOL.Parent_Line_Item_ID,
		SOL.Price,
		SOL.Gross_Quantity,
		SOL.Net_Quantity,
		SOL.Gross_Amount,
		SOL.Net_Amount,
		SOL.Discount_Amount,
		SOL.Is_Modifier,
		SOL.Is_Combo,
		SOL.Is_Coupon,
		SOL.Is_Clear,
		SOL.Is_Void,
		SOL.Is_Refund
FROM EDW_DEV.EDW.Fact_Sales_Order_Line SOL
INNER JOIN EDW_DEV.EDW.Dim_Date DD
	ON DD.Date_key = SOL.Date_Key
INNER JOIN EDW_DEV.EDW.Dim_Time DT
	ON DT.Time_key = SOL.Time_Key
INNER JOIN EDW_DEV.EDW.Dim_PointOfSale pos ON sol.POS_Key = pos.POS_Key
INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
	ON DR.Restaurant_key = SOL.Restaurant_Key
INNER JOIN EDW_DEV.EDW.Dim_MenuItem DMI
	ON DMI."MENU_KEY" = SOL."MENU_KEY"
LEFT JOIN EDW_DEV.EDW.Dim_PointOfPurchase POP
	ON POP.POP_key = SOL.POP_Key

LEFT JOIN EDW_DEV.EDW.Dim_Discount Disc
	ON Disc.Discount_key = SOL.Discount_Key

;



/* <sc-view> VW_AUDIT_EVENTS </sc-view> */

CREATE OR REPLACE VIEW "VW_AUDIT_EVENTS" ("DAYS_DATE", "REST_NUMBER", "REST_NAME", "EMPLOYEE_NAME", "EVENT_ID", "EVENT_NAME", "ORDER_ID", "CREDITCARD_HOLDER_NAME", "CREDITCARD_NUMBER", "Value", "IS_MULTIPLECARDS") AS SELECT 
		 DD.Days_Date
		,DR.Rest_Number 
		,DR.Rest_Name 
		,CE.Employee_Name
		,FA.Event_Key AS Event_ID
		,DAE.Event_Name AS Event_Name
		,FA.Order_ID
		, MAX(FSO.Payment_Name) AS Creditcard_Holder_Name 
		, MAX(FSO.Payment_Last4 )AS Creditcard_Number
		,SUM("Value") AS Value
		, CASE WHEN COUNT(1) > 1 THEN 1 ELSE 0 END AS IS_Multiplecards
	FROM EDW_DEV.EDW.Fact_Audit AS FA
	INNER JOIN EDW_DEV.EDW.Dim_Date AS DD
		ON DD.Date_Key = FA.Date_Key
	INNER JOIN EDW_DEV.EDW.Dim_AuditEvent AS DAE
	ON FA.Event_Key = DAE.Event_Key
	INNER JOIN EDW_DEV.EDW.Dim_Restaurant AS DR
	ON FA.Restaurant_Key = DR.Restaurant_Key
	INNER JOIN EDW_DEV.EDW.Dim_Employee AS DE
	ON FA.Employee_Key = DE.Employee_Key
	INNER JOIN EDW_DEV.EDW.Dim_Employee AS CE
	ON DE.Employee_Id = CE.Employee_Id
	AND CE.POS_Name = DE.POS_Name
	AND CE.Is_Current = 1
	LEFT JOIN (SELECT   Order_ID, Payment_Name, Payment_Last4 , Restaurant_Key  , date_key
	                     FROM   EDW_DEV.EDW.Fact_Sales_Order_Payment 
						 WHERE LTRIM(RTRIM(payment_desc)) IN ( 'CreditCard' , 'AMEX' , 'Visa' , 'Mastercard', 'Discover')
						 AND is_void = 0
						 ) AS FSO
	ON FSO.order_id = FA.order_id 
	AND FSO.Restaurant_Key = FA.Restaurant_Key
	AND FSO.date_key = FA.date_key
	WHERE FA.Order_ID IS NOT NULL 
	GROUP BY FA.Event_Key , DR.Rest_Number , DR.Rest_Name , DAE.Event_Name,FA.Order_ID,DD.Days_Date, CE.Employee_Name,
	                    CASE WHEN FSO.Payment_Name IS NOT NULL THEN '' ELSE FSO.Payment_Name END 
	UNION ALL
	SELECT 
		DD.Days_Date
		,DR.Rest_Number 
		,DR.Rest_Name 
		,CE.Employee_Name AS Employee_Name
		,FA.Event_Key AS Event_ID
		,DAE.Event_Name AS Event_Name
		,FA.Order_ID
		,'' AS Creditcard_Holder_Name 
		,'' AS Creditcard_Number 
		, "Value" AS Value
		, 1 AS No_of_Transactions
	FROM EDW_DEV.EDW.Fact_Audit AS FA
	INNER JOIN EDW_DEV.EDW.Dim_Date AS DD
	ON FA.Date_Key = DD.Date_Key
	INNER JOIN EDW_DEV.EDW.Dim_AuditEvent AS DAE
	ON FA.Event_Key = DAE.Event_Key
	INNER JOIN EDW_DEV.EDW.Dim_Restaurant AS DR
	ON FA.Restaurant_Key = DR.Restaurant_Key
	INNER JOIN EDW_DEV.EDW.Dim_Employee AS DE
	ON FA.Employee_Key = DE.Employee_Key
	INNER JOIN EDW_DEV.EDW.Dim_Employee AS CE
	ON DE.Employee_Id = CE.Employee_Id
	AND CE.POS_Name = DE.POS_Name
	AND CE.Is_Current = 1
    AND FA.Order_ID IS  NULL
;



/* <sc-view> VW_REPORT_TLD_CHECK_VIEWER </sc-view> */
CREATE OR REPLACE VIEW "VW_REPORT_TLD_CHECK_VIEWER"
("ORDER_ID", "ORDER_NUMBER", "ORDER_NAME", "DATE_KEY", "REST_NUMBER", "REST_ADDRCITY", "REST_ADDRSTATE", "REST_ADDRLINE1", "REST_ADDRPHONE", "GUEST_COUNT", "POS_ITEM_DESC", "PLU_DESC", "DISCOUNT_DESC", "POP_DESC", "ITEM_NET_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "TAX_AMOUNT", "PAYMENT_AMOUNT", "EMPLOYEE_ID", "EMPLOYEE_NAME", "CLOSETIME", "IS_VOID", "IS_CLEAR", "IS_REFUND")
AS
SELECT
	FSO.Order_ID,
	FSO.Order_Number,
	FSO.Order_Name,
	FSO.Date_Key,
	DR.Rest_Number,
	DR.Rest_AddrCity,
	DR.Rest_AddrState,
	DR.Rest_AddrLine1,
	DR.Rest_AddrPhone,
	FSo.Guest_Count,
	DMI.POS_Item_Desc,
	DMI.PLU_Desc,
		IFNULL(DDIS.Discount_Desc,'NA') AS Discount_Desc,
		IFNULL(DPOP.POP_Desc,'NA') AS POP_Desc,
	FSOL.Net_Amount AS Item_Net_Amount,
	FSOL.Discount_Amount,
	FSO.Net_Amount,
	FSO.Tax_Amount,
	FSO.Payment_Amount,
		IFNULL(DE.Employee_ID,0) AS Employee_ID,
		IFNULL(DE.Employee_Name,'NA') AS Employee_Name,
	LEFT(RIGHT(DT.TIME_OF_DAY_TIME,15),8) 	AS CloseTime,
	FSOL.IS_VOID,
	FSOL.IS_CLEAR,
	FSOL.IS_REFUND

FROM
	EDW_DEV.EDW.Fact_Sales_Order FSO
INNER JOIN
		EDW_DEV.EDW.Fact_Sales_Order_Line FSOL ON FSO.Order_ID=FSOL.Order_ID AND FSO.Date_Key =FSOL.Date_Key
INNER JOIN
		EDW_DEV.EDW.Dim_Restaurant DR ON DR.Restaurant_key=FSO.Restaurant_Key
INNER JOIN
		EDW_DEV.EDW.Dim_MenuItem DMI ON DMI.Menu_key=FSOL.Menu_Key
INNER JOIN
		EDW_DEV.EDW.Dim_PointOfPurchase DPOP ON DPOP.POP_key=FSO.POP_Key
LEFT JOIN
		EDW_DEV.EDW.Dim_Employee DE ON DE.Employee_Key=FSO.Employee_Key
INNER JOIN
		EDW_DEV.EDW.Dim_Time DT ON DT.TIME_KEY=FSO.TIME_KEY
LEFT JOIN
		EDW_DEV.EDW.Dim_Discount DDIS ON DDIS.Discount_Key = FSOL.Discount_Key;



/* <sc-view> VW_TLD_ITEM_PROMOTED_WEEK_BY_DMA_WITH_COMPONENT </sc-view> */

CREATE OR REPLACE VIEW "VW_TLD_ITEM_PROMOTED_WEEK_BY_DMA_WITH_COMPONENT" ("DAYS_DATE", "FISCAL_YEAR", "FISCAL_WEEK_NUMBER", "REST_NUMBER", "REST_DMA_CODE", "REST_DMA_DESC", "REST_TYPE", "PLU_NUMBER", "PLU_DESC", "ITEM_ID", "ITEM_DESC", "ITEM_PRODUCT_ID", "ITEM_PRODUCT_DESC", "COMPONENT_ID", "COMPONENT_NAME", "ITEM_TYPE_CODE", "POS_NAME", "POS_ITEM_DESC", "MENU_GROUP", "MENU_TYPE", "MAIN_GROUP", "PROMO_PRIMARY_TV", "PROMO_SECONDARY_TV", "PROMO_INSTORE", "DAYS_COUNT", "GROSS_QUANTITY", "GROSS_ORDER_COUNT", "GROSS_AMOUNT", "NET_AMOUNT", "NET_ORDER_AMOUNT", "U/S/D", "Item Mix %") AS WITH CTE_Order_Amt
AS
(
		SELECT 
			CAST(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') AS INT) AS Fiscal_Week_Start_Date_Key,
			DR.Rest_Number,
			DR.Rest_DMA_Code,
			COUNT(DISTINCT dd.Date_key) AS Days_Count 
		FROM EDW_DEV.EDW.Fact_Sales_Order FSO
		INNER JOIN EDW_DEV.EDW.Dim_Date DD
			ON FSO.Date_Key = DD.Date_key
		INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
			ON DR.Restaurant_key = FSO.Restaurant_Key
		WHERE FSO.Payment_Amount > 0
		GROUP BY CAST(TO_CHAR(dd.Fiscal_Week_Start_Date,'yyyyMMdd') AS INT),
				 DR.Rest_Number,
				DR.Rest_DMA_Code
)

,CTE_Total_Amount
AS
( 
		SELECT Date_Key,
			   DR.Rest_DMA_Code,
			   SUM(FSI.Net_Amount) - SUM(FSI.Discount_Amount) AS Total_Amount
		FROM EDW_DEV.EDW.Fact_Sales_Item_Week FSI
		INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
			ON DR.Restaurant_key = FSI.Restaurant_Key 

		GROUP BY FSI.Date_Key,
                 DR.Rest_DMA_Code
)

SELECT dd.DAYS_DATE,
       dd.Fiscal_Year,
       dd.Fiscal_Week_Number,
       res.Rest_Number,
       res.Rest_DMA_Code,
       res.Rest_DMA_Desc,
	   res.Rest_Type,
       mi.PLU_Number,
       mi.PLU_Desc,
       mi.Item_ID,
       mi.Item_Desc,
       mi.Item_Product_ID,
       mi.Item_Product_Desc,
	   mi.Component_ID,
	   mi.Component_Name,
       mi.Item_Type_Code,
       mi.POS_Name,
       mi.POS_Item_Desc,
       mi.Menu_Group,
       mi.Menu_Type,
       mi.Main_Group,
       IFNULL(p.Primary_TV,'NA') Promo_Primary_TV,
       IFNULL(p.Secondary_TV,'NA') Promo_Secondary_TV,
       IFNULL(p.InStore,'NA') Promo_InStore,
	   AVG(IFNULL(OA.Days_Count,0)) AS Days_Count,
       SUM(iw.Gross_Quantity) AS Gross_Quantity,
	   SUM(iw.Gross_Order_Count) AS Gross_Order_Count,
       SUM(iw.Gross_Amount) AS Gross_Amount,
       SUM(iw.Net_Amount - iw.Discount_Amount) AS Net_Amount,
	   SUM(iw.Net_Order_Amount) AS Net_Order_Amount,
	   CASE WHEN SUM(IFNULL(OA.Days_Count,0))  = 0
				THEN 0
			ELSE SUM(iw.Gross_Quantity)/AVG(IFNULL(OA.Days_Count,0))/COUNT(DISTINCT res.Rest_Number) 
	   END AS "U/S/D",
	
	   CASE WHEN IFNULL(CTA.Total_Amount,0) = 0
				THEN 0
			ELSE (
				 SUM( 
					CASE WHEN LOWER(mi.Item_Product_Desc) LIKE '%meal%'
							THEN (iw.Net_Amount - iw.Discount_Amount)+ 3.3 * iw.Gross_Quantity
						 ELSE (iw.Net_Amount - iw.Discount_Amount)
					END
				  ) /  IFNULL(CTA.Total_Amount,0)
				  )
	   END AS "Item Mix %"
FROM EDW_DEV.EDW.Fact_Sales_Item_Week iw
    INNER JOIN EDW_DEV.EDW.Dim_Date dd ON iw.Date_Key = dd.Date_key
     INNER JOIN EDW_DEV.EDW.Dim_Restaurant res ON iw.Restaurant_Key = res.Restaurant_key
     INNER JOIN EDW_DEV.EDW.Dim_MenuItem mi ON iw.Menu_Key = mi.Menu_key
     LEFT JOIN EDW_DEV.EDW.Fact_Promotions_Week p
        ON     mi.Item_Product_ID = p.Item_Product_ID
           AND iw.date_key =p.Date_Key
           AND res.Rest_DMA_Code = p.Rest_DMA_Code

	 LEFT JOIN CTE_Total_Amount CTA
		ON CTA.Rest_DMA_Code = res.Rest_DMA_Code
		AND CTA.Date_Key = iw.Date_Key  
	 LEFT JOIN CTE_Order_Amt OA
		ON res.Rest_Number = OA.Rest_Number
		AND iw.Date_Key = OA.Fiscal_Week_Start_Date_Key


GROUP BY IFNULL(p.Primary_TV, 'NA'),
         IFNULL(p.Secondary_TV, 'NA'),
         IFNULL(p.InStore, 'NA'),
         dd.Days_Date,
         dd.Fiscal_Year,
         dd.Fiscal_Week_Number,
         res.Rest_DMA_Code,
         res.Rest_DMA_Desc,
         res.Rest_Type,
         mi.Item_ID,
         mi.Item_Desc,
         mi.Item_Product_ID,
         mi.Item_Product_Desc,
         mi.Component_ID,
         mi.Component_Name,
         mi.Item_Type_Code,
         mi.POS_Name,
         mi.POS_Item_Desc,
         mi.Menu_Group,
         mi.Menu_Type,
         mi.Main_Group,
         CTA.Total_Amount,
       	 res.Rest_Number,
       mi.PLU_Number,
       mi.PLU_Desc


;



/* <sc-view> VW_COMP_ACQUISITIONDATES </sc-view> */
CREATE OR REPLACE VIEW "VW_COMP_ACQUISITIONDATES" ("REST_SNBR", "ACQUISITION_DATE") AS SELECT   Rest_Number Rest_sNbr
         --CAST(CAST(Rest_Number AS INT) AS varchar(10)) Rest_sNbr,  
         ,TO_DATE(TO_VARIANT(Rest_AcquisitionDt)::VARCHAR, 'YYYY-MM-DD') Acquisition_Date 
         
FROM EDW_DEV.EDW.Dim_Restaurant
WHERE  isCurrent = 1
AND Rest_Type = 'Arg'
AND Restaurant_key > 0
AND Rest_AcquisitionDt >= 20150118
;



/* <sc-view> VW_INMOMENT_SURVEY_DATA_WITH_DMA </sc-view> */
CREATE OR REPLACE VIEW "VW_INMOMENT_SURVEY_DATA_WITH_DMA"
("REST_NUMBER", "REST_NAME", "REST_TYPE", "REST_DMA_CODE", "REST_DMA_DESC", "SURVEY_DATE_KEY", "SURVEY_DATE", "SURVEY_TIME", "SURVEY_NUMBER", "SURVEY_TYPE", "SURVEY_NAME", "SURVEY_DESCRIPTION", "QUESTION_NUMBER", "QUESTION_TYPE", "QUESTION_CATEGORY", "QUESTION_DESCRIPTION", "RESPONSE_TEXT", "COMMENT_TEXT", "POINTS_POSSIBLE", "POINTS", "LIKERT_SCORE", "ADJUSTED_SCORE", "EXCLUSION_REASON")
AS
SELECT
r.Rest_Number,
r.Rest_Name,
r.Rest_Type,
r.Rest_DMA_Code,
r.Rest_DMA_Desc,
sr.SurveyDT_Key AS Survey_Date_Key,
d.days_date Survey_Date,
t.Time_Of_Day_Time Survey_Time,
s.Survey_Number,
st.MODE Survey_Type,
st.NAME Survey_Name,
st.DESCRIPTION Survey_Description,
q.Question_Number,
q.Type Question_Type,
q.Category Question_Category,
q.Description Question_Description,
sr.Response_Text Response_Text,
c.Comment_Text,
sr.Points_Possible,
sr.Points,
sr.Likert_Score,
sr.Adjusted_Score,
e.Exclusion_Reason
FROM
EDW_DEV.EDW.FACT_SURVEY_RESULT sr
JOIN
EDW_DEV.EDW.DIM_DATE d ON sr.SurveyDT_Key = d.Date_key
JOIN
EDW_DEV.EDW.DIM_SURVEY s ON s.Survey_Key = sr.Survey_Key
JOIN
EDW_DEV.EDW.DIM_SURVEY_TYPE st ON s.Survey_Type_Key = st.Survey_Type_Key
JOIN
EDW_DEV.EDW.DIM_SURVEY_QUESTION q ON sr.Question_Key = q.Question_Key
LEFT JOIN
EDW_DEV.EDW.DIM_SURVEY_COMMENT c ON sr.Comment_Key = c.Comment_Key
LEFT JOIN
EDW_DEV.EDW.DIM_RESTAURANT r ON s.Restaurant_Key = r.Restaurant_key
JOIN
EDW_DEV.EDW.DIM_TIME t ON t.Time_key = s.SurveyTm_Key
JOIN
EDW_DEV.EDW.DIM_EXCLUSION_REASON e ON sr.Exclusion_Key = e.Exclusion_key
WHERE r.Rest_Number IS NOT NULL
AND (e.Exclusion_Reason = 'NONE'
OR e.Exclusion_Reason = 'Not Excluded');



/* <sc-view> VW_PNL_SUMMARY_ACCOUNTS_OLD </sc-view> */

CREATE OR REPLACE VIEW "VW_PNL_SUMMARY_ACCOUNTS_OLD" ("PARENT_ACCOUNT_NUMBER", "ACCOUNT_DESC", "FISCAL_PERIOD_NUMBER", "FISCAL_YEAR", "REST_NAME", "REST_NUMBER", "PD ACT", "PD ACT %", "PD AOP", "PD AOP %") AS WITH CTE_A06000
As
(
	select  a.Parent_Account_Number,b.Account_Desc ,a.Fiscal_Period_Number, a.Fiscal_Year, c.Rest_Name ,c.Rest_number
	,round(sum(a.Actual_PTD_Amount),2)  AS "PD ACT",round(sum(a.Budget_PTD_Amount),2) AS "PD AOP" FROM EDW_DEV.EDW.FACT_PNLSUMMARY_PERIOD a JOIN EDW_DEV.EDW.Dim_PnLAccount b 
	 ON a.PARENT_ACCOUNT_NUMBER = b.Account_Number  JOIN EDW_DEV.EDW.Dim_Restaurant c ON 
	 c.Rest_Number = a.Rest_Number
	where  a.PARENT_ACCOUNT_NUMBER IN ('A06000')
	 --and a.fiscal_year = 2019
	--AND a.Fiscal_Period_Number = 1
	 --and a.Rest_Number in (00001) 
	AND c.IsCurrent = 1 and C.Rest_Type ='Arg'
	 GROUP BY 
	 a.Parent_Account_Number , b.Account_Desc,a.Fiscal_Period_Number, a.Fiscal_Year, c.Rest_name,c.Rest_number
 ),
-- SELECT * FROM CTE_A06000

 CTE_All
As
(
	select  a.Parent_Account_Number,b.Account_Desc ,a.Fiscal_Period_Number, a.Fiscal_Year, c.Rest_Name ,c.Rest_number
	,round(sum(a.Actual_PTD_Amount),2)  AS "PD ACT",round(sum(a.Budget_PTD_Amount),2) AS "PD AOP" 
	FROM EDW_DEV.EDW.FACT_PNLSUMMARY_PERIOD a JOIN EDW_DEV.EDW.Dim_PnLAccount b 
	 ON a.PARENT_ACCOUNT_NUMBER = b.Account_Number  JOIN EDW_DEV.EDW.Dim_Restaurant c ON 
	 c.Rest_Number = a.Rest_Number
	where  a.PARENT_ACCOUNT_NUMBER IN ('A06000','A26996' , 'A26997', 'A16997')
	 --and a.fiscal_year = 2019
	 -- AND a.Fiscal_Period_Number = 1
	--and a.Rest_Number in(00001) 
	AND c.IsCurrent = 1  and C.Rest_Type ='Arg'
	 GROUP BY 
	 a.Parent_Account_Number , b.Account_Desc,a.Fiscal_Period_Number, a.Fiscal_Year, c.Rest_name,c.Rest_number
 )

 SELECT b.Parent_Account_Number,b.Account_Desc,b.Fiscal_Period_Number, b.Fiscal_Year,b.Rest_Name,b.Rest_number,
 b."PD ACT",
---((b."PD ACT"/a."PD ACT")*100) + '%' AS "PD ACT %" ,
CAST( (b."PD ACT"/NULLIFZERO(a."PD ACT"))*100  as char(15) ) || '%' AS "PD ACT %",
 b."PD AOP" ,
CAST((b."PD AOP"/NULLIFZERO(a."PD AOP"))*100  AS CHAR(15) ) || '%' AS  "PD AOP %" 
--(b."PD AOP"/a."PD AOP")*100 AS "PD AOP %" 
 FROM CTE_A06000 A INNER JOIN CTE_All B ON A.Rest_number = B.Rest_number AND a.Fiscal_Period_Number = b.Fiscal_Period_Number AND
  a.Fiscal_Year = b.Fiscal_Year;



/* <sc-view> ZVW_REPORT_TLD_COMP_DESC_DAY </sc-view> */
CREATE OR REPLACE VIEW "ZVW_REPORT_TLD_COMP_DESC_DAY"
("DATE_KEY", "ORDER_NUMBER", "REST_NUMBER", "REST_NAME", "REST_ADDRCITY", "REST_ADDRSTATE", "DISCOUNT_DESC", "DISC_AMOUNT", "QUANTITY")
AS
SELECT
    FSLO.Date_Key,
    FSLO.ORDER_NUMBER,
    DR.Rest_Number,
    DR.Rest_Name,
    DR.Rest_AddrCity,
    DR.Rest_AddrState,
    DDIS.Discount_Desc,
    SUM(FSLO.Discount_Amount) AS Disc_Amount,
    SUM(FSLO.Gross_Quantity) AS Quantity
FROM
    EDW_DEV.EDW.FACT_SALES_ORDER FSO
INNER JOIN
        EDW_DEV.EDW.FACT_SALES_ORDER_LINE FSLO ON FSO.Order_ID=FSLO.Order_ID
INNER JOIN
        EDW_DEV.EDW.DIM_DISCOUNT DDIS ON DDIS.Discount_key = FSLO.Discount_Key
INNER JOIN
        EDW_DEV.EDW.DIM_RESTAURANT DR ON DR.Restaurant_key=FSO.Restaurant_Key
WHERE FSO.Discount_Amount>0 AND FSLO.Discount_Amount>0 AND FSO.Payment_Amount>0
GROUP BY
    	DR.Rest_Number,
    	FSLO.ORDER_NUMBER,
    	FSLO.Date_Key,
    	DR.Rest_Name,
    	DR.Rest_AddrCity,
    	DR.Rest_AddrState,
    	DDIS.Discount_Desc;



/* <sc-view> VW_DT_SPEED_OF_SERVICE_BYCAR </sc-view> */

CREATE OR REPLACE VIEW "VW_DT_SPEED_OF_SERVICE_BYCAR" ("REST_NUMBER", "ARG_AREA_NAME", "ARG_DISTRICT_NAME", "ARG_SUBREGION_NAME", "Date", "DAYS_DATE", "FISCAL_YEAR", "FISCAL_PERIOD_NUMBER", "FISCAL_WEEK_NUMBER", "DAY_PART_TEXT", "SPEAKER_ARR_TIME", "SPEAKER_DEPART_TIME", "WINDOW_ARR_TIME", "WINDOW_DEPART_TIME", "Speaker Time (sec)", "Window Time (sec)", "Total Time (sec)", "Stack Time (sec)") AS SELECT 
	    DR.Rest_Number
	   ,DR.Arg_Area_Name
	   ,DR.Arg_District_Name
	   ,DR.Arg_SubRegion_Name
       ,SOSB.Date_Key AS "Date"
	   ,DD.Days_Date
	   ,DD.Fiscal_Year
	   ,DD.Fiscal_Period_Number
	   ,DD.Fiscal_Week_Number
	   ,DT.Day_Part_Text
	   ,RIGHT(DT.Time_Of_Day_Time,15) AS Speaker_Arr_Time
	   ,RIGHT(DT1.Time_Of_Day_Time,15)  AS Speaker_Depart_Time
	   ,RIGHT(DT2.Time_Of_Day_Time,15)  AS Window_Arr_Time
       ,RIGHT(DT3.Time_Of_Day_Time,15)  AS Window_Depart_Time 
	   ,CASE WHEN 
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT.Time_Of_Day_Time AS TIMESTAMP),15))) > 
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT1.Time_Of_Day_Time AS TIMESTAMP),15)))
			THEN 24*3600 - ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT.Time_Of_Day_Time AS TIMESTAMP),15))) ,
											TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT1.Time_Of_Day_Time AS TIMESTAMP),15)))))
			ELSE ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT.Time_Of_Day_Time AS TIMESTAMP),15))) ,
								TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT1.Time_Of_Day_Time AS TIMESTAMP),15)))))
		END AS "Speaker Time (sec)"
	   ,CASE WHEN 
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT2.Time_Of_Day_Time AS TIMESTAMP),15))) >
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT3.Time_Of_Day_Time AS TIMESTAMP),15)))
			THEN 24*3600 - ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT2.Time_Of_Day_Time AS TIMESTAMP),15))) ,
											TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT3.Time_Of_Day_Time AS TIMESTAMP),15)))))
			ELSE ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT2.Time_Of_Day_Time AS TIMESTAMP),15))) ,
								TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT3.Time_Of_Day_Time AS TIMESTAMP),15)))))
		END AS "Window Time (sec)"
	   ,CASE WHEN 
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT.Time_Of_Day_Time AS TIMESTAMP),15))) >
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT3.Time_Of_Day_Time AS TIMESTAMP),15)))
			THEN 24*3600 - ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT.Time_Of_Day_Time AS TIMESTAMP),15))) ,
											TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT3.Time_Of_Day_Time AS TIMESTAMP),15)))))
			ELSE ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT.Time_Of_Day_Time AS TIMESTAMP),15))) ,
								TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT3.Time_Of_Day_Time AS TIMESTAMP),15)))))
		END AS "Total Time (sec)"
	   ,CASE WHEN 
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT1.Time_Of_Day_Time AS TIMESTAMP),15))) >
		TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT2.Time_Of_Day_Time AS TIMESTAMP),15)))
			THEN 24*3600 - ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT1.Time_Of_Day_Time AS TIMESTAMP),15))) ,
											TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT2.Time_Of_Day_Time AS TIMESTAMP),15)))))
			ELSE ABS(TIMESTAMPDIFF(SECOND,TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Speaker_Depart_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT1.Time_Of_Day_Time AS TIMESTAMP),15))) ,
								TO_TIMESTAMP(CONCAT(CAST(TO_DATE(CAST(Window_Arr_DateKey as VARCHAR(10)),'YYYYMMDD') AS VARCHAR(100)),' ',RIGHT(CAST(DT2.Time_Of_Day_Time AS TIMESTAMP),15)))))
		END AS "Stack Time (sec)"
FROM EDW_DEV.EDW.Fact_SpeedOfService_ByCar SOSB
  INNER JOIN EDW_DEV.EDW.Dim_Restaurant DR
  ON SOSB.Restaurant_Key=DR.Restaurant_Key
  INNER JOIN EDW_DEV.EDW.Dim_Date DD
  ON SOSB.Date_Key=DD.Date_Key
  INNER JOIN EDW_DEV.EDW.Dim_Time DT
  ON SOSB.Speaker_Arr_TimeKey = DT.Time_key
  INNER JOIN EDW_DEV.EDW.Dim_Time DT1
  ON SOSB.Speaker_Depart_TimeKey = DT1.Time_key
  INNER JOIN EDW_DEV.EDW.Dim_Time DT2
  ON SOSB.Window_Arr_TimeKey = DT2.Time_key
  INNER JOIN EDW_DEV.EDW.Dim_Time DT3
  ON SOSB.Window_Depart_TimeKey = DT3.Time_key
;



/* <sc-view> VW_REPORT_TLD_CHECK_VIEWER_WITH_LINE_ID_ROARK </sc-view> */
CREATE OR REPLACE VIEW "VW_REPORT_TLD_CHECK_VIEWER_WITH_LINE_ID_ROARK"
("ORDER_ID", "ORDER_NUMBER", "ORDER_NAME", "DATE_KEY", "REST_NUMBER", "REST_ADDRCITY", "REST_ADDRSTATE", "REST_ADDRLINE1", "REST_ADDRPHONE", "GUEST_COUNT", "POS_ITEM_DESC", "PLU_DESC", "DISCOUNT_DESC", "POP_DESC", "LINE_ITEM_ID", "ITEM_NET_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "TAX_AMOUNT", "PAYMENT_AMOUNT", "EMPLOYEE_ID", "EMPLOYEE_NAME", "CLOSETIME", "IS_VOID", "IS_CLEAR", "IS_REFUND", "PAYMENT_NAME", "PAYMENT_LAST4")
AS
SELECT
	FSO.Order_ID,
	FSO.Order_Number,
	FSO.Order_Name,
	FSO.Date_Key,
	DR.Rest_Number,
	DR.Rest_AddrCity,
	DR.Rest_AddrState,
	DR.Rest_AddrLine1,
	DR.Rest_AddrPhone,
	FSo.Guest_Count,
	DMI.POS_Item_Desc,
	DMI.PLU_Desc,
		IFNULL(DDIS.Discount_Desc,'NA') AS Discount_Desc,
		IFNULL(DPOP.POP_Desc,'NA') AS POP_Desc,
fsol.line_item_id,
	FSOL.Net_Amount AS Item_Net_Amount,
	FSOL.Discount_Amount,
	FSO.Net_Amount,
	FSO.Tax_Amount,
	FSO.Payment_Amount,
		IFNULL(DE.Employee_ID,0) AS Employee_ID,
		IFNULL(DE.Employee_Name,'NA') AS Employee_Name,
	LEFT(RIGHT(DT.TIME_OF_DAY_TIME,15),8) 	AS CloseTime,
	FSOL.IS_VOID,
	FSOL.IS_CLEAR,
	FSOL.IS_REFUND,
	P.PAYMENT_NAME,
	P.PAYMENT_LAST4


FROM
	EDW_DEV.EDW.Fact_Sales_Order FSO
INNER JOIN
		EDW_DEV.EDW.Fact_Sales_Order_Line FSOL ON FSO.Order_ID=FSOL.Order_ID AND FSO.Date_Key =FSOL.Date_Key
INNER JOIN
		EDW_DEV.EDW.Dim_Restaurant DR ON DR.Restaurant_key=FSO.Restaurant_Key
INNER JOIN
		EDW_DEV.EDW.Dim_MenuItem DMI ON DMI.Menu_key=FSOL.Menu_Key
INNER JOIN
		EDW_DEV.EDW.Dim_PointOfPurchase DPOP ON DPOP.POP_key=FSO.POP_Key
LEFT JOIN
		EDW_DEV.EDW.Dim_Employee DE ON DE.Employee_Key=FSO.Employee_Key
INNER JOIN
		EDW_DEV.EDW.Dim_Time DT ON DT.TIME_KEY=FSO.TIME_KEY
LEFT JOIN
		EDW_DEV.EDW.Dim_Discount DDIS ON DDIS.Discount_Key = FSOL.Discount_Key
LEFT JOIN
		EDW_DEV.EDW.FACT_SALES_ORDER_PAYMENT P ON FSO.ORDER_ID=P.ORDER_ID AND P.PAYMENT_DESC = 'CreditCard'

		;


/* <sc-view> VW_ACTIVEALLRESTAURANTS </sc-view> */
CREATE OR REPLACE VIEW "VW_ACTIVEALLRESTAURANTS"
("RESTAURANT_KEY", "REST_NUMBER", "REST_NAME", "REST_BRAND_ISACTIVE", "REST_DMA_CODE", "REST_DMA_DESC", "REST_ISAIRPORT", "REST_ISVIDEOSURV", "REST_ISBREAKFAST", "REST_IS24HR", "REST_ISMALL", "REST_ISDRIVETHRU", "REST_ADDRPHONE", "REST_ADDRLINE1", "REST_ADDRLINE2", "REST_ADDRCITY", "REST_ADDRCOUNTY", "REST_ADDRSTATE", "REST_ADDRZIP", "REST_COUNTRY", "REST_LATITUDE", "REST_LONGITUDE", "REST_BUILDINGTYPE", "REST_REMODELDT", "REST_ACQUISITIONDT", "REST_POSTYPE", "REST_COMP_ISELIGIBLE", "REST_COMP_ISTEMPCLOSURE", "REST_COMP_DATE", "REST_OPENDATE", "REST_CLOSEDATE", "REST_STATUS", "REST_TYPE", "REST_FACILITYTYPE", "REST_SEATCOUNT", "REST_PARKINGCOUNT", "REST_SQUAREFT", "REST_FRANCHISEE_NUMBER", "REST_FRAN_NAME", "REST_FRANENTITY_NAME", "REST_CSRASSIGNED_NAME", "REST_CSRDEFAULT_NAME", "REST_DISTCENTER_NAME", "REST_OC_NAME", "REST_REMODELPROG_NAME", "REST_LICENSE_NUMBER", "REST_AFA_PERC", "REST_ROYALTY_PER", "EFFECTIVE_BEGIN_DATE", "EFFECTIVE_END_DATE", "ISACTIVE", "INFERRED_FLAG", "LOAD_ID", "LAST_UPDATE_DATE_TIME")
AS
SELECT
            DR.Restaurant_key,
            DR.Rest_Number,
            DR.Rest_Name,
            DR.Rest_Brand_IsActive,
            DR.Rest_DMA_Code,
            DR.Rest_DMA_Desc,
            DR.Rest_IsAirport,
            DR.Rest_IsVideoSurv,
            DR.Rest_IsBreakfast,
            DR.Rest_Is24Hr,
            DR.Rest_IsMall,
            DR.Rest_IsDriveThru,
            DR.Rest_AddrPhone,
            DR.Rest_AddrLine1,
            DR.Rest_AddrLine2,
            DR.Rest_AddrCity,
            DR.Rest_AddrCounty,
            DR.Rest_AddrState,
            DR.Rest_AddrZip,
            DR.Rest_Country,
            DR.Rest_Latitude,
            DR.Rest_Longitude,
            DR.Rest_BuildingType,
            DR.Rest_RemodelDT,
            DR.Rest_AcquisitionDt,
            DR.Rest_POSType,
            DR.Rest_Comp_IsEligible,
            DR.Rest_Comp_IsTempClosure,
            DR.Rest_Comp_Date,
            DR.Rest_OpenDate,
            DR.Rest_CloseDate,
            DR.Rest_Status,
            DR.Rest_Type,
            DR.Rest_FacilityType,
            DR.Rest_SeatCount,
            DR.Rest_ParkingCount,
            DR.Rest_SquareFt,
            DR.Rest_Fran_Number Rest_FRANCHISEE_NUMBER,
            DR.Rest_Fran_Name,
            DR.Rest_FranEntity_Name,
            DR.Rest_CSRAssigned_Name,
            DR.Rest_CSRDefault_Name,
            DR.Rest_DistCenter_Name,
            DR.Fran_OC_Name Rest_OC_Name,
            DR.Rest_RemodelProg_Name,
            DR.Rest_License_Number,
            DR.Rest_AFA_Perc,
            DR.Rest_Royalty_Per,
            DR.Effective_Begin_Date,
            DR.Effective_End_Date,
            DR.IsCurrent IsActive,
            DR.Inferred_Flag,
            DR.Load_ID,
            DR.Last_Update_Date_Time
FROM
            EDW_DEV.EDW.Dim_Restaurant AS DR
WHERE   ( DR.IsCurrent = 1 )
        AND ( DR.Rest_Status = 'Open');



/* <sc-view> VW_PNLDETAIL_PERIOD </sc-view> */
CREATE OR REPLACE VIEW "VW_PNLDETAIL_PERIOD"
("FISCAL_PERIOD_NUMBER", "FISCAL_YEAR", "REST_NUMBER", "REST_NAME", "REST_STATUS", "ARG_AREA_NUMBER", "ARG_AREA_NAME", "ARG_DISTRICT_NUMBER", "ARG_DISTRICT_NAME", "ARG_SUBREGION_NUMBER", "ARG_SUBREGION_NAME", "ARG_REGION_NUMBER", "ARG_REGION_NAME", "ACCOUNT_SORT_NUMBER", "CHILD_ACCOUNT_NUMBER", "CHILD_ACCOUNT_TYPE", "CHILD_ACCOUNT_DESC", "PARENT_ACCOUNT_NUMBER", "PARENT_ACCOUNT_DESC", "LINE_NUMBER", "LINE_NAME", "DESCRIPTION", "INVOICE_NUMBER", "INVOICE_DATE", "VENDOR_NAME", "BATCH_NAME", "SOURCE_NAME", "ACTUAL_PTD_AMOUNT", "ISACTIVE_STORE_VIEW", "ISACTIVE_AREA_VIEW", "ISACTIVE_DISTRICT_VIEW")
AS
SELECT
          FISCAL_PERIOD_NUMBER,
          FISCAL_YEAR,
          RES.REST_NUMBER,
          RES.REST_NAME,
          RES.REST_STATUS,
          RES.ARG_AREA_NUMBER,
          RES.ARG_AREA_NAME,
          RES.ARG_DISTRICT_NUMBER,
          RES.ARG_DISTRICT_NAME,
          RES.ARG_SUBREGION_NUMBER,
          RES.ARG_SUBREGION_NAME,
          RES.ARG_REGION_NUMBER,
          RES.ARG_REGION_NAME,
          ACCOUNT_SORT_NUMBER,
          CHILD_ACCOUNT_NUMBER,
          CHILD_ACCOUNT_TYPE,
          CHILD_ACCOUNT_DESC,
          PARENT_ACCOUNT_NUMBER,
          PARENT_ACCOUNT_DESC,
          LINE_NUMBER,
          LINE_NAME,
          LINE_DESCRIPTION DESCRIPTION,
          INVOICE_NUMBER,
          CASE
                    WHEN RTRIM(INVOICE_DATE) = '1753-01-01'
                              THEN CAST ('' AS VARCHAR (10))
             ELSE LEFT(INVOICE_DATE, 10)
          END
             INVOICE_DATE,
          VENDOR_NAME,
          BATCH_NAME,
          SOURCE_NAME,
          ACTUAL_PTD_AMOUNT,
          ISACTIVE_STORE_VIEW,
          ISACTIVE_AREA_VIEW,
          ISACTIVE_DISTRICT_VIEW
     FROM
          EDW_DEV.EDW.FACT_PNLSUMMARY_DETAIL_PERIOD PSD
          JOIN
                    EDW_DEV.EDW.DIM_RESTAURANT RES
             ON PSD.REST_NUMBER = RES.REST_NUMBER AND RES.ISCURRENT = 1 AND REST_TYPE = 'Arg';