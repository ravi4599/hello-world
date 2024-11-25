CREATE OR REPLACE view IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE
(
    BRAND_ID,
    REST_ID,
    CALC_OPEN_DATE   COMMENT 'This is calculated Permanent open date for a brand + Restid based of the logic below' ,
    CALC_CLOSURE_DATE COMMENT 'This is calculated Permanent Close date for a brand + Restid if max(clouser date) is blank take max effective end date for all corp 
    condition'
) AS
    WITH CTE_SCD_DATA AS
    (
        SELECT brand_id, rest_id, fran_ind , OPEN_DATE, CLOSURE_DATE, effective_start_date, effective_end_date, curr_ind
        FROM "IDH_{{params.env}}"."D_LOC"."REST_SCD"  
        Where fran_ind is not null   AND   open_date is not null  
    )
    SELECT  brand_id, rest_id ,Calc_Open_Date, Calc_closure_date 
    FROM (
        -- when multiple fran_ind and current record is frand_ind = true then take min open_Date for all fran_ind = false
        -- when multiple fran_ind and current record is frand_ind = false then take min effective_start_date  for all fran_ind = false
        -- when single false fran_ind take min open_date 
            select  RS.brand_id, RS.rest_id , count(distinct fran_ind) Tot_Transfer , MAX(CR.fran_ind) Curr_Fran_Ind
            , CASE WHEN Tot_Transfer > 1 AND Curr_Fran_Ind = false THEN MIN(case when fran_ind = false then effective_start_date else null end ) else MIN(CR.open_date)  END Calc_Open_Date
            , CASE WHEN Tot_Transfer > 1 AND Curr_Fran_Ind = true  THEN MAX(case when fran_ind = false then effective_end_date   else null end )
            ELSE COALESCE( MAX(CR.CLOSURE_DATE), MAX(case when fran_ind = false  then effective_end_date else null end ),'9999-12-31') END Calc_closure_date
            from CTE_SCD_DATA RS             
            join ( select brand_id, rest_id, fran_ind , OPEN_DATE, CLOSURE_DATE 
                    FROM CTE_SCD_DATA  where curr_ind = true
                 ) CR using (brand_id, rest_id)
            group by RS.brand_id, RS.rest_id having  (Curr_Fran_Ind = false or Tot_Transfer = 2) AND Year(Calc_closure_date) > YEAR(CURRENT_DATE) - 3
    ) t1
;

CREATE OR REPLACE view IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE
(
    BRAND_ID,
    REST_ID,
    TEMP_CLOSE_DATE   COMMENT 'This is calculated Date on which a Rest is temp close for business' 
) AS
     select distinct  t1.Rest_id, t1.BRAND_ID, calendar_dt 
    --, temp_closed_date , CALC_RE_OPEN_DATE 
    FROM (
           SELECT  rest_id, brand_id, temp_closed_date , ifnull(REOPEN_DATE,EFFECTIVE_END_DATE) CALC_RE_OPEN_DATE
           from  "IDH_{{params.env}}"."D_LOC"."REST_SCD" 
           join (
                    select BRAND_ID, REST_ID, TEMP_CLOSED_DATE , MAX(EFFECTIVE_START_DATE) EFFECTIVE_START_DATE
                    from  "IDH_{{params.env}}"."D_LOC"."REST_SCD"
                    Where  rest_scd.fran_ind = FALSE AND TEMP_CLOSED_DATE IS NOT NULL
                    group by BRAND_ID, REST_ID, TEMP_CLOSED_DATE
            ) using (BRAND_ID, REST_ID, TEMP_CLOSED_DATE,EFFECTIVE_START_DATE)
            --this is needed to control number of rows return we don;t want to read those rows which changed there status before 1 year back
            Where CALC_RE_OPEN_DATE > DATEADD(Year,-1,CURRENT_DATE)  
    ) t1
    inner join "IDS_{{params.env}}"."INT_REF_BV"."DATE_DIM_BV"  c on  c.calendar_dt  > temp_closed_date and c.calendar_dt < CALC_RE_OPEN_DATE 
    where 1=1
    and c.calendar_dt BETWEEN DATEADD(Year,-2,CURRENT_DATE) AND CURRENT_DATE + 1
;
