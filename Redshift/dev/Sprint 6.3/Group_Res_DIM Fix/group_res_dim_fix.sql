SELECT d_res_id, 
       d_group_id, 
       d_rec_start_dttm, 
       d_rec_end_dttm, 
       load_date, 
       res_id, 
       group_id, 
       md5_landing, 
       etl_ld_dt , 
       etl_upd_dt, 
       next_load_date, 
       md5_dim, 
       curr_rec_cnt, 
       tot_rec_cnt, 
       CASE 
              WHEN ( 
                            ( 
                                   d_res_id IS NULL 
                            AND    d_group_id IS NULL
							/*Added as part of change - All the res_id with only valid group_ids should get inserted*/
							AND group_id<>-1
							/*End*/
							) 
                     OR     ( 
                                   d_res_id IS NOT NULL 
                            AND    d_group_id IS NOT NULL 
                            AND    res_id IS NOT NULL 
                            AND    group_id IS NOT NULL 
                            AND    d_rec_end_dttm = '9999-12-31 00:00:00' 
                            AND    md5_landing <> md5_dim 
                            AND    curr_rec_cnt = tot_rec_cnt 
                            AND    load_date > d_rec_start_dttm)) THEN 'FI_FIMR_UIL' 
              WHEN ( 
                            d_res_id IS NOT NULL 
                     AND    d_group_id IS NOT NULL 
                     AND    res_id IS NOT NULL 
                     AND    group_id IS NOT NULL 
                     AND    d_rec_end_dttm = '9999-12-31 00:00:00' 
                     AND    md5_landing <> md5_dim 
                     AND    load_date > d_rec_start_dttm 
                     AND    curr_rec_cnt <> tot_rec_cnt) THEN 'UUL_Land' 
              WHEN ( 
                            d_res_id IS NOT NULL 
                     AND    d_group_id IS NOT NULL 
                     AND    res_id IS NOT NULL 
                     AND    group_id IS NOT NULL 
                     AND    d_rec_end_dttm = '9999-12-31 00:00:00' 
                     AND    load_date > d_rec_start_dttm 
                     AND    curr_rec_cnt = 1) THEN 'UUL_Dim' 
			/*Introducing below condition to invalidate those records which have invalidated SKID*/
			  WHEN (
							rd_rec_end_dttm <>'9999-12-31 00:00:00' 
					 OR		gd_rec_end_dttm <>'9999-12-31 00:00:00'	
					) THEN '??' --Note this condition needs to be handled in Talend to update the DIM record with rec_end_dttm=load_date
			/*End*/			  
              WHEN ( 
                            ( 
                                   d_res_id IS NOT NULL 
                            AND    d_group_id IS NOT NULL 
                            AND    res_id IS NULL 
                            AND    group_id IS NULL ) 
                     OR     ( 
                                   d_res_id IS NOT NULL 
                            AND    d_group_id IS NOT NULL 
                            AND    res_id IS NOT NULL 
                            AND    group_id IS NOT NULL 
                            AND    d_rec_end_dttm = '9999-12-31 00:00:00' 
                            AND    md5_landing = md5_dim 
                            AND    load_date >= d_rec_start_dttm ) ) THEN 'HNI_SLC' 
       END AS etl_ld_status 
FROM   ( 
                 SELECT    d.res_id         AS d_res_id, 
                           d.group_id       AS d_group_id, 
                           d.rec_start_dttm AS d_rec_start_dttm, 
                           d.rec_end_dttm   AS d_rec_end_dttm, 
                           t.load_date, 
                           t.res_id, 
                           t.group_id, 
                           t.md5_landing, 
                           Lead(t.load_date) OVER (partition BY t.res_id,t.group_id ORDER BY t.load_date ) AS next_load_date,
                           md5_dim, 
                           Cast(Row_number() OVER (partition BY t.res_id,t.group_id ORDER BY t.load_date ) AS BIGINT) AS curr_rec_cnt,
                           Cast(Count(*) OVER (partition BY t.res_id,t.group_id ) AS                          BIGINT) AS tot_rec_cnt,
                           d.etl_ld_dt, 
                           d.etl_upd_dt,
						   d.rd_rec_end_dttm,
						   d.gd_rec_end_dttm 
                 FROM      /*( 
                                  SELECT * 
                                  FROM   vv_db.hvtb_nbx_core_sw_group_res_dim 
                                  WHERE  rec_end_dttm = '9999-12-31 00:00:00')d */
							(SELECT grd.res_id,grd.group_id,grd.rec_start_dttm,grd.rec_end_dttm,grd.etl_ld_dt,grd.etl_upd_dt,
							rd.rec_end_dttm rd_rec_end_dttm, gd.rec_end_dttm gd_rec_end_dttm   
                                  FROM   vv_db.hvtb_nbx_core_sw_group_res_dim grd 
                                  JOIN vv_db.hvtb_nbx_core_sw_reservation_dim rd on grd.res_id = rd.res_id 
                                  JOIN vv_db.hvtb_nbx_core_sw_group_dim gd on gd.group_id = grd.group_id 
                                  WHERE  grd.rec_end_dttm = '9999-12-31 00:00:00') d 	  
                 FULL JOIN 
                           ( 
                                    SELECT   temp.res_id, 
                                             temp.group_id, 
                                             temp.load_date, 
                                             temp.md5_landing, 
                                             CASE 
                                                      WHEN temp.md5_landing = Lag(temp.md5_landing) OVER (partition BY temp.res_id/*, temp.group_id*/ ORDER BY temp.load_date) THEN 'dup'
                                                      ELSE 'nodup' 
                                             END AS dup_check 
                                    FROM     ( 
                                                        SELECT     res_dim.res_id                  AS res_id,
                                                                   COALESCE(group_dim.group_id,-1) AS group_id,
                                                                   load_date, 
                                                                   Md5(Concat(COALESCE(res_dim.res_id,0),COALESCE(group_dim.group_id,0))) AS md5_landing
                                                        FROM       ( 
                                                                              SELECT     load_date,
                                                                                         reshdr.res_id,
                                                                                         COALESCE(reshdr.group_id,-1) AS group_id
                                                                              FROM       ( 
                                                                                                  SELECT   rec_res_id,
																											res_id,
                                                                                                           group_id
                                                                                                  FROM     vv_db.hvtb_nbx_landing_sw_dwh_res_header
                                                                                                  GROUP BY 
																											rec_res_id,
																											res_id,
                                                                                                           group_id) reshdr
                                                                              INNER JOIN 
                                                                                         ( 
                                                                                                  SELECT   Max (dwh.load_date) AS load_date,
                                                                                                           Max(dwh.entity_id)  AS entity_id,
                                                                                                           entity_origin_id
                                                                                                  FROM     vv_db.hvtb_nbx_landing_sw_dwh_extract_load dwh
                                                                                                  WHERE    dwh.entity_type = 'RES'
                                                                                                  GROUP BY entity_origin_id ) el
                                                                              --ON         reshdr.res_id = el.entity_origin_id 
																			  ON         reshdr.rec_res_id = el.entity_id 
                                                                              GROUP BY   load_date,
                                                                                         res_id,
                                                                                         group_id ) landing
                                                        INNER JOIN vv_db.hvtb_nbx_core_sw_reservation_dim res_dim
                                                        ON         landing.res_id = res_dim.src_res_id
                                                        AND        to_date(res_dim.rec_end_dttm) = '9999-12-31' 
                                                        LEFT JOIN  vv_db.hvtb_nbx_core_sw_group_dim group_dim
                                                        ON         landing.group_id = group_dim.src_group_id
                                                        AND        to_date(group_dim.rec_end_dttm) = '9999-12-31' and group_dim.group_id <> -1
                                                        --WHERE      group_dim.group_id <> -1 
                                                        GROUP BY   res_dim.res_id, 
                                                                   group_dim.group_id, 
                                                                   load_date)temp)t 
                 ON        d.res_id = t.res_id 
                 --AND       d.group_id = t.group_id 
                 WHERE     COALESCE(dup_check, 'nodup') = 'nodup')tab