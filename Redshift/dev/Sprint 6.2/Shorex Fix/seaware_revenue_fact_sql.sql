SELECT CURRENT_TIMESTAMP()                               AS snapshot_date, 					
       COALESCE(rd.res_id, -1)                           AS res_id, 					
       COALESCE(gm.guest_id, -1)                         AS guest_id,
	   /*Fix for shorex - START*/
	   --COALESCE(pm.package_id, -1)                       AS package_id, 		
	   COALESCE(CASE WHEN pm.package_id IS NULL THEN shorex_pm.package_id ELSE pm.package_id END, -1) AS package_id, 	
       /*Fix for shorex - End*/
       COALESCE(invitm.invoice_item_type_id, -1)         AS invoice_item_type_id 					
       , 					
       COALESCE(lkparea.price_area_id, -1)               AS 					
       price_area_id, 					
       COALESCE(sh.ship_id, -1)                          AS ship_id, 					
       COALESCE(am.agent_id, -1)                         AS agent_id, 					
       COALESCE(agency.agency_id, -1)                    AS agency_id, 					
       CASE 					
         WHEN Length(inv.promo_code) = 0 THEN -1 					
         ELSE COALESCE(pro_lkp.promotion_id, -1) 					
       END                                               AS promotion_id, 					
       COALESCE(addon_id, -1)                            AS addon_id, 					
       -1                                                AS cardeck_id, 					
       -1                                                AS shorex_id, 					
	   /*Hotel Request Change - START*/				
       ---1                                                AS hotel_id, --Rename hotel_id to hotel_res_req_id					
	   COALESCE(hd.hotel_res_req_id, -1)				AS hotel_res_req_id,
	   /*Hotel Request Change - END*/				
       COALESCE(sail.sail_id, -1)                        AS sail_id, 					
       COALESCE(( Round(inv.amount, 2) ), 0)             AS amount, 					
       COALESCE(( Round(inv.commission_percent, 2) ), 0) AS commission_percent, 					
       CASE 					
         WHEN gm.guest_seqn = 1 					
               OR gm.guest_seqn IS NULL THEN COALESCE(reshdr.net_due, -1) 					
         ELSE 0 					
       END                                               AS net_due, 					
       NULL                                              AS addon_qty, 					
       COALESCE(reshdr.currency, 'USD')                  AS currency, 					
       COALESCE(reshdr.currency_rate, 0)                 AS currency_rate 					
FROM   (SELECT Max(dwh.load_date) AS load_date, 					
               Max(dwh.entity_id) AS entity_id, 					
               entity_origin_id 					
        FROM   vv_db.hvtb_nbx_landing_sw_dwh_extract_load dwh 					
        WHERE  dwh.entity_type = 'RES' 					
        GROUP  BY entity_origin_id) el 					
       JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_invoice inv 					
         ON inv.rec_res_id = el. entity_id 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_reservation_dim rd 					
              ON el.entity_origin_id = rd.src_res_id 					
                 AND To_date(rd.rec_end_dttm) = '9999-12-31' 					
       LEFT JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_guest rg 					
              ON inv.rec_guest_id = rg.rec_guest_id 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_guest_dim gm 					
              ON rg.guest_id = gm.src_guest_id 					
                 AND To_date(gm.rec_end_dttm) = '9999-12-31' 					
       LEFT JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_package rp 					
              ON inv.rec_pkg_id = rp.rec_pkg_id 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_package_dim pm 					
              ON rp.package_id = pm.src_package_id 					
                 AND To_date(pm.rec_end_dttm) = '9999-12-31' 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_invoice_item_type_lkp invitm 					
              ON inv.invoice_item_type = invitm.invoice_item_type 					
                 AND ( CASE 					
                         WHEN Length(inv.invoice_item_subtype) = 0 					
                               OR inv.invoice_item_subtype IS NULL THEN '-1' 					
                         ELSE inv.invoice_item_subtype 					
                       END ) = COALESCE(invitm.invoice_item_subtype, '-1') 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_price_area_lkp lkparea 					
              ON inv.price_area = lkparea.price_area 					
       LEFT JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_header reshdr 					
              ON inv.rec_res_id = reshdr.rec_res_id 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_ship_dim sh 					
              ON reshdr.ship = sh.ship 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_agency_dim Agency 					
              ON reshdr.agency_id = Agency.src_agency_id 					
                 AND To_date(Agency.rec_end_dttm) = '9999-12-31' 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_sail_dim sail 					
              ON rp.sail_id = sail.src_sail_id 					
                 AND To_date(sail.rec_end_dttm) = '9999-12-31' 					
       LEFT JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_addon addon 					
              ON inv.rec_addon_id = addon.rec_addon_id 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_addon_lkp al 					
              ON Nvl(addon.res_addon_code, '~') = Nvl(al.res_addon_code, '~') 					
                 AND Nvl(addon.addon_category, '~') = 					
                     Nvl(al.addon_category, '~') 					
                 AND Nvl(addon.addon_type, '~') = Nvl(al.addon_type, '~') 					
                 AND Nvl(addon.addon_name, '~') = Nvl(al.addon_name, '~') 					
       LEFT JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_promotion pro 					
              ON inv.rec_res_id = pro.rec_res_id 					
                 AND inv.rec_guest_id = pro.rec_guest_id 					
                 AND inv.rec_pkg_id = pro.rec_pkg_id 					
                 AND Nvl(inv.promo_code, '~') = Nvl(pro.promo_code, '~') 					
       LEFT JOIN vv_db.hvtb_nbx_core_sw_promotion_lkp pro_lkp 					
              ON Nvl(pro.promo_group, '~') = Nvl(pro_lkp.promo_group, '~') 					
                 AND Nvl(pro.promo_code, '~') = Nvl(pro_lkp.promo_code, '~') 					
                 AND Nvl(pro.promo_name, '~') = Nvl(pro_lkp.promo_name, '~') 					
       LEFT JOIN vv_db.hvtb_nbx_core_crm_agent_master_dim am 					
              ON reshdr.agent_id = am.seaware_agent_id__c 					
                 AND To_date(am.rec_end_dttm) = '9999-12-31' 
		/*Fix for shorex - START*/ 		 
	    LEFT JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_shorex shorex on inv.rec_shorex_id = shorex.rec_shorex_id 
		LEFT JOIN vv_db.hvtb_nbx_core_sw_package_dim shorex_pm on shorex.PACKAGE_ID= shorex_pm.src_package_id 
		AND To_date(shorex_pm.rec_end_dttm) = '9999-12-31' 
		/*Fix for shorex - END*/
		/*Hotel Request Change - START*/		 	
	   LEFT JOIN vv_db.hvtb_nbx_landing_sw_dwh_res_hotel ht 				
			ON inv.rec_hotel_id=ht.rec_hotel_id		
	   LEFT JOIN vv_db.hvtb_nbx_core_sw_dwh_res_hotel_dim hd 				
			ON reshdr.res_id=hd.src_res_id and rg.guest_id=hd.src_guest_id and ht.request_type=hd.request_type and ht.room_seq_number=hd.room_seq_number 		
	
		/*Hotel Request Change - END*/			
