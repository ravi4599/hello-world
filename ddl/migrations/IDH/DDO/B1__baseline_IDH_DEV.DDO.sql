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
create view IF NOT EXISTS CMX_AUDIT_DETAIL(
	RPTG_AUDIT_KEY,
	RPTG_STORE_KEY,
	AUDIT_CATEGORY_DESC,
	CMX_ID,
	AUDIT_ID,
	STORE_ID,
	CMX_AUDIT_SERVICE_NM,
	CMX_AUDIT_SERVICE_VERSION_NBR,
	CMX_GROUP_NM,
	CMX_REPORT_TAG_NM,
	CMX_REPORT_TAG_POINT_QTY,
	CMX_REPORT_TAG_MAXIMUM_POINT_QTY,
	AUDIT_SCORE,
	AUDIT_DT_KEY,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_NBR,
	RESTAURANT_TYPE,
	STORE_FRANCHISE_NM,
	STORE_FRANCHISE_ENTITY_NM,
	STORE_DIVISION,
	STORE_DMA_CD,
	STORE_DMA_DESC,
	FRANCHISE_CONSULTANT1_NAME,
	FRANCHISE_CONSULTANT2_NAME,
	FRANCHISE_CONSULTANT3_NAME,
	LEVEL1_MANAGER_NM,
	LEVEL2_MANAGER_NM,
	LEVEL3_MANAGER_NM,
	LEVEL4_MANAGER_NM,
	LEVEL5_MANAGER_NM,
	DVP
) as (
    WITH cmx_audit_cte AS (
        SELECT
            concat(brand_id, cmx_id, cmx_audit_id) as RPTG_AUDIT_KEY,
            concat(brand_id, lpad(store_id, 5, 0)) as RPTG_STORE_KEY,
            concat(cmx_group_nm, ':', cmx_report_tag_nm) as AUDIT_CATEGORY_DESC,
            cmx_id,
            cmx_audit_id as AUDIT_ID,
            lpad(store_id, 5, 0) as STORE_ID,
            cmx_audit_service_nm,
            cmx_audit_service_version_nbr,
            cmx_group_nm,
            cmx_report_tag_nm,
            cmx_report_tag_point_qty,
            cmx_report_tag_maximum_point_qty,
            cmx_report_tag_val as AUDIT_SCORE,
            to_char(audit_dt, 'YYYYMMDD') as AUDIT_DT_KEY,
            brand_id,
            source_system_nm
        FROM
            IDH_DEV.LOCN.CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV
    )
    SELECT
        cmx_audit.*,
        dateDim.fiscal_week_nbr,
        dateDim.fiscal_year_nbr,
        dateDim.fiscal_period_nbr,
        DDO_EXTENSION.restaurant_type as restaurant_type,
        DDO_Extension.franchisee_name as STORE_FRANCHISE_NM,
        DDO_Extension.legal_name as STORE_FRANCHISE_ENTITY_NM,
        DDO_Extension.division as STORE_Division,
        DDO_Extension.dma as store_dma_cd,
        DDO_Extension.dma_name as store_dma_desc,
        DDO_Extension.FRANCHISE_CONSULTANT1_NAME,
        DDO_Extension.FRANCHISE_CONSULTANT2_NAME,
        DDO_Extension.FRANCHISE_CONSULTANT3_NAME,
        DDO_Extension.L1MANAGER as level1_manager_nm,
        DDO_Extension.L2MANAGER as level2_manager_nm,
        DDO_Extension.L3MANAGER as level3_manager_nm,
        DDO_Extension.L4MANAGER as level4_manager_nm,
        DDO_Extension.L5MANAGER as level5_manager_nm,
        DDO_Extension.DVP
    FROM
        cmx_audit_cte as cmx_audit
    LEFT JOIN REST_PROFILE as DDO_Extension ON
        cmx_audit.brand_id = DDO_Extension.brand_id
        AND try_to_number(cmx_audit.store_id) = DDO_Extension.store_id::INTEGER
    LEFT JOIN IDH_DEV.SHARED.DATE_DIM_V as dateDim ON
        cmx_audit.AUDIT_DT_KEY = dateDim.date_key
);
create view IF NOT EXISTS CONSOLIDATED_AUDIT(
	RPTG_AUDIT_KEY,
	RPTG_STORE_KEY,
	BRAND_ID,
	STORE_ID,
	AUDIT_ID,
	AUDIT_DT_KEY,
	SOURCE_SYSTEM_NM,
	AUDIT_CATEGORY_DESC,
	AUDIT_SCORE,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_NBR,
	RESTAURANT_TYPE,
	STORE_FRANCHISE_NM,
	STORE_FRANCHISE_ENTITY_NM,
	STORE_DIVISION,
	STORE_DMA_CD,
	STORE_DMA_DESC,
	FRANCHISE_CONSULTANT1_NAME,
	FRANCHISE_CONSULTANT2_NAME,
	FRANCHISE_CONSULTANT3_NAME,
	LEVEL1_MANAGER_NM,
	LEVEL2_MANAGER_NM,
	LEVEL3_MANAGER_NM,
	LEVEL4_MANAGER_NM,
	LEVEL5_MANAGER_NM,
	DVP
) as (
    SELECT
        RPTG_AUDIT_KEY,
        RPTG_STORE_KEY,
        BRAND_ID,
        STORE_ID,
        AUDIT_ID,
        AUDIT_DT_KEY,
        SOURCE_SYSTEM_NM,
        AUDIT_CATEGORY_DESC,
        AUDIT_SCORE,
        FISCAL_WEEK_NBR,
        FISCAL_YEAR_NBR,
        FISCAL_PERIOD_NBR,
        restaurant_type,
	    STORE_FRANCHISE_NM,
	    STORE_FRANCHISE_ENTITY_NM,
	    STORE_DIVISION,
	    STORE_DMA_CD,
	    STORE_DMA_DESC,
	    FRANCHISE_CONSULTANT1_NAME,
	    FRANCHISE_CONSULTANT2_NAME,
	    FRANCHISE_CONSULTANT3_NAME,
	    LEVEL1_MANAGER_NM,
	    LEVEL2_MANAGER_NM,
	    LEVEL3_MANAGER_NM,
	    LEVEL4_MANAGER_NM,
	    LEVEL5_MANAGER_NM,
	    DVP
    FROM
        CMX_AUDIT_DETAIL

    UNION ALL

    SELECT
        RPTG_AUDIT_KEY,
        RPTG_STORE_KEY,
        BRAND_ID,
        STORE_ID,
        AUDIT_ID :: string as AUDIT_ID,
        AUDIT_DT_KEY,
        SOURCE_SYSTEM_NM,
        AUDIT_CATEGORY_DESC,
        AUDIT_SCORE,
        FISCAL_WEEK_NBR,
        FISCAL_YEAR_NBR,
        FISCAL_PERIOD_NBR,
        restaurant_type,
	    STORE_FRANCHISE_NM,
	    STORE_FRANCHISE_ENTITY_NM,
	    STORE_DIVISION,
	    STORE_DMA_CD,
	    STORE_DMA_DESC,
	    FRANCHISE_CONSULTANT1_NAME,
	    FRANCHISE_CONSULTANT2_NAME,
	    FRANCHISE_CONSULTANT3_NAME,
	    LEVEL1_MANAGER_NM,
	    LEVEL2_MANAGER_NM,
	    LEVEL3_MANAGER_NM,
	    LEVEL4_MANAGER_NM,
	    LEVEL5_MANAGER_NM,
	    DVP
    FROM
        ECOSURE_AUDIT_DETAIL
);
create view IF NOT EXISTS DDO_CMX_AUDIT_DETAIL_VAPP(
	RPTG_AUDIT_KEY,
	RPTG_STORE_KEY,
	AUDIT_CATEGORY_DESC,
	CMX_ID,
	AUDIT_ID,
	STORE_ID,
	CMX_AUDIT_SERVICE_NM,
	CMX_AUDIT_SERVICE_VERSION_NBR,
	CMX_GROUP_NM,
	CMX_REPORT_TAG_NM,
	CMX_REPORT_TAG_POINT_QTY,
	CMX_REPORT_TAG_MAXIMUM_POINT_QTY,
	AUDIT_SCORE,
	AUDIT_DT_KEY,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_NBR,
	STORE_FRANCHISE_NM,
	STORE_FRANCHISE_ENTITY_NM,
	STORE_COMPANY_NM,
	STORE_DMA_CD,
	STORE_DMA_DESC,
	STORE_BUILDING_TYPE,
	STORE_REMODEL_PROGRAM_NM,
	STORE_REMODEL_DT_KEY,
	SUB_REGION_NM,
	STORE_AREA_SUPERVISOR_NM,
	STORE_DO_NM,
	STORE_VPO_NM,
	STORE_SVP_NM,
	FRANCHISE_TC_NM,
	FRANCHISE_OC_NM,
	FRANCHISE_DFO_NM,
	LEVEL1_NM,
	LEVEL2_NM,
	LEVEL3_NM,
	LEVEL4_NM,
	LEVEL5_NM,
	LEVEL1_MANAGER_NM,
	LEVEL2_MANAGER_NM,
	LEVEL3_MANAGER_NM,
	LEVEL4_MANAGER_NM,
	LEVEL5_MANAGER_NM
) as (
    WITH cmx_audit_cte AS (
        SELECT
            concat(brand_id, cmx_id, cmx_audit_id) as RPTG_AUDIT_KEY,
            concat(brand_id, lpad(store_id, 5, 0)) as RPTG_STORE_KEY,
            concat(cmx_group_nm, ':', cmx_report_tag_nm) as AUDIT_CATEGORY_DESC,
            cmx_id,
            cmx_audit_id as AUDIT_ID,
            lpad(store_id, 5, 0) as STORE_ID,
            cmx_audit_service_nm,
            cmx_audit_service_version_nbr,
            cmx_group_nm,
            cmx_report_tag_nm,
            cmx_report_tag_point_qty,
            cmx_report_tag_maximum_point_qty,
            cmx_report_tag_val as AUDIT_SCORE,
            to_char(audit_dt, 'YYYYMMDD') as AUDIT_DT_KEY,
            brand_id,
            source_system_nm
        FROM
            IDH_DEV.LOCN.CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV
    )
    SELECT
        cmx_audit.*,
        dateDim.fiscal_week_nbr,
        dateDim.fiscal_year_nbr,
        dateDim.fiscal_period_nbr,
        store.store_franchise_nm,
        store.store_franchise_entity_nm,
        store.store_company_nm,
        store.store_dma_cd,
        store.store_dma_desc,
        store.store_building_typ_nm as store_building_type,
        store.store_remodel_program_nm,
        store.store_remodel_dt_key,
        store.sub_region_nm,
        store.store_area_supervisor_nm,
        store.store_do_nm,
        store.store_vpo_nm,
        store.store_svp_nm,
        store.franchise_tc_nm,
        store.franchise_oc_nm,
        store.franchise_dfo_nm,
        loc.level1_nm,
        loc.level2_nm,
        loc.level3_nm,
        loc.level4_nm,
        loc.level5_nm,
        loc.level1_manager_nm,
        loc.level2_manager_nm,
        loc.level3_manager_nm,
        loc.l4_manager_nm as level4_manager_nm,
        loc.level5_manager_nm
    FROM
        cmx_audit_cte as cmx_audit
        LEFT JOIN IDH_DEV.LOCN.STORE_DETAIL_VAPP as store ON
            cmx_audit.brand_id = store.brand_id
            AND cmx_audit.store_id = store.store_id
            AND store.current_ind = True
        LEFT JOIN IDH_DEV.OPERATION.LOCATION_HIERARCHY_V as loc ON
            cmx_audit.brand_id = loc.brand_id
            AND cmx_audit.store_id = loc.location_id
        LEFT JOIN IDH_DEV.SHARED.DATE_DIM_V as dateDim ON
            cmx_audit.AUDIT_DT_KEY = dateDim.date_key
);
create view IF NOT EXISTS DDO_CONSOLIDATED_AUDIT_VAPP(
	RPTG_AUDIT_KEY,
	RPTG_STORE_KEY,
	BRAND_ID,
	STORE_ID,
	AUDIT_ID,
	AUDIT_DT_KEY,
	SOURCE_SYSTEM_NM,
	AUDIT_CATEGORY_DESC,
	AUDIT_SCORE,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_NBR,
	STORE_FRANCHISE_NM,
	STORE_FRANCHISE_ENTITY_NM,
	STORE_COMPANY_NM,
	STORE_DMA_CD,
	STORE_DMA_DESC,
	STORE_BUILDING_TYPE,
	STORE_REMODEL_PROGRAM_NM,
	STORE_REMODEL_DT_KEY,
	SUB_REGION_NM,
	STORE_AREA_SUPERVISOR_NM,
	STORE_VPO_NM,
	STORE_SVP_NM,
	FRANCHISE_TC_NM,
	FRANCHISE_OC_NM,
	FRANCHISE_DFO_NM,
	LEVEL1_NM,
	LEVEL2_NM,
	LEVEL3_NM,
	LEVEL4_NM,
	LEVEL5_NM,
	LEVEL1_MANAGER_NM,
	LEVEL2_MANAGER_NM,
	LEVEL3_MANAGER_NM,
	LEVEL4_MANAGER_NM,
	LEVEL5_MANAGER_NM
) as (
    SELECT
        RPTG_AUDIT_KEY,
        RPTG_STORE_KEY,
        BRAND_ID,
        STORE_ID,
        AUDIT_ID,
        AUDIT_DT_KEY,
        SOURCE_SYSTEM_NM,
        AUDIT_CATEGORY_DESC,
        AUDIT_SCORE,
        FISCAL_WEEK_NBR,
        FISCAL_YEAR_NBR,
        FISCAL_PERIOD_NBR,
        STORE_FRANCHISE_NM,
        STORE_FRANCHISE_ENTITY_NM,
        STORE_COMPANY_NM,
        STORE_DMA_CD,
        STORE_DMA_DESC,
        STORE_BUILDING_TYPE,
        STORE_REMODEL_PROGRAM_NM,
        STORE_REMODEL_DT_KEY,
        SUB_REGION_NM,
        STORE_AREA_SUPERVISOR_NM,
        STORE_DO_NM STORE_VPO_NM,
        STORE_SVP_NM,
        FRANCHISE_TC_NM,
        FRANCHISE_OC_NM,
        FRANCHISE_DFO_NM,
        LEVEL1_NM,
        LEVEL2_NM,
        LEVEL3_NM,
        LEVEL4_NM,
        LEVEL5_NM,
        LEVEL1_MANAGER_NM,
        LEVEL2_MANAGER_NM,
        LEVEL3_MANAGER_NM,
        LEVEL4_MANAGER_NM,
        LEVEL5_MANAGER_NM
    FROM
        DDO_CMX_AUDIT_DETAIL_VAPP

    UNION ALL

    SELECT
        RPTG_AUDIT_KEY,
        RPTG_STORE_KEY,
        BRAND_ID,
        STORE_ID,
        AUDIT_ID :: string as AUDIT_ID,
        AUDIT_DT_KEY,
        SOURCE_SYSTEM_NM,
        AUDIT_CATEGORY_DESC,
        AUDIT_SCORE,
        FISCAL_WEEK_NBR,
        FISCAL_YEAR_NBR,
        FISCAL_PERIOD_NBR,
        STORE_FRANCHISE_NM,
        STORE_FRANCHISE_ENTITY_NM,
        STORE_COMPANY_NM,
        STORE_DMA_CD,
        STORE_DMA_DESC,
        STORE_BUILDING_TYPE,
        STORE_REMODEL_PROGRAM_NM,
        STORE_REMODEL_DT_KEY,
        SUB_REGION_NM,
        STORE_AREA_SUPERVISOR_NM,
        STORE_DO_NM STORE_VPO_NM,
        STORE_SVP_NM,
        FRANCHISE_TC_NM,
        FRANCHISE_OC_NM,
        FRANCHISE_DFO_NM,
        LEVEL1_NM,
        LEVEL2_NM,
        LEVEL3_NM,
        LEVEL4_NM,
        LEVEL5_NM,
        LEVEL1_MANAGER_NM,
        LEVEL2_MANAGER_NM,
        LEVEL3_MANAGER_NM,
        LEVEL4_MANAGER_NM,
        LEVEL5_MANAGER_NM
    FROM
        DDO_ECOSURE_AUDIT_DETAIL_VAPP
);
create view IF NOT EXISTS DDO_ECOSURE_AUDIT_DETAIL_VAPP(
	RPTG_AUDIT_KEY,
	RPTG_STORE_KEY,
	AUDIT_CATEGORY_DESC,
	AUDIT_ID,
	AUDIT_SCORE,
	AUDIT_DT_KEY,
	STORE_ID,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_NBR,
	STORE_FRANCHISE_NM,
	STORE_FRANCHISE_ENTITY_NM,
	STORE_COMPANY_NM,
	STORE_DMA_CD,
	STORE_DMA_DESC,
	STORE_BUILDING_TYPE,
	STORE_REMODEL_PROGRAM_NM,
	STORE_REMODEL_DT_KEY,
	SUB_REGION_NM,
	STORE_AREA_SUPERVISOR_NM,
	STORE_DO_NM,
	STORE_VPO_NM,
	STORE_SVP_NM,
	FRANCHISE_TC_NM,
	FRANCHISE_OC_NM,
	FRANCHISE_DFO_NM,
	LEVEL1_NM,
	LEVEL2_NM,
	LEVEL3_NM,
	LEVEL4_NM,
	LEVEL5_NM,
	LEVEL1_MANAGER_NM,
	LEVEL2_MANAGER_NM,
	LEVEL3_MANAGER_NM,
	LEVEL4_MANAGER_NM,
	LEVEL5_MANAGER_NM
) as (
    WITH ecosure_audit_cte AS (
        SELECT
            ecosure_rest_audit_score.brand_id || ecosure_rest_audit_score.ecosure_audit_id || ecosure_rest_audit_score.ecosure_score_nm as RPTG_AUDIT_KEY,
            ecosure_rest_audit_score.brand_id || lpad(ecosure_rest_audit.store_id, 5, 0) as RPTG_STORE_KEY,
            ecosure_rest_audit_score.ecosure_score_nm as AUDIT_CATEGORY_DESC,
            ecosure_rest_audit_score.ecosure_audit_id as AUDIT_ID,
            ecosure_rest_audit_score.ecosure_score_amt as AUDIT_SCORE,
            TO_VARCHAR(ecosure_rest_audit.audit_dt, 'YYYYMMDD') as AUDIT_DT_KEY,
            LPAD(ecosure_rest_audit.store_id, 5, 0) as STORE_ID,
            ecosure_rest_audit.brand_id as BRAND_ID,
            ecosure_rest_audit.source_system_nm as SOURCE_SYSTEM_NM
        FROM
            IDH_DEV.LOCN.ECOSURE_RESTAURANT_AUDIT_SCORE_BV ecosure_rest_audit_score
            JOIN IDH_DEV.LOCN.ECOSURE_RESTAURANT_AUDIT_BV ecosure_rest_audit ON ecosure_rest_audit_score.ecosure_audit_id = ecosure_rest_audit.ecosure_audit_id
    )
    SELECT
        ecosure_audit.*,
        date_dim.fiscal_week_nbr,
        date_dim.fiscal_year_nbr,
        date_dim.fiscal_period_nbr,
        store_detail.store_franchise_nm,
        store_detail.store_franchise_entity_nm,
        store_detail.store_company_nm,
        store_detail.store_dma_cd,
        store_detail.store_dma_desc,
        store_detail.store_building_typ_nm as store_building_type,
        store_detail.store_remodel_program_nm,
        store_detail.store_remodel_dt_key,
        store_detail.sub_region_nm,
        store_detail.store_area_supervisor_nm,
        store_detail.store_do_nm,
        store_detail.store_vpo_nm,
        store_detail.store_svp_nm,
        store_detail.franchise_tc_nm,
        store_detail.franchise_oc_nm,
        store_detail.franchise_dfo_nm,
        loc_hier.level1_nm,
        loc_hier.level2_nm,
        loc_hier.level3_nm,
        loc_hier.level4_nm,
        loc_hier.level5_nm,
        loc_hier.level1_manager_nm,
        loc_hier.level2_manager_nm,
        loc_hier.level3_manager_nm,
        loc_hier.l4_manager_nm as level4_manager_nm,
        loc_hier.level5_manager_nm
    FROM
        ecosure_audit_cte AS ecosure_audit
        LEFT JOIN IDH_DEV.LOCN.STORE_DETAIL_VAPP store_detail ON store_detail.brand_id = ecosure_audit.brand_id
        AND store_detail.store_id = ecosure_audit.STORE_ID
        AND store_detail.current_ind = True
        LEFT JOIN IDH_DEV.OPERATION.LOCATION_HIERARCHY_V loc_hier ON ecosure_audit.brand_id = loc_hier.brand_id
        AND lpad(loc_hier.location_id, 5, 0) = ecosure_audit.STORE_ID
        LEFT JOIN IDH_DEV.SHARED.DATE_DIM_V date_dim ON ecosure_audit.AUDIT_DT_KEY = date_dim.date_key
);
create view IF NOT EXISTS ECOSURE_AUDIT_DETAIL(
	RPTG_AUDIT_KEY,
	RPTG_STORE_KEY,
	AUDIT_CATEGORY_DESC,
	AUDIT_ID,
	AUDIT_SCORE,
	AUDIT_DT_KEY,
	STORE_ID,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_NBR,
	RESTAURANT_TYPE,
	STORE_FRANCHISE_NM,
	STORE_FRANCHISE_ENTITY_NM,
	STORE_DIVISION,
	STORE_DMA_CD,
	STORE_DMA_DESC,
	FRANCHISE_CONSULTANT1_NAME,
	FRANCHISE_CONSULTANT2_NAME,
	FRANCHISE_CONSULTANT3_NAME,
	LEVEL1_MANAGER_NM,
	LEVEL2_MANAGER_NM,
	LEVEL3_MANAGER_NM,
	LEVEL4_MANAGER_NM,
	LEVEL5_MANAGER_NM,
	DVP
) as (
    WITH ecosure_audit_cte AS (
        SELECT
            ecosure_rest_audit_score.brand_id || ecosure_rest_audit_score.ecosure_audit_id || ecosure_rest_audit_score.ecosure_score_nm as RPTG_AUDIT_KEY,
            ecosure_rest_audit_score.brand_id || lpad(ecosure_rest_audit.store_id, 5, 0) as RPTG_STORE_KEY,
            ecosure_rest_audit_score.ecosure_score_nm as AUDIT_CATEGORY_DESC,
            ecosure_rest_audit_score.ecosure_audit_id as AUDIT_ID,
            ecosure_rest_audit_score.ecosure_score_amt as AUDIT_SCORE,
            TO_VARCHAR(ecosure_rest_audit.audit_dt, 'YYYYMMDD') as AUDIT_DT_KEY,
            LPAD(ecosure_rest_audit.store_id, 5, 0) as STORE_ID,
            ecosure_rest_audit.brand_id as BRAND_ID,
            ecosure_rest_audit.source_system_nm as SOURCE_SYSTEM_NM
        FROM
            IDH_DEV.LOCN.ECOSURE_RESTAURANT_AUDIT_SCORE_BV ecosure_rest_audit_score
            JOIN IDH_DEV.LOCN.ECOSURE_RESTAURANT_AUDIT_BV ecosure_rest_audit ON ecosure_rest_audit_score.ecosure_audit_id = ecosure_rest_audit.ecosure_audit_id
    )
    SELECT
        ecosure_audit.*,
        date_dim.fiscal_week_nbr,
        date_dim.fiscal_year_nbr,
        date_dim.fiscal_period_nbr,
        DDO_EXTENSION.restaurant_type as restaurant_type,
        DDO_Extension.franchisee_name as STORE_FRANCHISE_NM,
        DDO_Extension.legal_name as STORE_FRANCHISE_ENTITY_NM,
        DDO_Extension.division as STORE_Division,
        DDO_Extension.dma as store_dma_cd,
        DDO_Extension.dma_name as store_dma_desc,
        DDO_Extension.FRANCHISE_CONSULTANT1_NAME,
        DDO_Extension.FRANCHISE_CONSULTANT2_NAME,
        DDO_Extension.FRANCHISE_CONSULTANT3_NAME,
        DDO_Extension.L1MANAGER as level1_manager_nm,
        DDO_Extension.L2MANAGER as level2_manager_nm,
        DDO_Extension.L3MANAGER as level3_manager_nm,
        DDO_Extension.L4MANAGER as level4_manager_nm,
        DDO_Extension.L5MANAGER as level5_manager_nm,
        DDO_Extension.DVP 
    FROM
        ecosure_audit_cte AS ecosure_audit
        LEFT JOIN REST_PROFILE as DDO_Extension
            ON DDO_Extension.brand_id = ecosure_audit.brand_id
            AND DDO_Extension.store_id::INTEGER = try_to_number(ecosure_audit.STORE_ID)
        LEFT JOIN IDH_DEV.SHARED.DATE_DIM_V date_dim 
            ON ecosure_audit.AUDIT_DT_KEY = date_dim.date_key
);
create view IF NOT EXISTS MARC_CMX_TEST(
	RPTG_KEY,
	STORE_KEY,
	AUDIT_DT,
	BRAND_ID,
	STORE_ID,
	SOURCE_SYSTEM_NM,
	AUDIT_CATEGORY_DESC,
	CMX_ID,
	CMX_AUDIT_ID,
	CMX_AUDIT_SERVICE_NM,
	CMX_AUDIT_SERVICE_VERSION_NBR,
	GROUP_NAME,
	REPORT_NAME,
	POINTS,
	MAX_POINTS,
	SCORE,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_START_DT,
	FISCAL_PERIOD_END_DT,
	FISCAL_PERIOD_NBR
) as (
select cmx.brand_id || cmx.cmx_id || cmx.cmx_audit_id as "rptg_key"
    ,cmx.brand_id || lpad(cmx.store_id, 5, 0) as "store_key"
    ,to_char(cmx.audit_dt, 'YYYYMMDD') as audit_dt
    ,cmx.brand_id
    ,lpad(cmx.store_id, 5, 0) as "store_id" --ADD TO CMX
    ,cmx.source_system_nm
    ,cmx.cmx_group_nm || ':' || cmx.cmx_report_tag_nm as "audit_category_desc"
    ,cmx.cmx_id
    ,cmx.cmx_audit_id
    ,cmx.cmx_audit_service_nm
    ,cmx.cmx_audit_service_version_nbr
    ,cmx.cmx_group_nm as "group_name"
    ,cmx.cmx_report_tag_nm as "report_name"
    ,cmx.cmx_report_tag_point_qty as "points"
    ,cmx.cmx_report_tag_maximum_point_qty as "max_points"
    ,cmx.cmx_report_tag_val as "score"
    ,dateDim.fiscal_week_nbr
    ,dateDim.fiscal_year_nbr
    ,dateDim.FISCAL_PERIOD_START_DT
    ,dateDim.FISCAL_PERIOD_END_DT
    ,dateDim.FISCAL_PERIOD_NBR
from ids_dev.locn_bv.cmx_restaurant_audit_report_tag_agg_bv cmx
join idh_dev.shared.date_dim_v dateDim
    on to_char(cmx.audit_dt, 'YYYYMMDD') = dateDim.date_key);
create view IF NOT EXISTS MARC_CONSOLIDATED_VIEW(
	RPTG_KEY,
	STORE_KEY,
	AUDIT_DT,
	BRAND_ID,
	STORE_ID,
	SOURCE_SYSTEM_NM,
	AUDIT_CATEGORY_DESC,
	SCORE,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_START_DT,
	FISCAL_PERIOD_END_DT,
	FISCAL_PERIOD_NBR
) as (
select esv.rptg_key
,esv.store_key
,esv.audit_dt
,esv.brand_id
,esv.store_id
,esv.source_system_nm
,esv.audit_category_desc
,esv.score
,esv.fiscal_week_nbr
,esv.fiscal_year_nbr
,esv.FISCAL_PERIOD_START_DT
,esv.FISCAL_PERIOD_END_DT
,esv.FISCAL_PERIOD_NBR
--,esv. [location stuff - not added to query yet]
from marc_ecosure_test esv
union all
select cmx.rptg_key
,cmx.store_key
,cmx.audit_dt
,cmx.brand_id
,cmx.store_id
,cmx.source_system_nm
,cmx.audit_category_desc
,cmx.score
,cmx.fiscal_week_nbr
,cmx.fiscal_year_nbr
,cmx.FISCAL_PERIOD_START_DT
,cmx.FISCAL_PERIOD_END_DT
,cmx.FISCAL_PERIOD_NBR
--,esv. [location stuff - not added to query yet]
from marc_cmx_test cmx
);
create view IF NOT EXISTS MARC_ECOSURE_TEST(
	RPTG_KEY,
	STORE_KEY,
	AUDIT_DT,
	BRAND_ID,
	STORE_ID,
	SOURCE_SYSTEM_NM,
	AUDIT_CATEGORY_DESC,
	ECOSURE_AUDIT_ID,
	SCORE,
	FISCAL_WEEK_NBR,
	FISCAL_YEAR_NBR,
	FISCAL_PERIOD_START_DT,
	FISCAL_PERIOD_END_DT,
	FISCAL_PERIOD_NBR
) as (
select es_audit.brand_id || es_audit.ecosure_audit_id || es_score.ecosure_score_nm as "rptg_key"
    ,es_audit.brand_id || es_audit.store_id as "store_key"
    ,to_char(es_audit.audit_dt, 'YYYYMMDD') as audit_dt
    ,es_audit.brand_id
    ,es_audit.store_id as "store_id"
    ,es_score.source_system_nm
    ,es_score.ecosure_score_nm as "audit_category_desc"
    ,es_score.ecosure_audit_id
    ,es_score.ecosure_score_amt as "score"
    ,dateDim.fiscal_week_nbr
    ,dateDim.fiscal_year_nbr
    ,dateDim.FISCAL_PERIOD_START_DT
    ,dateDim.FISCAL_PERIOD_END_DT
    ,dateDim.FISCAL_PERIOD_NBR
from IDS_dev.LOCN_BV.ECOSURE_RESTAURANT_AUDIT_SCORE_BV es_score
join IDS_dev.LOCN_BV.ECOSURE_RESTAURANT_AUDIT_BV es_audit
    on es_score.ecosure_audit_id = es_audit.ecosure_audit_id
    and es_score.brand_id = es_audit.brand_id
join idh_dev.shared.date_dim_v dateDim
    on dateDim.date_key = to_char(es_audit.AUDIT_DT, 'YYYYMMDD')
 join IDH_DEV.OPERATION.LOCATION_HIERARCHY_V loc
  on loc.brand_id||store_id = es_audit.brand_id||es_audit.store_id);
create view IF NOT EXISTS PERIOD_FLASH_COMPARABLE_SALES(
	BRAND_ID,
	FISC_YR_NBR,
	FISC_QTR_NBR,
	FISC_PERIOD_NBR,
	WEEKS_IN_PERIOD,
	STORE_ID,
	STORE_STATUS_TYPE,
	CURR_OWNER,
	SALES_TIME_OWNER,
	ORDER_CHANNEL_TYPE,
	NET_SALES,
	COMP_SALES_THIS_YR,
	COMP_SALES_LAST_YR_CURR_YR,
	COMP_SALES_THIS_YR_PREV_YR,
	COMP_SALES_LAST_YR_PREV_YR,
	COMP_SALES_THIS_YR_PREV_2_YR,
	COMP_SALES_LAST_YR_PREV_2_YR,
	COMP_TRANS_THIS_YR,
	COMP_TRANS_LAST_YR_CURR_YR,
	COMP_TRANS_THIS_YR_PREV_YR,
	COMP_TRANS_LAST_YR_PREV_YR,
	COMP_TRANS_THIS_YR_PREV_2_YR,
	COMP_TRANS_LAST_YR_PREV_2_YR
) as
SELECT
    SALES.BRAND_ID,
    SALES.FISC_YR_NBR,
    SALES.FISC_QTR_NBR,
    SALES.FISC_PERIOD_NBR,
    SALES.WEEKS_IN_PERIOD,
    SALES.STORE_ID as STORE_ID,
    re.STORE_STATUS_TYP as STORE_STATUS_TYPE,
    re.FRANCHISEE_NM as CURR_OWNER,
    SALES.SALES_TIME_OWNER,
    SALES.ORDER_CHANNEL_TYPE,
    SALES.NET_SALES,
    SALES.COMP_SALES_THIS_YR,
    SALES.COMP_SALES_LAST_YR_CURR_YR,
    SALES.COMP_SALES_THIS_YR_PREV_YR,
    SALES.COMP_SALES_LAST_YR_PREV_YR,
    SALES.COMP_SALES_THIS_YR_PREV_2_YR,
    SALES.COMP_SALES_LAST_YR_PREV_2_YR,
    SALES.COMP_TRANS_THIS_YR,
    SALES.COMP_TRANS_LAST_YR_CURR_YR,
    SALES.COMP_TRANS_THIS_YR_PREV_YR,
    SALES.COMP_TRANS_LAST_YR_PREV_YR,
    SALES.COMP_TRANS_THIS_YR_PREV_2_YR,
    SALES.COMP_TRANS_LAST_YR_PREV_2_YR

FROM IDS_DEV.TXN.PERIOD_FLASH_COMPARABLE_SALES SALES
left join IDM_DEV.COREDIM.RESTAURANT_SCD_DIM re
    on SALES.brand_id = re.brand_id
    and SALES.store_id = re.store_id
    and re.current_ind = TRUE;
create view IF NOT EXISTS REST_PROFILE(
	BRAND_ID,
	STORE_ID,
	RESTAURANT_TYPE,
	RESTAURANT_STATUS,
	FRANCHISEE_NUMBER,
	FRANCHISEE_NAME,
	LEGAL_NAME,
	DIVISION,
	DMA,
	DMA_NAME,
	FRANCHISE_CONSULTANT1_NAME,
	FRANCHISE_CONSULTANT2_NAME,
	FRANCHISE_CONSULTANT3_NAME,
	L1MANAGER,
	L2MANAGER,
	L3MANAGER,
	L4MANAGER,
	L5MANAGER,
	DVP
) as (
WITH RESTAURANT_CTE AS (
    SELECT
        STORE_ID,
        BRAND_ID,
        OWNERSHIP_TYP AS OWNERSHIP_TYPE,
        DMA_CD AS DMA_CODE,
        DMA_SOURCE_NM AS DMA_SOURCE_NAME,
        LEGAL_NM AS LEGAL_NAME,
        FRANCHISEE_NM AS FRANCHISEE_NAME,
        DIVISION_NM AS DIVISION_NAME,
        BUILDING_TYP AS BUILDING_TYPE,
        REMODEL_PROGRAM_TYP AS REMODEL_PROGRAM,
        REOPEN_DT AS REOPEN_DATE,
        STORE_STATUS_TYP,
        OWNER_ID,
        DMA_NM AS DMA_NAME
    FROM IDM_DEV.COREDIM.RESTAURANT_SCD_DIM
    WHERE CURRENT_IND = TRUE
),
HIERARCHY_CTE AS (
    SELECT
        LOCATION_ID AS LOCATIONID,
        BRAND_ID AS BRANDID,
        LEVEL1_NM AS L1NAME,
        LEVEL1_MANAGER_NM AS L1MANAGER,
        LEVEL2_NM AS L2NAME,
        LEVEL2_MANAGER_NM AS L2MANAGER,
        LEVEL3_NM AS L3NAME,
        LEVEL3_MANAGER_NM AS L3MANAGER,
        LEVEL4_NM AS L4NAME,
        LEVEL4_MANAGER_NM AS L4MANAGER,
        LEVEL5_NM AS L5NAME,
        LEVEL5_MANAGER_NM AS L5MANAGER,
        FRANCHISE_CONSULTANT1_NM AS FRANCHISE_CONSULTANT1_NAME,
        FRANCHISE_CONSULTANT2_NM AS FRANCHISE_CONSULTANT2_NAME,
        FRANCHISE_CONSULTANT3_NM AS FRANCHISE_CONSULTANT3_NAME
    FROM IDM_DEV.COREDIM.OPERATION_MANAGEMENT_HIERARCHY
)
select
    RESTAURANT.BRAND_ID,
    RESTAURANT.STORE_ID,
    RESTAURANT.OWNERSHIP_TYPE as Restaurant_Type,
    RESTAURANT.STORE_STATUS_TYP as Restaurant_Status,
    RESTAURANT.OWNER_ID as Franchisee_Number,
    RESTAURANT.FRANCHISEE_NAME as Franchisee_Name,
    RESTAURANT.LEGAL_NAME as Legal_Name,
    UPPER(IFF(RESTAURANT.OWNERSHIP_TYPE='Company Owned', HIERARCHY.l3Name, RESTAURANT.DIVISION_NAME)) as Division,
    RESTAURANT.DMA_CODE as DMA,
    RESTAURANT.DMA_NAME as DMA_NAME,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Franchised', HIERARCHY.FRANCHISE_CONSULTANT1_NAME, NULL) AS FRANCHISE_CONSULTANT1_NAME,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Franchised', HIERARCHY.FRANCHISE_CONSULTANT2_NAME, NULL) AS FRANCHISE_CONSULTANT2_NAME,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Franchised', HIERARCHY.FRANCHISE_CONSULTANT3_NAME, NULL) AS FRANCHISE_CONSULTANT3_NAME,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Company Owned', HIERARCHY.L5MANAGER, NULL) as L1Manager,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Company Owned', HIERARCHY.L4Manager, NULL) as L2Manager,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Company Owned', HIERARCHY.L3Manager, NULL) as L3Manager,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Company Owned', HIERARCHY.L2Manager, NULL) as L4Manager,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Company Owned', HIERARCHY.L1Manager, NULL) as L5Manager,
    IFF(RESTAURANT.OWNERSHIP_TYPE='Franchised', HIERARCHY.FRANCHISE_CONSULTANT3_NAME, HIERARCHY.L3Manager) as DVP
  from RESTAURANT_CTE RESTAURANT
  left join HIERARCHY_CTE HIERARCHY
  on RESTAURANT.brand_id = HIERARCHY.brandid and RESTAURANT.Store_Id::INTEGER = HIERARCHY.locationid::INTEGER);