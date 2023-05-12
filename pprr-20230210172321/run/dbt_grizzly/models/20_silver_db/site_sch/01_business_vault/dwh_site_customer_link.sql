
  
    

        create or replace transient table PRFR_00_DEV_20_SILVER_DB.PRFR_DWH_SITE_SCH.dwh_site_customer_link  as
        (

    -- Generated by dbtvault.

    

WITH  __dbt__cte__dwh_site_tmp as (
WITH
    
F0006 AS (
  SELECT 
    F0006_HUB."ID", "MCMCU",
    F0006_S01."FD", "TD", "MCSTYL", "MCDC", "MCLDM", "MCCO", "MCAN8", "MCAN8O", "MCCNTY", "MCADDS", "MCFMOD", "MCDL01", "MCDL02", "MCDL03", "MCDL04", "MCRP01", "MCRP02", "MCRP03", "MCRP04", "MCRP05", "MCRP06", "MCRP07", "MCRP08", "MCRP09", "MCRP10", "MCRP11", "MCRP12", "MCRP13", "MCRP14", "MCRP15", "MCRP16", "MCRP17", "MCRP18", "MCRP19", "MCRP20", "MCRP21", "MCRP22", "MCRP23", "MCRP24", "MCRP25", "MCRP26", "MCRP27", "MCRP28", "MCRP29", "MCRP30", "MCTA", "MCTXJS", "MCTXA1", "MCEXR1", "MCTC01", "MCTC02", "MCTC03", "MCTC04", "MCTC05", "MCTC06", "MCTC07", "MCTC08", "MCTC09", "MCTC10", "MCND01", "MCND02", "MCND03", "MCND04", "MCND05", "MCND06", "MCND07", "MCND08", "MCND09", "MCND10", "MCCC01", "MCCC02", "MCCC03", "MCCC04", "MCCC05", "MCCC06", "MCCC07", "MCCC08", "MCCC09", "MCCC10", "MCPECC", "MCALS", "MCISS", "MCGLBA", "MCALCL", "MCLMTH", "MCLF", "MCOBJ1", "MCOBJ2", "MCOBJ3", "MCSUB1", "MCTOU", "MCSBLI", "MCANPA", "MCCT", "MCCERT", "MCMCUS", "MCBTYP", "MCPC", "MCPCA", "MCPCC", "MCINTA", "MCINTL", "MCD1J", "MCD2J", "MCD3J", "MCD4J", "MCD5J", "MCD6J", "MCFPDJ", "MCCAC", "MCPAC", "MCEEO", "MCERC", "MCUSER", "MCPID", "MCUPMJ", "MCJOBN", "MCUPMT", "MCBPTP", "MCAPSB", "MCTSBU", "MCRP31", "MCRP32", "MCRP33", "MCRP34", "MCRP35", "MCRP36", "MCRP37", "MCRP38", "MCRP39", "MCRP40", "MCRP41", "MCRP42", "MCRP43", "MCRP44", "MCRP45", "MCRP46", "MCRP47", "MCRP48", "MCRP49", "MCRP50", "MCAN8GCA1", "MCAN8GCA2", "MCAN8GCA3", "MCAN8GCA4", "MCAN8GCA5", "MCRMCU1", "MCDOCO", "MCPCTN", "MCCLNU", "MCBUCA", "MCADJENT", "MCUAFL"
  FROM 
    PRFR_00_DEV_10_BRONZE_DB.PRFR_RAW_JDE_LOGIS_SCH.TDWH_JDE_F0006_HUB F0006_HUB 
    INNER JOIN PRFR_00_DEV_10_BRONZE_DB.PRFR_RAW_JDE_LOGIS_SCH.TDWH_JDE_F0006_S01 F0006_S01 ON F0006_HUB.ID = F0006_S01.ID
    where F0006_HUB.ID <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY F0006_S01.ID ORDER BY F0006_S01.FD DESC) = 1 
),
    
F0005 AS (
  SELECT 
    F0005_HUB."ID", "DRSY", "DRRT", "DRKY",
    F0005_S01."FD", "TD", "DRDL01", "DRDL02", "DRSPHD", "DRUDCO", "DRHRDC", "DRUSER", "DRPID", "DRUPMJ", "DRJOBN", "DRUPMT"
  FROM 
    PRFR_00_DEV_10_BRONZE_DB.PRFR_RAW_JDE_LOGIS_SCH.TDWH_JDE_F0005_HUB F0005_HUB 
    INNER JOIN PRFR_00_DEV_10_BRONZE_DB.PRFR_RAW_JDE_LOGIS_SCH.TDWH_JDE_F0005_S01 F0005_S01 ON F0005_HUB.ID = F0005_S01.ID
    where F0005_HUB.ID <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY F0005_S01.ID ORDER BY F0005_S01.FD DESC) = 1 
),
    
F0101 AS (
  SELECT 
    F0101_HUB."ID", "ABAN8",
    F0101_S01."FD", "TD", "ABALKY", "ABTAX", "ABALPH", "ABDC", "ABMCU", "ABSIC", "ABLNGP", "ABAT1", "ABCM", "ABTAXC", "ABAT2", "ABAT3", "ABAT4", "ABAT5", "ABATP", "ABATR", "ABATPR", "ABAB3", "ABATE", "ABSBLI", "ABEFTB", "ABAN81", "ABAN82", "ABAN83", "ABAN84", "ABAN86", "ABAN85", "ABAC01", "ABAC02", "ABAC03", "ABAC04", "ABAC05", "ABAC06", "ABAC07", "ABAC08", "ABAC09", "ABAC10", "ABAC11", "ABAC12", "ABAC13", "ABAC14", "ABAC15", "ABAC16", "ABAC17", "ABAC18", "ABAC19", "ABAC20", "ABAC21", "ABAC22", "ABAC23", "ABAC24", "ABAC25", "ABAC26", "ABAC27", "ABAC28", "ABAC29", "ABAC30", "ABGLBA", "ABPTI", "ABPDI", "ABMSGA", "ABRMK", "ABTXCT", "ABTX2", "ABALP1", "ABURCD", "ABURDT", "ABURAT", "ABURAB", "ABURRF", "ABUSER", "ABPID", "ABUPMJ", "ABJOBN", "ABUPMT", "ABPRGF", "ABSCCLTP", "ABTICKER", "ABEXCHG", "ABDUNS", "ABCLASS01", "ABCLASS02", "ABCLASS03", "ABCLASS04", "ABCLASS05", "ABNOE", "ABGROWTHR", "ABYEARSTAR", "ABAEMPGP", "ABACTIN", "ABREVRNG", "ABSYNCS", "ABPERRS", "ABCAAD"
  FROM 
    PRFR_00_DEV_10_BRONZE_DB.PRFR_RAW_JDE_LOGIS_SCH.TDWH_JDE_F0101_HUB F0101_HUB 
    INNER JOIN PRFR_00_DEV_10_BRONZE_DB.PRFR_RAW_JDE_LOGIS_SCH.TDWH_JDE_F0101_S01 F0101_S01 ON F0101_HUB.ID = F0101_S01.ID
    where F0101_HUB.ID <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY F0101_S01.ID ORDER BY F0101_S01.FD DESC) = 1 
),

site_type as (
    --select aban8, abat1,abalph,mcrp05,mcrp07,mcmcu from
    select TRIM(F0006.MCMCU) as MCMCU,
    CASE WHEN TRIM(F0006.MCMCU) = TRIM(F0006.MCRP05) THEN 'DER'
         WHEN F0006.MCRP07 = 'SU' THEN 'SU'
         WHEN F0006.MCRP07 = 'A' THEN 'ACH'
         WHEN F0006.MCRP07 = 'ST' THEN 'ST'
         WHEN F0101.ABAT1 = 'ST' THEN 'ST'
         WHEN F0101.ABAT1 = 'KT' THEN 'DISP'
         WHEN F0101.ABAT1 = 'D' THEN 'DL'
         WHEN F0101.ABAT1 = 'AD' THEN 'DF'
    END as SITE_TYPE_CD --Code du type de site
    from F0101
    inner join F0006 on F0101.ABAN8 = F0006.MCAN8
    where F0101.abat1 in ('D','AD','ST','KT')
),

source_data as (
  select
    sha2_binary(to_variant(array_construct(TRIM(F0006.MCMCU))),512) as SITE_KEY, --cle technique
    sha2_binary(to_variant(array_construct(F0006.MCCO)),512) as COMPANY_KEY, --Clé hashée des sociétés
    sha2_binary(to_variant(array_construct(F0006.MCAN8)),512) AS CUSTOMER_KEY, --Clé hashée des utilisateurs
    sha2_binary(to_variant(array_construct(SITE_KEY,COMPANY_KEY)),512) as SITE_COMPANY_LINK_KEY, --Clé hashée des sociétés
    sha2_binary(to_variant(array_construct(SITE_KEY,CUSTOMER_KEY)),512) AS SITE_CUSTOMER_LINK_KEY, --Clé hashée des utilisateurs
    TRIM(F0006.MCMCU)  as SITE_CD, --Magasin - Code
    F0006.MCDL01 as SITE_NAME, --Libellé du site
    IFF(AD_P_PARAM.PARAM_CLASS_CD = 'LSV_TYPE_SITE', F0101_TIER.ABAT1 , NULL) as TYP_REF, --Typologie du Referentiel tracabilité
    site_type.SITE_TYPE_CD as SITE_TYPE_CD, --Code du type de site*/
    AD_P_PARAM.OBJ_NAME as SITE_TYPE_NAME, --Libellé du type de site
    AD_P_PARAM.OBJ_CPLT_1 as SITE_PRF_CD,  --Code du site PRF
    AD_P_PARAM.OBJ_CPLT_2 as SITE_PRF_NAME,  --Libellé du site PRF
    IFF(SITE_TYPE.SITE_TYPE_CD = 'DER', 1 , 0) as DER_FLG, --Flag indiquant que nous sommes sur une DER
    F0006.MCCO as COMPANY_ID,
    --company_hub.COMPANY_CD as COMPANY_ID, --Identifiant technique de la société
    --case when TRIM(F0101_0.ABAT1) in ('D','AD') then F0006.MCAN8 else NULL end  as PARTY_ID, --Identifiant technique du tiers
    F0006.MCRP05 as SITE_GUARDSHIP_CD, --Code du site de rattachement (DER)
    F0005_SITE_GUARDSHIP_NAME.DRDL01 as SITE_GUARDSHIP_NAME, --Libellé du site de rattachement (DER)
    F0005_CAPACITY_PROPRE_NBR_PL.DRDL01 as CAPACITY_PROPRE_NBR_PL, --Capacité en nombre de palette pour les produits propres
    F0005_CAPACITY_NEGOCE_NBR_PL.DRDL01 as CAPACITY_NEGOCE_NBR_PL, --Capacité en nombre de palette pour les produits de negoce
    F0005_CAPACITY_NBR_PL.DRDL01 as CAPACITY_NBR_PL, --Capacité en nombre de palette
    F0006.FD as EFFECTIVE_FROM, -- Date effective  
    '2023-04-12 12:57:07'::DATETIME as LDTS, -- Date de chargement
    'JDE' as RSCR -- Donnees JDE
  from F0006

  inner join SITE_TYPE on TRIM(SITE_TYPE.MCMCU) = TRIM(F0006.MCMCU)
  LEFT JOIN PRFR_00_DEV_20_SILVER_DB.PRFR_PARAMETERS_SCH.AD_P_PARAM AD_P_PARAM ON TRIM(AD_P_PARAM.OBJ_CD) = TRIM(SITE_TYPE.SITE_TYPE_CD) AND PARAM_CLASS_CD = 'LSV_TYPE_SITE'

  LEFT JOIN (select distinct ABMCU from F0101 where ABAT1 in ('ST','KT','D','AD')) F0101 ON TRIM(F0101.ABMCU)=TRIM(F0006.MCMCU)
  LEFT JOIN F0101 F0101_TIER ON F0101_TIER.ABAN8 = TRIM(F0006.MCAN8)
  LEFT JOIN F0005 F0005_SITE_GUARDSHIP_NAME ON TRIM(F0005_SITE_GUARDSHIP_NAME.DRKY) = F0006.MCRP05 AND RTRIM(F0005_SITE_GUARDSHIP_NAME.DRSY) = '00' AND RTRIM(F0005_SITE_GUARDSHIP_NAME.DRRT) = '05' --Récupération du libellé dans la table TDWH_JDE_F0005_HUB
  LEFT JOIN F0005 F0005_CAPACITY_PROPRE_NBR_PL ON TRIM(F0005_CAPACITY_PROPRE_NBR_PL.DRKY) = F0006.MCRP02 AND RTRIM(F0005_CAPACITY_PROPRE_NBR_PL.DRSY) = '00' AND RTRIM(F0005_CAPACITY_PROPRE_NBR_PL.DRRT) = '02' --Récupération du libellé dans la table TDWH_JDE_F0005_HUB
  LEFT JOIN F0005 F0005_CAPACITY_NEGOCE_NBR_PL ON TRIM(F0005_CAPACITY_NEGOCE_NBR_PL.DRKY) = F0006.MCRP03 AND RTRIM(F0005_CAPACITY_NEGOCE_NBR_PL.DRSY) = '00' AND RTRIM(F0005_CAPACITY_NEGOCE_NBR_PL.DRRT) = '03' --Récupération du libellé dans la table TDWH_JDE_F0005_HUB
  LEFT JOIN F0005 F0005_CAPACITY_NBR_PL ON TRIM(F0005_CAPACITY_NBR_PL.DRKY) = F0006.MCRP04 AND RTRIM(F0005_CAPACITY_NBR_PL.DRSY) = '00' AND RTRIM(F0005_CAPACITY_NBR_PL.DRRT) = '04' --Récupération du libellé dans la table TDWH_JDE_F0005_HUB
)
select
SITE_KEY, COMPANY_KEY, CUSTOMER_KEY, SITE_COMPANY_LINK_KEY, SITE_CUSTOMER_LINK_KEY, SITE_CD, SITE_NAME, TYP_REF, SITE_TYPE_CD, SITE_TYPE_NAME, SITE_PRF_CD, SITE_PRF_NAME, DER_FLG,COMPANY_ID, SITE_GUARDSHIP_CD, SITE_GUARDSHIP_NAME, CAPACITY_PROPRE_NBR_PL, CAPACITY_NEGOCE_NBR_PL, CAPACITY_NBR_PL, EFFECTIVE_FROM, LDTS, RSCR,
sha2_binary(to_variant(ARRAY_CONSTRUCT(SITE_KEY, COMPANY_KEY, CUSTOMER_KEY, SITE_COMPANY_LINK_KEY, SITE_CUSTOMER_LINK_KEY, SITE_CD, SITE_NAME, TYP_REF, SITE_TYPE_CD, SITE_TYPE_NAME, SITE_PRF_CD, SITE_PRF_NAME, DER_FLG, COMPANY_ID, SITE_GUARDSHIP_CD, SITE_GUARDSHIP_NAME, CAPACITY_PROPRE_NBR_PL, CAPACITY_NEGOCE_NBR_PL, CAPACITY_NBR_PL)),512) as HASHDIFF
from source_data
),row_rank_1 AS (
    SELECT rr."SITE_CUSTOMER_LINK_KEY", rr."SITE_KEY", rr."CUSTOMER_KEY", rr."LDTS", rr."RSCR",
           ROW_NUMBER() OVER(
               PARTITION BY rr."SITE_CUSTOMER_LINK_KEY"
               ORDER BY rr."LDTS"
           ) AS row_number
    FROM __dbt__cte__dwh_site_tmp AS rr
    WHERE rr."SITE_CUSTOMER_LINK_KEY" IS NOT NULL
    AND rr."SITE_KEY" IS NOT NULL
    AND rr."CUSTOMER_KEY" IS NOT NULL
    QUALIFY row_number = 1
),

records_to_insert AS (
    SELECT a."SITE_CUSTOMER_LINK_KEY", a."SITE_KEY", a."CUSTOMER_KEY", a."LDTS", a."RSCR"
    FROM row_rank_1 AS a
)

SELECT * FROM records_to_insert
        );
      
  