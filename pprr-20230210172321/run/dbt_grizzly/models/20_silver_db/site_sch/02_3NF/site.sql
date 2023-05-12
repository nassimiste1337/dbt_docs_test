
  create or replace   view PRFR_00_DEV_20_SILVER_DB.PRFR_DWH_SITE_SCH.site
  
    
    
(
  
    "SITE_CD" COMMENT $$Magasin - Code$$, 
  
    "SITE_NAME" COMMENT $$Libellé du site$$, 
  
    "TYP_REF" COMMENT $$Typologie du Referentiel tracabilité$$, 
  
    "SITE_TYPE_CD" COMMENT $$Code du type de site$$, 
  
    "SITE_TYPE_NAME" COMMENT $$Libellé du type de site$$, 
  
    "SITE_PRF_CD" COMMENT $$Code du site PRF$$, 
  
    "SITE_PRF_NAME" COMMENT $$Libellé du site PRF$$, 
  
    "DER_FLG" COMMENT $$Flag indiquant que nous sommes sur une DER$$, 
  
    "COMPANY_ID" COMMENT $$Identifiant technique de la société$$, 
  
    "SITE_GUARDSHIP_CD" COMMENT $$Code du site de rattachement (DER)$$, 
  
    "SITE_GUARDSHIP_NAME" COMMENT $$Libellé du site de rattachement (DER)$$, 
  
    "CAPACITY_PROPRE_NBR_PL" COMMENT $$Capacité en nombre de palette pour les produits propres$$, 
  
    "CAPACITY_NEGOCE_NBR_PL" COMMENT $$Capacité en nombre de palette pour les produits de negoce$$, 
  
    "CAPACITY_NBR_PL" COMMENT $$Capacité en nombre de palette$$
  
)

   as (
    with source_data_init as (
    select distinct sat."SITE_CD", sat."SITE_NAME", sat."TYP_REF", sat."SITE_TYPE_CD", sat."SITE_TYPE_NAME", sat."SITE_PRF_CD", sat."SITE_PRF_NAME", sat."DER_FLG", sat."COMPANY_ID", sat."SITE_GUARDSHIP_CD", sat."SITE_GUARDSHIP_NAME", sat."CAPACITY_PROPRE_NBR_PL", sat."CAPACITY_NEGOCE_NBR_PL", sat."CAPACITY_NBR_PL"
    from PRFR_00_DEV_20_SILVER_DB.PRFR_DWH_SITE_SCH.dwh_site_hub hub
    inner join PRFR_00_DEV_20_SILVER_DB.PRFR_DWH_SITE_SCH.dwh_site_sat sat on hub.SITE_KEY = sat.SITE_KEY
    qualify row_number() over (partition by sat.SITE_KEY order by sat.LDTS desc) = 1
)
select "SITE_CD", "SITE_NAME", "TYP_REF", "SITE_TYPE_CD", "SITE_TYPE_NAME", "SITE_PRF_CD", "SITE_PRF_NAME", "DER_FLG", "COMPANY_ID", "SITE_GUARDSHIP_CD", "SITE_GUARDSHIP_NAME", "CAPACITY_PROPRE_NBR_PL", "CAPACITY_NEGOCE_NBR_PL", "CAPACITY_NBR_PL"
from source_data_init



  );

