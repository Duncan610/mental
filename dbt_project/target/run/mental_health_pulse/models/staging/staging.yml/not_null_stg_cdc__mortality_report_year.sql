select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select report_year
from "mental_health_pulse"."main_staging"."stg_cdc__mortality"
where report_year is null



      
    ) dbt_internal_test