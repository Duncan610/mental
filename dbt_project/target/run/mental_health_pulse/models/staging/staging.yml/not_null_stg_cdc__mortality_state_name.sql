select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select state_name
from "mental_health_pulse"."main_staging"."stg_cdc__mortality"
where state_name is null



      
    ) dbt_internal_test