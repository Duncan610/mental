select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select topic_category
from "mental_health_pulse"."main_staging"."stg_bluesky__posts"
where topic_category is null



      
    ) dbt_internal_test