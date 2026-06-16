select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select post_created_at
from "mental_health_pulse"."main_staging"."stg_bluesky__posts"
where post_created_at is null



      
    ) dbt_internal_test