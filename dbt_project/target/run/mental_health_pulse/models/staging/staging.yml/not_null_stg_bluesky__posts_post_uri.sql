select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select post_uri
from "mental_health_pulse"."main_staging"."stg_bluesky__posts"
where post_uri is null



      
    ) dbt_internal_test