select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select post_uri
from "mental_health_pulse"."raw_bluesky"."posts"
where post_uri is null



      
    ) dbt_internal_test