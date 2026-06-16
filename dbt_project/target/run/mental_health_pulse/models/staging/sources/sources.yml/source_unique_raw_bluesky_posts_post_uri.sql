select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    post_uri as unique_field,
    count(*) as n_records

from "mental_health_pulse"."raw_bluesky"."posts"
where post_uri is not null
group by post_uri
having count(*) > 1



      
    ) dbt_internal_test