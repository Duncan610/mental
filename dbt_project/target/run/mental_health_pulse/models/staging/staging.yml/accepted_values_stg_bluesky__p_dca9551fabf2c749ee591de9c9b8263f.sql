select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        topic_category as value_field,
        count(*) as n_records

    from "mental_health_pulse"."main_staging"."stg_bluesky__posts"
    group by topic_category

)

select *
from all_values
where value_field not in (
    'depression','anxiety','crisis','recovery','general'
)



      
    ) dbt_internal_test