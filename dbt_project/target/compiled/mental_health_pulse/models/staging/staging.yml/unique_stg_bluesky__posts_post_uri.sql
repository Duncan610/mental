
    
    

select
    post_uri as unique_field,
    count(*) as n_records

from "mental_health_pulse"."main_staging"."stg_bluesky__posts"
where post_uri is not null
group by post_uri
having count(*) > 1


