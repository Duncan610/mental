
with source as (
    select * from {{ source('raw_bluesky', 'posts') }}
),

renamed as (
    select
        post_uri                                    as post_uri,
        post_cid                                    as post_cid,
        author_did                                  as author_did,
        lower(author_handle)                        as author_handle,
        nullif(trim(post_text), '')                 as post_text,
        coalesce(like_count, 0)                     as like_count,
        coalesce(repost_count, 0)                   as repost_count,
        coalesce(reply_count, 0)                    as reply_count,
        coalesce(like_count, 0)
            + coalesce(repost_count, 0)
            + coalesce(reply_count, 0)              as total_engagement,
        search_term                                 as matched_search_term,
        case
            when lower(search_term) in ('#depression', 'feeling depressed')
                then 'depression'
            when lower(search_term) in ('#anxiety', 'anxiety attack')
                then 'anxiety'
            when lower(search_term) in ('#suicideprevention', 'mental health crisis')
                then 'crisis'
            when lower(search_term) in ('#recoveryispossible', '#therapyworks')
                then 'recovery'
            else 'general'
        end                                         as topic_category,
        langs                                       as post_langs,
        case when langs ilike '%en%' then true
             else false end                         as is_english,
        cast(created_at as timestamp)               as post_created_at,
        cast(created_at as date)                    as post_date,
        _dlt_load_id                                as _loaded_at
    from source
),

deduplicated as (
    select *
    from renamed
    qualify row_number() over (
        partition by post_uri
        order by post_created_at desc
    ) = 1
)

select * from deduplicated
