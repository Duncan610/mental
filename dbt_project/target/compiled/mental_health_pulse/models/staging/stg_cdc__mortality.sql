with source as (
    select * from "mental_health_pulse"."raw_cdc"."drug_overdose_monthly"
),

renamed as (
    select
        state_name                                  as state_name,
        state_abbr                                  as state_abbr,
        year                                        as report_year,
        month                                       as report_month,
        period                                      as report_period,
        indicator                                   as substance_indicator,
        data_value                                  as death_count,
        case when data_value is null
             then true else false end               as is_suppressed,
        data_as_of                                  as data_as_of,
        _dlt_load_id                                as _loaded_at
    from source
),

deduplicated as (
    select *
    from renamed
    qualify row_number() over (
        partition by state_name, report_year, report_month, substance_indicator
        order by data_as_of desc
    ) = 1
)

select * from deduplicated