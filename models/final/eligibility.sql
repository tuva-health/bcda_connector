with file_variable as(

    select
        cast('{{ var('bcda_coverage_file_prefix') }}' as {{ dbt.type_string() }} ) as bcda_coverage_file_prefix
)


, parse_enrollment_start_date as(
    select
        beneficiary_reference as patient_id
        , filename
        , cast(substring(replace(filename,f.bcda_coverage_file_prefix,''),1,6) as varchar)||'01' as enrollment_start_date
    from {{ ref('coverage') }} p
    cross join file_variable f
)
, parse_enrollment_end_date as(
 select
    beneficiary_reference as patient_id
    , cov.filename
     , cast(substring(replace(cov.filename,f.bcda_coverage_file_prefix,''),1,6) as varchar)||'01' as enrollment_end_date
    from {{ ref('coverage') }} cov
    left join {{ ref('coverage_extension') }} ext
        on cov.id = ext.coverage_id
        and url = 'https://bluebutton.cms.gov/resources/variables/a_trm_cd'
    cross join file_variable f
    where coverage_id is not null
    and valuecoding_code <> '0'
)
, min_enrollment as (
    select
        s.patient_id
        , min({{ the_tuva_project.try_to_cast_date('s.enrollment_start_date', 'YYYYMMDD') }} ) as enrollment_start_date
        , min({{ the_tuva_project.try_to_cast_date('e.enrollment_end_date', 'YYYYMMDD') }} ) as enrollment_end_date
    from parse_enrollment_start_date s
    left join parse_enrollment_end_date e
        on s.patient_id = e.patient_id
    group by
        s.patient_id

)
, medicare_status as(
    select
        beneficiary_reference
        , coverage_id
        , valuecoding_code as medicare_status_code
        , valuecoding_display as medicare_status_description
    from {{ ref('coverage') }} cov
    left join {{ ref('coverage_extension') }} ext
        on cov.id = ext.coverage_id
        and url = 'https://bluebutton.cms.gov/resources/variables/ms_cd'
    where coverage_id is not null
)


select distinct
    cast(pat.id as {{ dbt.type_string() }} ) as patient_id
    , cast(pat_id.value as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as subscriber_id
    , cast(gender as {{ dbt.type_string() }} ) as gender
    , cast(case
        when extension_0_valuecoding_display = 'Unknown' then 'Unknown'
        when extension_0_valuecoding_display = 'White' then 'White'
        when extension_0_valuecoding_display = 'Black' then 'black or african american'
        when extension_0_valuecoding_display = 'Other' then 'other race'
        when extension_0_valuecoding_display = 'Asian' then 'asian'
        when extension_0_valuecoding_display = 'Hispanic' then 'other race'
        when extension_0_valuecoding_display = 'North American Native' then 'american indian or alaska native'
     end as {{ dbt.type_string() }} ) as race
    , cast(birthdate as date) as birth_date
    , cast(null as date) as death_date
    , cast(case
        when lower(deceasedboolean) = 'true' then 1
            else 0
        end as {{ dbt.type_int() }} ) as death_flag
    , cast(enrollment_start_date as date) as enrollment_start_date
    , cast(coalesce(enrollment_end_date, {{ the_tuva_project.try_to_cast_date('last_day(getdate())', 'YYYYMMDD') }} ) as date) as enrollment_end_date
    , cast('cms' as {{ dbt.type_string() }} ) as payer
    , cast('medicare' as {{ dbt.type_string() }} ) as payer_type
    , cast('medicare' as {{ dbt.type_string() }} ) as plan
    , cast(null as {{ dbt.type_string() }} ) as original_reason_entitlement_code
    , cast(null as {{ dbt.type_string() }} ) as dual_status_code
    , cast(m.medicare_status_code as {{ dbt.type_string() }} ) as medicare_status_code
    , cast(name_0_family as {{ dbt.type_string() }} ) as first_name
    , cast(name_0_given_0 as {{ dbt.type_string() }} ) as last_name
    , cast(null as {{ dbt.type_string() }} ) as social_security_number
    , cast(null as {{ dbt.type_string() }} ) as subscriber_relation
    , cast(null as {{ dbt.type_string() }} ) as address
    , cast(null as {{ dbt.type_string() }} ) as city
    , cast(address_0_state as {{ dbt.type_string() }} ) as state
    , cast(address_0_postalcode as {{ dbt.type_string() }} ) as zip_code
    , cast(null as {{ dbt.type_string() }} ) as phone
    , cast('bcda' as {{ dbt.type_string() }} ) as data_source
    , cast(pat.filename as {{ dbt.type_string() }} ) as file_name
    , cast(pat.processed_datetime as timestamp) as ingest_datetime
from {{ ref('patient') }} pat
left join min_enrollment en
    on pat.id = en.patient_id
left join {{ ref('patient_identifier') }} pat_id
    on pat.id = pat_id.patient_id
    and pat_id.type_coding_0_code = 'MC'
left join medicare_status m
    on pat.resourcetype||'/'||pat.id = beneficiary_reference
