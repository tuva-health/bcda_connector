with file_variable as(

    select
        '{{ var('bcda_patient_file_prefix') }}' as bcda_patient_file_prefix
        , '{{ var('bcda_coverage_file_prefix') }}' as bcda_coverage_file_prefix
)


, parse_enrollment_start_date as(

    select
        id as patient_id
        , filename
        , min(date(cast(substring(replace(filename,f.bcda_patient_file_prefix,''),1,6) as varchar)||'01','YYYYMMDD'))
        as enrollment_start_date
    from {{ ref('patient') }} p
    cross join file_variable f
    group by
        id
        , filename
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
, parse_enrollment_end_date as(
 select
    beneficiary_reference as patient_id
    , cov.filename
     , min(date(cast(substring(replace(cov.filename,f.bcda_coverage_file_prefix,''),1,6) as varchar)||'01','YYYYMMDD'))
        as enrollment_end_date
    from {{ ref('coverage') }} cov
    left join {{ ref('coverage_extension') }} ext
        on cov.id = ext.coverage_id
        and url = 'https://bluebutton.cms.gov/resources/variables/a_trm_cd'
    cross join file_variable f
    where coverage_id is not null
    and valuecoding_code <> '0'
    group by
        beneficiary_reference
        , cov.filename
)

select distinct
    pat.id as patient_id
    , pat_id.value as member_id
    , null as subscriber_id
    , gender
    , case
        when extension_0_valuecoding_display = 'Unknown' then 'Unknown'
        when extension_0_valuecoding_display = 'White' then 'White'
        when extension_0_valuecoding_display = 'Black' then 'black or african american'
        when extension_0_valuecoding_display = 'Other' then 'other race'
        when extension_0_valuecoding_display = 'Asian' then 'asian'
        when extension_0_valuecoding_display = 'Hispanic' then 'other race'
        when extension_0_valuecoding_display = 'North American Native' then 'american indian or alaska native'
    end as race
    , birthdate as birth_date
    , null as death_date
    , case
        when lower(deceasedboolean) = 'true' then 1
            else 0
        end as death_flag
    , enrollment_start_date as enrollment_start_date
    , coalesce(enrollment_end_date, last_day(getdate())) as enrollment_end_date
    , 'cms' as payer
    , 'medicare' as payer_type
    , 'medicare' as plan
    , null as original_reason_entitlement_code
    , null as dual_status_code
    , m.medicare_status_code as medicare_status_code
    , name_0_family as first_name
    , name_0_given_0 as last_name
    , null as social_security_number
    , null as subscriber_relation
    , null as address
    , null as city
    , address_0_state as state
    , address_0_postalcode as zip_code
    , null as phone
    , 'bcda' as data_source
    , pat.filename as file_name
    , pat.processed_datetime as ingest_datetime
from {{ ref('patient') }} pat
left join parse_enrollment_start_date es
    on pat.id = es.patient_id
left join parse_enrollment_end_date ee
    on pat.id = ee.patient_id
left join {{ ref('patient_identifier') }} pat_id
    on pat.id = pat_id.patient_id
    and pat_id.type_coding_0_code = 'MC'
left join medicare_status m
    on pat.resourcetype||'/'||pat.id = beneficiary_reference
