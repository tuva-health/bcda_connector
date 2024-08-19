select
    cast(identifier_0_value as {{ dbt.type_string() }} ) as claim_id
    , cast(item_0_sequence as {{ dbt.type_int() }} ) as claim_line_number
    , cast(type_coding_1_code as {{ dbt.type_string() }} ) as claim_type
    , cast(replace(patient_reference,'Patient/','') as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast('medicare' as {{ dbt.type_string() }} ) as payer
    , cast('medicare' as {{ dbt.type_string() }} ) as plan
    , cast(npi.prescribing as {{ dbt.type_string() }} ) as prescribing_provider_npi
    , cast(null as {{ dbt.type_string() }} ) as dispensing_provider_npi
    , cast(item_0_serviceddate as date) as dispensing_date
    , cast(item_0_productorservice_coding_0_code as {{ dbt.type_string() }} ) as ndc_code
    , cast(case
        when item_0_quantity_value = '' then null
            else item_0_quantity_value
    end as {{ dbt.type_int() }} ) as quantity
    , cast(null as {{ dbt.type_int() }} ) as days_supply
    , cast(null as {{ dbt.type_int() }} ) as refills
    , cast(payment_date as date) as paid_date
    , cast(case
        when total_0_amount_value = '' then null
            else total_0_amount_value
    end as {{ dbt.type_float() }} ) as paid_amount
    , cast(null as {{ dbt.type_float() }} ) as allowed_amount
    , cast(null as {{ dbt.type_float() }} ) as charge_amount
    , cast(null as {{ dbt.type_float() }} ) as coinsurance_amount
    , cast(null as {{ dbt.type_float() }} ) as copayment_amount
    , cast(null as {{ dbt.type_float() }} ) as deductible_amount
    , cast(null as {{ dbt.type_float() }} ) as in_network_flag
    , cast('bcda' as {{ dbt.type_string() }} ) as data_source
    , cast(filename as {{ dbt.type_string() }} ) as file_name
    , cast(processed_datetime as timestamp) as ingest_datetime
from {{ ref('explanationofbenefit') }} eob
left join {{ ref('careteam_pivot') }} npi
    on eob.id = npi.eob_id
where eob.type_coding_1_code = 'pharmacy'