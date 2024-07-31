select
    identifier_0_value as claim_id
    , item_0_sequence as claim_line_number
    , type_coding_1_code as claim_type
    , billableperiod_extension_0_valuecoding_code as claim_status
    , replace(patient_reference,'Patient/') as patient_id
    , null as member_id
    , 'medicare' as payer
    , 'medicare' as plan
    , null as prescribing_provider_npi
    , null as dispensing_provider_npi
    , item_0_serviceddate as dispensing_date
    , item_0_productorservice_coding_0_code as ndc_code
    , case
        when item_0_quantity_value = '' then null
            else item_0_quantity_value
    end as quantity
    , null as days_supply
    , null as refills
    , payment_date as paid_date
    , case
        when total_0_amount_value = '' then null
            else total_0_amount_value
    end as paid_amount
    , null as allowed_amount
    , null as charge_amount
    , null as coinsurance_amount
    , null as copayment_amount
    , null as deductible_amount
    , null as in_network_flag
    , 'bcda' as data_source
    , filename as file_name
    , processed_datetime as ingest_datetime
from {{ ref('explanationofbenefit') }} eob
where eob.type_coding_1_code = 'pharmacy'