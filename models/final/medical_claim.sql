select
      cast(identifier_0_value as {{ dbt.type_string() }} ) as claim_id
    , cast(item_0_sequence as {{ dbt.type_int() }} ) as claim_line_number
    , cast(type_coding_2_code as {{ dbt.type_string() }} ) as claim_type
    , cast(replace(patient_reference,'Patient/','') as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast('medicare' as {{ dbt.type_string() }} ) as payer
    , cast('medicare' as {{ dbt.type_string() }} ) as plan
    , {{ try_to_cast_date('billableperiod_start', 'YYYY-MM-DD') }}  as claim_start_date
    , {{ try_to_cast_date('billableperiod_end', 'YYYY-MM-DD') }} as claim_end_date
    , {{ try_to_cast_date('eob.item_0_servicedperiod_start', 'YYYY-MM-DD') }} as claim_line_start_date
    , {{ try_to_cast_date('eob.item_0_servicedperiod_end', 'YYYY-MM-DD') }}  as claim_line_end_date
    , {{ try_to_cast_date('admission.timingperiod_start', 'YYYY-MM-DD') }} as admission_date
    , {{ try_to_cast_date('admission.timingperiod_end', 'YYYY-MM-DD') }} as discharge_date
    , cast(ad_src.code_coding_0_code as {{ dbt.type_string() }} ) as admit_source_code
    , cast(ad_type.code_coding_0_code as {{ dbt.type_string() }} ) as admit_type_code
    , cast(dis.code_coding_0_code as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(item_0_locationcodeableconcept_coding_0_code as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(facility_extension_0_valuecoding_code as {{ dbt.type_string() }} )||
        cast(tob_2.valuecoding_code as {{ dbt.type_string() }} )||
        cast(tob_3.code_coding_0_code as {{ dbt.type_string() }} )
        as bill_type_code
    , cast(null as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(null as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(item_0_revenue_coding_0_code as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(replace(item_0_quantity_value, '',null) as {{ dbt.type_int() }} ) as service_unit_quantity
    , cast(replace(eob.item_0_productorservice_coding_0_code,'NULL',null) as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , cast(npi.attending as {{ dbt.type_string() }} ) as rendering_npi
    , cast(null as {{ dbt.type_int() }} ) as rendering_tin
    , cast(eob.provider_identifier_value as {{ dbt.type_string() }} ) as billing_npi
    , cast(null as {{ dbt.type_int() }} ) as billing_tin
    , cast(eob.contained_0_identifier_1_value as {{ dbt.type_string() }} ) as facility_npi
    , cast(replace(payment_date,'',null) as date ) as paid_date
    , cast(replace(payment_amount_value,'',null) as {{ dbt.type_float() }} )as paid_amount
    , cast(null as {{ dbt.type_float() }} ) as allowed_amount
    , cast(null as {{ dbt.type_float() }} ) as charge_amount
    , cast(null as {{ dbt.type_float() }} ) as coinsurance_amount
    , cast(null as {{ dbt.type_float() }} ) as copayment_amount
    , cast(null as {{ dbt.type_float() }} ) as deductible_amount
    , cast(null as {{ dbt.type_float() }} ) as total_cost_amount
    , cast('icd_10_cm' as {{ dbt.type_string() }} ) as diagnosis_code_type
    , cast(dx.diagnosis_code_1 as {{ dbt.type_string() }} ) as diagnosis_code_1
    , cast(dx.diagnosis_code_2 as {{ dbt.type_string() }} ) as diagnosis_code_2
    , cast(dx.diagnosis_code_3 as {{ dbt.type_string() }} ) as diagnosis_code_3
    , cast(dx.diagnosis_code_4 as {{ dbt.type_string() }} ) as diagnosis_code_4
    , cast(dx.diagnosis_code_5 as {{ dbt.type_string() }} ) as diagnosis_code_5
    , cast(dx.diagnosis_code_6 as {{ dbt.type_string() }} ) as diagnosis_code_6
    , cast(dx.diagnosis_code_7 as {{ dbt.type_string() }} ) as diagnosis_code_7
    , cast(dx.diagnosis_code_8 as {{ dbt.type_string() }} ) as diagnosis_code_8
    , cast(dx.diagnosis_code_9 as {{ dbt.type_string() }} ) as diagnosis_code_9
    , cast(dx.diagnosis_code_10 as {{ dbt.type_string() }} ) as diagnosis_code_10
    , cast(dx.diagnosis_code_11 as {{ dbt.type_string() }} ) as diagnosis_code_11
    , cast(dx.diagnosis_code_12 as {{ dbt.type_string() }} ) as diagnosis_code_12
    , cast(dx.diagnosis_code_13 as {{ dbt.type_string() }} ) as diagnosis_code_13
    , cast(dx.diagnosis_code_14 as {{ dbt.type_string() }} ) as diagnosis_code_14
    , cast(dx.diagnosis_code_15 as {{ dbt.type_string() }} ) as diagnosis_code_15
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_16
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_17
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_18
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_19
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_20
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_21
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_22
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_23
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_24
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_25
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , cast('icd-10-pcs' as {{ dbt.type_string() }} ) as procedure_code_type
    , cast(px.procedure_code_1 as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(px.procedure_code_2 as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(px.procedure_code_3 as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(px.procedure_code_4 as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(px.procedure_code_5 as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(px.procedure_code_6 as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(px.procedure_code_7 as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(px.procedure_code_8 as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(px.procedure_code_9 as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(px.procedure_code_10 as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(px.procedure_code_11 as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(px.procedure_code_12 as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(px.procedure_code_13 as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(px.procedure_code_14 as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(px.procedure_code_15 as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(px.procedure_code_16 as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(null as {{ dbt.type_string() }} ) as procedure_code_25
    , {{ try_to_cast_date('px.procedure_date_1', 'YYYYMMDD') }} as procedure_date_1
    , {{ try_to_cast_date('px.procedure_date_2', 'YYYYMMDD') }} as procedure_date_2
    , {{ try_to_cast_date('px.procedure_date_3', 'YYYYMMDD') }} as procedure_date_3
    , {{ try_to_cast_date('px.procedure_date_4', 'YYYYMMDD') }} as procedure_date_4
    , {{ try_to_cast_date('px.procedure_date_5', 'YYYYMMDD') }} as procedure_date_5
    , {{ try_to_cast_date('px.procedure_date_6', 'YYYYMMDD') }} as procedure_date_6
    , {{ try_to_cast_date('px.procedure_date_7', 'YYYYMMDD') }} as procedure_date_7
    , {{ try_to_cast_date('px.procedure_date_8', 'YYYYMMDD') }} as procedure_date_8
    , {{ try_to_cast_date('px.procedure_date_9', 'YYYYMMDD') }} as procedure_date_9
    , {{ try_to_cast_date('px.procedure_date_10', 'YYYYMMDD') }} as procedure_date_10
    , {{ try_to_cast_date('px.procedure_date_11', 'YYYYMMDD') }} as procedure_date_11
    , {{ try_to_cast_date('px.procedure_date_12', 'YYYYMMDD') }} as procedure_date_12
    , {{ try_to_cast_date('px.procedure_date_13', 'YYYYMMDD') }} as procedure_date_13
    , {{ try_to_cast_date('px.procedure_date_14', 'YYYYMMDD') }} as procedure_date_14
    , {{ try_to_cast_date('px.procedure_date_15', 'YYYYMMDD') }} as procedure_date_15
    , {{ try_to_cast_date('px.procedure_date_16', 'YYYYMMDD') }} as procedure_date_16
    , cast(null as date) as procedure_date_17
    , cast(null as date) as procedure_date_18
    , cast(null as date) as procedure_date_19
    , cast(null as date) as procedure_date_20
    , cast(null as date) as procedure_date_21
    , cast(null as date) as procedure_date_22
    , cast(null as date) as procedure_date_23
    , cast(null as date) as procedure_date_24
    , cast(null as date) as procedure_date_25
    , cast(null as {{ dbt.type_int() }} ) as in_network_flag
    , cast('bcda' as {{ dbt.type_string() }} ) as data_source
    , cast(eob.filename as {{ dbt.type_string() }} ) as file_name
    , cast(eob.processed_datetime as timestamp) as ingest_datetime
from {{ ref('explanationofbenefit') }} eob
left join {{ ref('explanationofbenefit_extension') }} tob_2
    on eob.id = tob_2.eob_id
    and url = 'https://bluebutton.cms.gov/resources/variables/clm_srvc_clsfctn_type_cd'
left join {{ ref('explanationofbenefit_supportinginfo') }} tob_3
    on eob.id = tob_3.eob_id
    and lower(category_coding_0_display) = 'type of bill'
left join {{ ref('explanationofbenefit_supportinginfo') }} dis
    on eob.id = dis.eob_id
    and LOWER(dis.category_coding_0_display) = 'discharge status'
left join {{ ref('explanationofbenefit_supportinginfo') }} ad_type
    on eob.id = ad_type.eob_id
    and lower(ad_type.category_coding_0_display) = 'information'
    and lower(ad_type.category_coding_1_display) = 'claim inpatient admission type code'
left join {{ ref('explanationofbenefit_supportinginfo') }} ad_src
    on eob.id = ad_src.eob_id
    and lower(ad_src.category_coding_0_display) = 'information'
    and lower(ad_src.category_coding_1_display) = 'claim source inpatient admission code'
left join {{ ref('explanationofbenefit_supportinginfo') }} admission
    on eob.id = admission.eob_id
    and lower(admission.category_coding_0_code) = 'admissionperiod'
left join {{ ref('diagnosis_pivot') }} dx
    on eob.id = dx.eob_id
left join {{ ref('procedure_pivot') }} px
    on eob.id = px.eob_id
left join {{ ref('careteam_pivot') }} npi
    on eob.id = npi.eob_id
where eob.type_coding_1_code <> 'pharmacy'