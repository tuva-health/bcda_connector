select
    eob.id
    , identifier_0_value as claim_id
    , item_0_sequence as claim_line_number
    , type_coding_2_code as claim_type
    , billableperiod_extension_0_valuecoding_code as claim_status
    , replace(patient_reference,'Patient/') as patient_id
    , null as member_id
    , 'medicare' as payer
    , 'medicare' as plan
    , nullif(billableperiod_start,'') as claim_start_date
    , nullif(billableperiod_end,'') as claim_end_date
    , null as claim_line_start_date
    , null as claim_line_end_date
    , nullif(admission.timingperiod_start,'') as admission_date
    , nullif(admission.timingperiod_end,'') as discharge_date
    , ad_src.code_coding_0_code as admit_source_code
    , ad_type.code_coding_0_code as admit_type_code
    , dis.code_coding_0_code as discharge_disposition_code
    , item_0_locationcodeableconcept_coding_0_code as place_of_service_code
    , cast(facility_extension_0_valuecoding_code as varchar)||
        cast(tob_2.valuecoding_code as varchar)||
        cast(tob_3.code_coding_0_code as varchar)
        as bill_type_code
    , null as ms_drg_code
    , null as apr_drg_code
    , item_0_revenue_coding_0_code as revenue_center_code
    , nullif(item_0_quantity_value, '') as service_unit_quantity
    , item_0_productorservice_coding_0_code as hcpcs_code
    , null as hcpcs_modifier_1
    , null as hcpcs_modifier_2
    , null as hcpcs_modifier_3
    , null as hcpcs_modifier_4
    , null as hcpcs_modifier_5
    , null as rendering_npi
    , null as rendering_tin
    , null as billing_npi
    , null as billing_tin
    , null as facility_npi
    , nullif(payment_date,'') as paid_date
    , nullif(payment_amount_value,'') as paid_amount
    , null as allowed_amount
    , null as charge_amount
    , null as coinsurance_amount
    , null as copayment_amount
    , null as deductible_amount
    , null as total_cost_amount
    , 'icd_10_cm' as diagnosis_code_type
    , dx.diagnosis_code_1
    , dx.diagnosis_code_2
    , dx.diagnosis_code_3
    , dx.diagnosis_code_4
    , dx.diagnosis_code_5
    , dx.diagnosis_code_6
    , dx.diagnosis_code_7
    , dx.diagnosis_code_8
    , dx.diagnosis_code_9
    , dx.diagnosis_code_10
    , dx.diagnosis_code_11
    , dx.diagnosis_code_12
    , dx.diagnosis_code_13
    , dx.diagnosis_code_14
    , dx.diagnosis_code_15
    , dx.diagnosis_code_16
    , dx.diagnosis_code_17
    , dx.diagnosis_code_18
    , dx.diagnosis_code_19
    , dx.diagnosis_code_20
    , null as diagnosis_code_21
    , null as diagnosis_code_22
    , null as diagnosis_code_23
    , null as diagnosis_code_24
    , null as diagnosis_code_25
    , null as diagnosis_poa_1
    , null as diagnosis_poa_2
    , null as diagnosis_poa_3
    , null as diagnosis_poa_4
    , null as diagnosis_poa_5
    , null as diagnosis_poa_6
    , null as diagnosis_poa_7
    , null as diagnosis_poa_8
    , null as diagnosis_poa_9
    , null as diagnosis_poa_10
    , null as diagnosis_poa_11
    , null as diagnosis_poa_12
    , null as diagnosis_poa_13
    , null as diagnosis_poa_14
    , null as diagnosis_poa_15
    , null as diagnosis_poa_16
    , null as diagnosis_poa_17
    , null as diagnosis_poa_18
    , null as diagnosis_poa_19
    , null as diagnosis_poa_20
    , null as diagnosis_poa_21
    , null as diagnosis_poa_22
    , null as diagnosis_poa_23
    , null as diagnosis_poa_24
    , null as diagnosis_poa_25
    , 'icd-10-pcs' as procedure_code_type
    , px.procedure_code_1
    , px.procedure_code_2
    , px.procedure_code_3
    , px.procedure_code_4
    , px.procedure_code_5
    , px.procedure_code_6
    , px.procedure_code_7
    , px.procedure_code_8
    , px.procedure_code_9
    , px.procedure_code_10
    , px.procedure_code_11
    , px.procedure_code_12
    , px.procedure_code_13
    , px.procedure_code_14
    , px.procedure_code_15
    , px.procedure_code_16
    , px.procedure_code_17
    , px.procedure_code_18
    , px.procedure_code_19
    , px.procedure_code_20
    , px.procedure_code_21
    , null as procedure_code_22
    , null as procedure_code_23
    , null as procedure_code_24
    , null as procedure_code_25
    , px.procedure_date_1
    , px.procedure_date_2
    , px.procedure_date_3
    , px.procedure_date_4
    , px.procedure_date_5
    , px.procedure_date_6
    , px.procedure_date_7
    , px.procedure_date_8
    , px.procedure_date_9
    , px.procedure_date_10
    , px.procedure_date_11
    , px.procedure_date_12
    , px.procedure_date_13
    , px.procedure_date_14
    , px.procedure_date_15
    , px.procedure_date_16
    , px.procedure_date_17
    , px.procedure_date_18
    , px.procedure_date_19
    , px.procedure_date_20
    , px.procedure_date_21
    , null as procedure_date_22
    , null as procedure_date_23
    , null as procedure_date_24
    , null as procedure_date_25
    , null as in_network_flag
    , 'bcda' as data_source
    , eob.filename as file_name
    , eob.processed_datetime as ingest_datetime
from {{ ref('explanationofbenefit') }} eob
left join {{ source('explanationofbenefit_extension') }} tob_2
    on eob.id = tob_2.eob_id
    and url = 'https://bluebutton.cms.gov/resources/variables/clm_srvc_clsfctn_type_cd'
left join {{ ref('explanationofbenefit_supportinginfo') }} tob_3
    on eob.id = tob_3.eob_id
    and LOWER(category_coding_0_display) = 'type of bill'
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
where eob.type_coding_1_code <> 'pharmacy'
