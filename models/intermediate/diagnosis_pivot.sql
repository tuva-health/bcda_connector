{% set pivot_values = dbt_utils.get_column_values(table=ref('stg_explanationofbenefit_procedure'), column='sequence') %}

      select
      eob_id

      {% if pivot_values %}
      , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('stg_explanationofbenefit_diagnosis'),'sequence')
            , agg = 'max'
            , then_value = 'diagnosiscodeableconcept_coding_0_code'
            , prefix = 'diagnosis_code_'
            , else_value = 'null'
            , quote_identifiers = false
              )
        }}
           , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('stg_explanationofbenefit_diagnosis'),'sequence')
            , agg = 'max'
            , then_value = 'type_0_coding_0_code'
            , prefix = 'type_'
            , else_value = 'null'
            , quote_identifiers = false
              )
        }}
      {% else %}
        -- Fallback behavior: Table is empty
        -- Manually select null casted to the correct type for expected columns
        -- OR just select a dummy column to keep the SQL valid
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_1
        , cast(null as {{ dbt.type_string() }}) as type_1
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_2
        , cast(null as {{ dbt.type_string() }}) as type_2
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_3
        , cast(null as {{ dbt.type_string() }}) as type_3
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_4
        , cast(null as {{ dbt.type_string() }}) as type_4
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_5
        , cast(null as {{ dbt.type_string() }}) as type_5
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_6
        , cast(null as {{ dbt.type_string() }}) as type_6
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_7
        , cast(null as {{ dbt.type_string() }}) as type_7
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_8
        , cast(null as {{ dbt.type_string() }}) as type_8
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_9
        , cast(null as {{ dbt.type_string() }}) as type_9
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_10
        , cast(null as {{ dbt.type_string() }}) as type_10
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_11
        , cast(null as {{ dbt.type_string() }}) as type_11
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_12
        , cast(null as {{ dbt.type_string() }}) as type_12
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_13
        , cast(null as {{ dbt.type_string() }}) as type_13
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_14
        , cast(null as {{ dbt.type_string() }}) as type_14
        , cast(null as {{ dbt.type_string() }}) as diagnosis_code_15
        , cast(null as {{ dbt.type_string() }}) as type_15
      {% endif %}
    from {{ ref('stg_explanationofbenefit_diagnosis') }}
    where sequence <> ''
    group by
      eob_id
