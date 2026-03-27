
{% set pivot_values = dbt_utils.get_column_values(table=ref('stg_explanationofbenefit_procedure'), column='sequence') %}

      select
      eob_id

      {% if pivot_values %}
      , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('stg_explanationofbenefit_procedure'),'sequence')
            , agg = 'max'
            , then_value = 'procedurecodeableconcept_coding_0_code'
            , prefix = 'procedure_code_'
            , else_value = 'null'
            , quote_identifiers = false
              )
          }}
      , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('stg_explanationofbenefit_procedure'),'sequence')
            , agg = 'max'
            , then_value = 'date'
            , prefix = 'procedure_date_'
            , else_value = 'null'
            , quote_identifiers = false
              )
          }}
      {% else %}
        -- Fallback behavior: Table is empty
        -- Manually select null casted to the correct type for expected columns
        -- OR just select a dummy column to keep the SQL valid
        , cast(null as {{ dbt.type_string() }}) as procedure_code_1
        , cast(null as {{ dbt.type_string() }}) as procedure_date_1
        , cast(null as {{ dbt.type_string() }}) as procedure_code_2
        , cast(null as {{ dbt.type_string() }}) as procedure_date_2
        , cast(null as {{ dbt.type_string() }}) as procedure_code_3
        , cast(null as {{ dbt.type_string() }}) as procedure_date_3
        , cast(null as {{ dbt.type_string() }}) as procedure_code_4
        , cast(null as {{ dbt.type_string() }}) as procedure_date_4
        , cast(null as {{ dbt.type_string() }}) as procedure_code_5
        , cast(null as {{ dbt.type_string() }}) as procedure_date_5
        , cast(null as {{ dbt.type_string() }}) as procedure_code_6
        , cast(null as {{ dbt.type_string() }}) as procedure_date_6
        , cast(null as {{ dbt.type_string() }}) as procedure_code_7
        , cast(null as {{ dbt.type_string() }}) as procedure_date_7
        , cast(null as {{ dbt.type_string() }}) as procedure_code_8
        , cast(null as {{ dbt.type_string() }}) as procedure_date_8
        , cast(null as {{ dbt.type_string() }}) as procedure_code_9
        , cast(null as {{ dbt.type_string() }}) as procedure_date_9
        , cast(null as {{ dbt.type_string() }}) as procedure_code_10
        , cast(null as {{ dbt.type_string() }}) as procedure_date_10
        , cast(null as {{ dbt.type_string() }}) as procedure_code_11
        , cast(null as {{ dbt.type_string() }}) as procedure_date_11
        , cast(null as {{ dbt.type_string() }}) as procedure_code_12
        , cast(null as {{ dbt.type_string() }}) as procedure_date_12
        , cast(null as {{ dbt.type_string() }}) as procedure_code_13
        , cast(null as {{ dbt.type_string() }}) as procedure_date_13
        , cast(null as {{ dbt.type_string() }}) as procedure_code_14
        , cast(null as {{ dbt.type_string() }}) as procedure_date_14
        , cast(null as {{ dbt.type_string() }}) as procedure_code_15
        , cast(null as {{ dbt.type_string() }}) as procedure_date_15
      {% endif %}

           
    from {{ ref('stg_explanationofbenefit_procedure') }}
    group by
      eob_id
