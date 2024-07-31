

      select
      eob_id
      , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('explanationofbenefit_diagnosis'),'sequence')
            , agg = 'max'
            , then_value = 'diagnosiscodeableconcept_coding_0_code'
            , prefix = 'diagnosis_code_'
            , else_value = 'null'
            , quote_identifiers = false
              )
}}
           , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('explanationofbenefit_diagnosis'),'sequence')
            , agg = 'max'
            , then_value = 'type_0_coding_0_code'
            , prefix = 'type_'
            , else_value = 'null'
            , quote_identifiers = false
              )
}}
    from {{ ref('explanationofbenefit_diagnosis') }}
    group by
      eob_id
