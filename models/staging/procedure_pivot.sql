

      select
      eob_id
      , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('explanationofbenefit_procedure'),'sequence')
            , agg = 'max'
            , then_value = 'procedurecodeableconcept_coding_0_code'
            , prefix = 'procedure_code_'
            , else_value = 'null'
            , quote_identifiers = false
              )
}}
           , {{ dbt_utils.pivot('sequence'
            , dbt_utils.get_column_values(ref('explanationofbenefit_procedure'),'sequence')
            , agg = 'max'
            , then_value = 'date'
            , prefix = 'procedure_date_'
            , else_value = 'null'
            , quote_identifiers = false
              )
}}
    from {{ ref('explanationofbenefit_procedure') }}
    group by
      eob_id
