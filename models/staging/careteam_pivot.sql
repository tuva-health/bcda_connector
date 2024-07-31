

      select
      eob_id
      , {{ dbt_utils.pivot('role_coding_0_code'
            , dbt_utils.get_column_values(source('multicare_bcda','explanationofbenefit_careteam'),'role_coding_0_code')
            , agg = 'max'
            , then_value = 'provider_identifier_value'
            , else_value = 'null'
            , quote_identifiers = false
              ) }}

    from {{ source('multicare_bcda','explanationofbenefit_careteam') }}
    where provider_identifier_type_coding_0_code = 'npi'
    group by
      eob_id
