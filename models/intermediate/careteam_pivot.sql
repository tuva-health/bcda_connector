select
  eob_id
  , max(provider_identifier_value) as attending
from {{ ref('stg_explanationofbenefit_careteam') }}
where provider_identifier_type_coding_0_code = 'npi'
group by
  eob_id
