with explanationofbenefit_benefitbalance_0_financial as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit_benefitbalance_0_financial') }} 
  {% else %} {{ source('bcda', 'explanationofbenefit_benefitbalance_0_financial') }}{% endif %}
),

SELECT
    *
FROM explanationofbenefit_benefitbalance_0_financial