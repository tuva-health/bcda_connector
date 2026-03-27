with explanationofbenefit_procedure as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit_procedure') }} 
  {% else %} {{ source('bcda', 'explanationofbenefit_procedure') }}{% endif %}
)

SELECT
    *
FROM explanationofbenefit_procedure