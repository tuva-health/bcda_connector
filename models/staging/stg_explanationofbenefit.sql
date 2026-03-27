with explanationofbenefit as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit') }} 
  {% else %} {{ source('bcda', 'explanationofbenefit') }}{% endif %}
)

SELECT
    *
FROM explanationofbenefit