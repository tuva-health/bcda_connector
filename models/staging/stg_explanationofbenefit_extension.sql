with explanationofbenefit_extension as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit_extension') }} {% else %} {{ source('bcda', 'explanationofbenefit_extension') }}{% endif %}
)

SELECT
    *
FROM explanationofbenefit_extension