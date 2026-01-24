with explanationofbenefit_item_0_extension as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit_item_0_extension') }} 
  {% else %} {{ source('bcda', 'explanationofbenefit_item_0_extension') }}{% endif %}
),

SELECT
    *
FROM explanationofbenefit_item_0_extension