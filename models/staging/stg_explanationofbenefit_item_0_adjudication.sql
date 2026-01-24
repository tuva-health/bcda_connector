with explanationofbenefit_item_0_adjudication as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit_item_0_adjudication') }} 
  {% else %} {{ source('bcda', 'explanationofbenefit_item_0_adjudication') }}{% endif %}
),

SELECT
    *
FROM explanationofbenefit_item_0_adjudication