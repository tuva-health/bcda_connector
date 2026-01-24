with explanationofbenefit_careteam as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit_careteam') }} {% else %} {{ source('bcda', 'explanationofbenefit_careteam') }}{% endif %}
),

SELECT
    *
FROM explanationofbenefit_careteam