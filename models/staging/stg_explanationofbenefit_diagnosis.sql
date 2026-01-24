with explanationofbenefit_diagnosis as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('explanationofbenefit_diagnosis') }} {% else %} {{ source('bcda', 'explanationofbenefit_diagnosis') }}{% endif %}
),

SELECT
    *
FROM explanationofbenefit_diagnosis