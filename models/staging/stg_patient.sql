with patient as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('patient') }} 
  {% else %} {{ source('bcda', 'patient') }}{% endif %}
),

SELECT
    *
FROM patient