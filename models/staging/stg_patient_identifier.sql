with patient_identifier as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('patient_identifier') }} 
  {% else %} {{ source('bcda', 'patient_identifier') }}{% endif %}
)

SELECT
    *
FROM patient_identifier