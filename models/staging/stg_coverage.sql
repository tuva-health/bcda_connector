with coverage as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('coverage') }} {% else %} {{ source('bcda', 'coverage') }}{% endif %}
)

SELECT
    *
FROM coverage