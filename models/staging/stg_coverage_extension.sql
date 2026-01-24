with coverage_extension as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('coverage_extension') }} {% else %} {{ source('bcda', 'coverage_extension') }}{% endif %}
),

SELECT
    *
FROM coverage_extension