-- Generic test macro: assert all values in a column are positive (> 0).
-- Usage in schema yml:
--   data_tests:
--     - positive_values
{% test positive_values(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
