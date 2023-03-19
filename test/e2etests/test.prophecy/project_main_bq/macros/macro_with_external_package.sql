{% macro macro_with_external_package() %}
select
  c_int64,
  c_bignumeric,
  c_numeric_1,
  c_numeric_2,
  c_bool,
  count(*)
from `prophecy-qa`.qa_test_dataset.all_type_table
{{ dbt_utils.group_by(5) }}
{% endmacro %}

 