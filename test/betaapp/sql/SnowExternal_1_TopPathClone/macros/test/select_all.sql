{% macro select_all(name='customers') %}
select * from {{name}}
{% endmacro %}

{% macro get_all_where_column_is_not_null(model_name, column_name) %}
select * from {{ model_name }} where {{ column_name }} is not null
{% endmacro %}

{% macro get_distinct_columns_not_in_int_range(model_name, column_name_int, unaccepted_values_int=[1, 2]) %}
{% set max_more_than=4 %}      
with all_values as (
  select distinct {{ column_name_int }} as distinct_column from {{ model_name }}
),
all_values_not_in_list as (
  select distinct_column from all_values where distinct_column not in (
  {% for unaccepted_value_int in unaccepted_values_int %}
  {% if unaccepted_value_int >= max_more_than %}
  4
  {% else %}
  {{ unaccepted_value_int }}
  {% endif %}
  {% if not loop.last %},{% endif %}
  {% endfor %}
)
)

select * from all_values_not_in_list
{% endmacro %}

 