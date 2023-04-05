{% macro customers_all(id_max=2) %}

select first_name, last_name from customers where id <= {{ id_max }}
{% endmacro %}

 