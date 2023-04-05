{% macro accepted_values_complex(column_name='first_name', model='customers', values=['Nathan', 'Joe', 'Joseph']) %}
with all_values as (
    select distinct {{ column_name }} as value_field from {{ model }}
),
validation_errors as (
    select value_field from all_values
    where value_field not in (
        {% for value in values %}
            {% if value %}
            'Nothing'
            {% else %}
            {{ value }}
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    )
)

select * from validation_errors
{% endmacro %}

 