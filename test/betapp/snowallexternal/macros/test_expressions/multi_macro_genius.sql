{% macro test_not_null_qa(model, column_name) %}

select count(*) from {{ model }} where {{ column_name }} is null
{% endmacro %}

 {% macro test_relationships_qa(model1, model2, model1_col, model2_col) %}

select count(*)
from (
    select {{ model1_col }} as id from {{ model }}
) as child
left join (
    select {{ model2_col }} as id from {{ model2 }}
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null
{% endmacro %}

 {% macro payments_order_id_table_null_macro(accepted_values=[1, 2]) %}


with all_values as (
    select distinct ORDER_ID as ORDER_ID_MAIN from payments
),
payments_validation_errors as (
    select
        ORDER_ID_MAIN
    from all_values
    where ORDER_ID_MAIN not in (
        {% for accepted_value in accepted_values %}
            {% if accepted_value >= 5 %}
            5
            {% else %}
            {{ accepted_value }}
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    )
)

select count(*) from payments_validation_errors
{% endmacro %}

 {% macro multi_macro_genius() %}

SELECT count(*) FROM TABLE(generator(rowcount => 10))
{% endmacro %}

{% macro qa_customers_all_above_given_id(id_min=2) %}
SELECT * from customers where id > {{ id_min }}
{% endmacro %}

 {% macro test_unique_qa(model, column_name) %}

select count(*)
from (

    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) >= 1
) validation_errors
{% endmacro %}

 