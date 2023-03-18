{% macro order_payment_method_amounts(payment_methods) %}
SELECT 
  order_id,
  {% for payment_method in payment_methods %}
    sum(CASE
      WHEN payment_method = '{{payment_method}}'
        THEN amount
    END) AS {{payment_method}}_amount,
  {% endfor %}
  
  sum(amount) AS total_amount

FROM `prophecy-qa`.qa_test_dataset.payments

GROUP BY 1

{% endmacro %}

 