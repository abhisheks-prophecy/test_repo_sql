WITH raw_payments AS (

  SELECT * 
  
  FROM {{ ref('raw_payments')}}

),

raw_orders AS (

  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

Join_1 AS (

  SELECT 
    in0.id AS id,
    in0.user_id AS user_id,
    in0.order_date AS order_date,
    in0.status AS status,
    in1.order_id AS order_id,
    in1.payment_method AS payment_method,
    in1.amount AS amount
  
  FROM raw_orders AS in0
  INNER JOIN raw_payments AS in1
     ON in0.id = in1.id

),

Reformat_1 AS (

  SELECT * 
  
  FROM Join_1 AS in0

)

SELECT *

FROM Reformat_1
