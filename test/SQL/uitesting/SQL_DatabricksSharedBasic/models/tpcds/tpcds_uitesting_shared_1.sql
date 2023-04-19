WITH raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

raw_customers_1 AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

Join_1 AS (

  SELECT 
    in0.id AS id,
    in1.first_name AS first_name,
    in0.last_name AS last_name
  
  FROM raw_customers AS in0
  INNER JOIN raw_customers_1 AS in1
     ON in0.id = in1.id

)

SELECT *

FROM Join_1
