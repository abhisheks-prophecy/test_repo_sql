WITH all_type_parquet AS (

  {#test comment 2 kiran#}
  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

raw_customers_1 AS (

  {#blah#}
  SELECT * 
  
  FROM raw_customers

),

Join_1 AS (

  SELECT 
    in0.id AS id,
    in1.first_name AS first_name,
    in0.last_name AS last_name
  
  FROM raw_customers AS in0
  INNER JOIN raw_customers_1 AS in1
     ON in0.id = in1.id
  INNER JOIN all_type_parquet AS in2
     ON in1.id != in2.c_int

)

SELECT *

FROM Join_1
