WITH customers_all_1 AS (

  {{ SnowSimpleSchemaProject.customers_all() }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM customers_all_1 AS in0

),

TABLE_COMPLEX_TYPES_1 AS (

  SELECT * 
  
  FROM {{ source('QA_DATABASE.qa_simple_schema', 'TABLE_COMPLEX_TYPES_1') }}

),

Reformat_2 AS (

  SELECT * 
  
  FROM TABLE_COMPLEX_TYPES_1 AS in0

)

SELECT * 

FROM Reformat_2
