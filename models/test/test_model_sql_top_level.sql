WITH ALL_TYPE_TABLE_SMALLER AS (

  SELECT * 
  
  FROM {{ source('QA_DATABASE.QA_SIMPLE_SCHEMA', 'ALL_TYPE_TABLE_SMALLER') }}

),

TABLE_COMPLEX_TYPES_1 AS (

  SELECT * 
  
  FROM {{ source('QA_DATABASE.QA_SIMPLE_SCHEMA', 'TABLE_COMPLEX_TYPES_1') }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM TABLE_COMPLEX_TYPES_1 AS in0

),

SUPPLIER AS (

  SELECT * 
  
  FROM {{ source('xyz1', 'SUPPLIER') }}

)

SELECT * 

FROM Reformat_1
