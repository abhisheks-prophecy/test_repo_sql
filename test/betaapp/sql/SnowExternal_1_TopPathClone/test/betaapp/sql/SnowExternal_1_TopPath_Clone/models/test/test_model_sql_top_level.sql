WITH test_seed_1 AS (

  SELECT * 
  
  FROM {{ ref('test_seed_1')}}

),

Reformat_2 AS (

  SELECT * 
  
  FROM test_seed_1 AS in0

),

TABLE_COMPLEX_TYPES_1 AS (

  SELECT * 
  
  FROM {{ source('QA_DATABASE.QA_SIMPLE_SCHEMA', 'TABLE_COMPLEX_TYPES_1') }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM TABLE_COMPLEX_TYPES_1 AS in0

)

SELECT * 

FROM Reformat_1
