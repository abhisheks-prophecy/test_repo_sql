WITH env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

),

Reformat_1 AS (

  SELECT 
    c_tinyint AS c_tinyint,
    c_smallint AS c_smallint,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    c_string AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct,
    {{ SQL_DatabricksSharedBasic.qa_concat_function_main('c_string', 'c_boolean') }} AS c_macro
  
  FROM env_uitesting_shared_parent_model_1 AS in0

),

Macro_1 AS (

  {{ SQL_DatabricksSharedBasic.qa_select_all_not_null_gem(model = 'env_uitesting_shared_parent_model_1', col = 'c_string') }}

),

SQLStatement_1 AS (

  SELECT *
  
  FROM Macro_1 AS in0
  
  WHERE c_tinyint > -1

),

Limit_1 AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  LIMIT 10

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in0.c_int AS c_int,
    in1.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM SQLStatement_1 AS in0
  INNER JOIN Limit_1 AS in1
     ON in0.c_tinyint = in1.c_tinyint

)

SELECT *

FROM Join_1
