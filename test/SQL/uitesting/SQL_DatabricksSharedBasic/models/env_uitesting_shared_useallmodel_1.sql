WITH env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

),

env_uitesting_shared_child_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_child_model_1')}}

),

env_uitesting_shared_mid_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_mid_model_1')}}

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in0.c_smallint AS c_smallint,
    in1.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM env_uitesting_shared_child_model_1 AS in0
  INNER JOIN env_uitesting_shared_mid_model_1 AS in1
     ON in0.c_smallint = in1.c_smallint

),

Join_2 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in0.c_smallint AS c_smallint,
    in0.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM Join_1 AS in0
  INNER JOIN env_uitesting_shared_parent_model_1 AS in1
     ON in0.c_double = in1.c_double

)

SELECT *

FROM Join_2
