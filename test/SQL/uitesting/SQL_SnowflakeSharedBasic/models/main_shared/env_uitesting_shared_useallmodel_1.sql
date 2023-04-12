WITH env_uitesting_shared_child_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_child_model_1')}}

),

env_uitesting_shared_mid_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_mid_model_1')}}

),

Join_1 AS (

  SELECT 
    in0.C_NUM AS C_NUM,
    in0.C_NUM10 AS C_NUM10,
    in1.C_DEC AS C_DEC,
    in0.C_NUMERIC AS C_NUMERIC,
    in0.C_INT AS C_INT,
    in0.C_INTEGER AS C_INTEGER,
    in0.C_DOUBLE AS C_DOUBLE,
    in0.C_FLOAT AS C_FLOAT,
    in0.C_COUBLE_PRECISION AS C_COUBLE_PRECISION,
    in0.C_REAL AS C_REAL,
    in0.C_VARCHAR AS C_VARCHAR,
    in0.C_VARCHAR50 AS C_VARCHAR50,
    in0.C_CHAR AS C_CHAR,
    in0.C_CHAR10 AS C_CHAR10,
    in0.C_STRING AS C_STRING,
    in0.C_STRING20 AS C_STRING20,
    in0.C_TEXT AS C_TEXT,
    in0.C_TEXT30 AS C_TEXT30,
    in0.C_BINARY AS C_BINARY,
    in0.C_BINARY100 AS C_BINARY100,
    in0.C_VARBINARY AS C_VARBINARY,
    in0.C_BOOL AS C_BOOL,
    in0.C_TIMESTAMP AS C_TIMESTAMP,
    in0.C_DATE AS C_DATE,
    in0.C_DATETIME AS C_DATETIME,
    in0.C_TIME AS C_TIME,
    in0.C_ARRAY AS C_ARRAY,
    in0.C_OBJECT AS C_OBJECT,
    in0.C_GEOGRAPHY AS C_GEOGRAPHY
  
  FROM env_uitesting_shared_child_model_1 AS in0
  INNER JOIN env_uitesting_shared_mid_model_1 AS in1
     ON in0.c_num = in1.c_num

),

env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

),

Join_2 AS (

  SELECT 
    in0.C_NUM AS C_NUM,
    in0.C_NUM10 AS C_NUM10,
    in0.C_DEC AS C_DEC,
    in1.C_NUMERIC AS C_NUMERIC,
    in0.C_INT AS C_INT,
    in0.C_INTEGER AS C_INTEGER,
    in0.C_DOUBLE AS C_DOUBLE,
    in0.C_FLOAT AS C_FLOAT,
    in0.C_COUBLE_PRECISION AS C_COUBLE_PRECISION,
    in0.C_REAL AS C_REAL,
    in0.C_VARCHAR AS C_VARCHAR,
    in0.C_VARCHAR50 AS C_VARCHAR50,
    in0.C_CHAR AS C_CHAR,
    in0.C_CHAR10 AS C_CHAR10,
    in0.C_STRING AS C_STRING,
    in0.C_STRING20 AS C_STRING20,
    in0.C_TEXT AS C_TEXT,
    in0.C_TEXT30 AS C_TEXT30,
    in0.C_BINARY AS C_BINARY,
    in0.C_BINARY100 AS C_BINARY100,
    in0.C_VARBINARY AS C_VARBINARY,
    in0.C_BOOL AS C_BOOL,
    in0.C_TIMESTAMP AS C_TIMESTAMP,
    in0.C_DATE AS C_DATE,
    in0.C_DATETIME AS C_DATETIME,
    in0.C_TIME AS C_TIME,
    in0.C_ARRAY AS C_ARRAY,
    in0.C_OBJECT AS C_OBJECT,
    in0.C_GEOGRAPHY AS C_GEOGRAPHY
  
  FROM Join_1 AS in0
  INNER JOIN env_uitesting_shared_parent_model_1 AS in1
     ON in0.C_BOOL = in1.C_BOOL

)

SELECT *

FROM Join_2
