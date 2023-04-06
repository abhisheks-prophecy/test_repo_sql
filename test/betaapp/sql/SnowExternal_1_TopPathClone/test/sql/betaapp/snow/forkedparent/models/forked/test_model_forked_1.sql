WITH ALL_TYPE_TABLE_SMALLER AS (

  SELECT * 
  
  FROM {{ source('TestSchemaQA', 'ALL_TYPE_TABLE_SMALLER') }}

),

Reformat_1 AS (

  SELECT 
    C_NUM AS C_NUM,
    C_NUM10 AS C_NUM10,
    C_DEC AS C_DEC,
    C_NUMERIC AS C_NUMERIC,
    C_INT AS C_INT,
    C_INTEGER AS C_INTEGER,
    C_DOUBLE AS C_DOUBLE,
    C_FLOAT AS C_FLOAT,
    C_COUBLE_PRECISION AS C_COUBLE_PRECISION,
    C_REAL AS C_REAL,
    C_VARCHAR AS C_VARCHAR,
    C_VARCHAR50 AS C_VARCHAR50,
    C_CHAR AS C_CHAR,
    C_CHAR10 AS C_CHAR10,
    concat(C_STRING, {{ SnowForked_P1.test_function_all1('c_string', 'c_string20') }}) AS C_STRING,
    C_STRING20 AS C_STRING20,
    C_TEXT AS C_TEXT,
    C_TEXT30 AS C_TEXT30,
    C_BINARY AS C_BINARY,
    C_BINARY100 AS C_BINARY100,
    C_VARBINARY AS C_VARBINARY,
    C_BOOL AS C_BOOL,
    C_TIMESTAMP AS C_TIMESTAMP,
    C_DATE AS C_DATE,
    C_DATETIME AS C_DATETIME,
    C_TIME AS C_TIME,
    C_ARRAY AS C_ARRAY,
    C_OBJECT AS C_OBJECT,
    C_GEOGRAPHY AS C_GEOGRAPHY
  
  FROM ALL_TYPE_TABLE_SMALLER AS in0

)

SELECT * 

FROM Reformat_1
