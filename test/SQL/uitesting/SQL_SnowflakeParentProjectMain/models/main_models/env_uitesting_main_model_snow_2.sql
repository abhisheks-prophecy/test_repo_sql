WITH model_with_only_seed AS (

  SELECT * 
  
  FROM {{ ref('model_with_only_seed')}}

),

Reformat_1 AS (

  SELECT 
    COUNTRY_CODE AS COUNTRY_CODE,
    COUNTRY_LABEL AS COUNTRY_LABEL,
    CODE AS CODE,
    SERVICE_LABEL AS SERVICE_LABEL
  
  FROM model_with_only_seed AS in0

),

ALL_TYPE_TABLE_NON_GEOMETRY AS (

  SELECT * 
  
  FROM {{ source('alias_base_QA_DATABASE_QA_SCHEMA', 'ALL_TYPE_TABLE_NON_GEOMETRY') }}

),

goods_classification AS (

  SELECT * 
  
  FROM {{ ref('goods_classification')}}

),

Join_1 AS (

  SELECT 
    in0.COUNTRY_CODE AS COUNTRY_CODE,
    in0.COUNTRY_LABEL AS COUNTRY_LABEL,
    in0.CODE AS CODE,
    in0.SERVICE_LABEL AS SERVICE_LABEL,
    in1.NZHSC_Level_2_Code_HS4 AS NZHSC_Level_2_Code_HS4,
    in1.NZHSC_Level_1_Code_HS2 AS NZHSC_Level_1_Code_HS2,
    in1.NZHSC_Level_2 AS NZHSC_Level_2,
    in1.NZHSC_Level_1 AS NZHSC_Level_1,
    in1.Status_HS4 AS Status_HS4,
    in2.C_STRING AS C_STRING
  
  FROM Reformat_1 AS in0
  INNER JOIN goods_classification AS in1
     ON in0.SERVICE_LABEL != in1.Status_HS4
  INNER JOIN ALL_TYPE_TABLE_NON_GEOMETRY AS in2
     ON in0.COUNTRY_CODE != in2.C_STRING

)

SELECT *

FROM Join_1
