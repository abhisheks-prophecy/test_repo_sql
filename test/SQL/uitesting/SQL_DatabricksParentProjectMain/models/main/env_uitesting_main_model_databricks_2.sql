WITH model_with_only_seed AS (

  SELECT * 
  
  FROM {{ ref('model_with_only_seed')}}

),

service_classification AS (

  SELECT * 
  
  FROM {{ ref('service_classification')}}

),

Join_1 AS (

  SELECT 
    in0.country_code AS country_code,
    in0.country_label AS country_label,
    in1.code AS code,
    in0.service_label AS service_label
  
  FROM model_with_only_seed AS in0
  INNER JOIN service_classification AS in1
     ON in0.service_label = in1.service_label

),

raw_orders AS (

  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

Reformat_1 AS (

  SELECT * 
  
  FROM raw_orders AS in0

),

SQLStatement_1 AS (

  SELECT * 
  
  FROM raw_orders

)

SELECT *

FROM raw_orders

{% if is_incremental() %}
  
  WHERE user_id > (
    SELECT MAX(user_id)
    
    FROM {{this}}
   )
{% endif %}
