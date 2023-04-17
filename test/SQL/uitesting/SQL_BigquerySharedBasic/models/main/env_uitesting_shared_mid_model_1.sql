WITH env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

),

Reformat_1 AS (

  SELECT * 
  
  FROM env_uitesting_shared_parent_model_1 AS in0

)

SELECT *

FROM Reformat_1

{% if is_incremental() %}
  
  WHERE c_int64 > (
    SELECT MAX(c_int64)
    
    FROM {{this}}
   )
{% endif %}
