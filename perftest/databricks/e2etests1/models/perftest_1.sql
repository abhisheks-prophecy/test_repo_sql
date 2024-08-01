WITH perftestseed_1 AS (

  SELECT * 
  
  FROM {{ ref('perftestseed_1')}}

),

Reformat_1 AS (

  {#Simplifies data extraction from a performance test dataset.#}
  SELECT c_int AS c_int
  
  FROM perftestseed_1 AS in0

)

SELECT *

FROM Reformat_1
