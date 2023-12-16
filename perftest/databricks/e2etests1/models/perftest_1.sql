WITH perftestseed_1 AS (

  SELECT * 
  
  FROM {{ ref('perftestseed_1')}}

),

Reformat_1 AS (

  SELECT * 
  
  FROM perftestseed_1 AS in0

)

SELECT *

FROM Reformat_1
