{{
  config({    
    "tags": ['ab']
  })
}}

WITH SQLStatement_6 AS (

  SELECT 
    w_warehouse_name,
    w_warehouse_sq_ft,
    w_city,
    w_county,
    w_state,
    w_country,
    ship_carriers,
    year,
    sum(jan_sales) AS jan_sales,
    sum(feb_sales) AS feb_sales,
    sum(mar_sales) AS mar_sales,
    sum(apr_sales) AS apr_sales,
    sum(may_sales) AS may_sales,
    sum(jun_sales) AS jun_sales,
    sum(jul_sales) AS jul_sales,
    sum(aug_sales) AS aug_sales,
    sum(sep_sales) AS sep_sales,
    sum(oct_sales) AS oct_sales,
    sum(nov_sales) AS nov_sales,
    sum(dec_sales) AS dec_sales,
    sum(jan_sales / w_warehouse_sq_ft) AS jan_sales_per_sq_foot,
    sum(feb_sales / w_warehouse_sq_ft) AS feb_sales_per_sq_foot,
    sum(mar_sales / w_warehouse_sq_ft) AS mar_sales_per_sq_foot,
    sum(apr_sales / w_warehouse_sq_ft) AS apr_sales_per_sq_foot,
    sum(may_sales / w_warehouse_sq_ft) AS may_sales_per_sq_foot,
    sum(jun_sales / w_warehouse_sq_ft) AS jun_sales_per_sq_foot,
    sum(jul_sales / w_warehouse_sq_ft) AS jul_sales_per_sq_foot,
    sum(aug_sales / w_warehouse_sq_ft) AS aug_sales_per_sq_foot,
    sum(sep_sales / w_warehouse_sq_ft) AS sep_sales_per_sq_foot,
    sum(oct_sales / w_warehouse_sq_ft) AS oct_sales_per_sq_foot,
    sum(nov_sales / w_warehouse_sq_ft) AS nov_sales_per_sq_foot,
    sum(dec_sales / w_warehouse_sq_ft) AS dec_sales_per_sq_foot,
    sum(jan_net) AS jan_net,
    sum(feb_net) AS feb_net,
    sum(mar_net) AS mar_net,
    sum(apr_net) AS apr_net,
    sum(may_net) AS may_net,
    sum(jun_net) AS jun_net,
    sum(jul_net) AS jul_net,
    sum(aug_net) AS aug_net,
    sum(sep_net) AS sep_net,
    sum(oct_net) AS oct_net,
    sum(nov_net) AS nov_net,
    sum(dec_net) AS dec_net
  
  FROM (
    SELECT 
      w_warehouse_name,
      w_warehouse_sq_ft,
      w_city,
      w_county,
      w_state,
      w_country,
      'GREAT EASTERN' || ',' || 'UPS' AS ship_carriers,
      d_year AS year,
      sum(CASE
        WHEN d_moy = 1
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS jan_sales,
      sum(CASE
        WHEN d_moy = 2
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS feb_sales,
      sum(CASE
        WHEN d_moy = 3
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS mar_sales,
      sum(CASE
        WHEN d_moy = 4
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS apr_sales,
      sum(CASE
        WHEN d_moy = 5
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS may_sales,
      sum(CASE
        WHEN d_moy = 6
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS jun_sales,
      sum(CASE
        WHEN d_moy = 7
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS jul_sales,
      sum(CASE
        WHEN d_moy = 8
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS aug_sales,
      sum(CASE
        WHEN d_moy = 9
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS sep_sales,
      sum(CASE
        WHEN d_moy = 10
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS oct_sales,
      sum(CASE
        WHEN d_moy = 11
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS nov_sales,
      sum(CASE
        WHEN d_moy = 12
          THEN ws_ext_sales_price * ws_quantity
        ELSE 0
      END) AS dec_sales,
      sum(CASE
        WHEN d_moy = 1
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS jan_net,
      sum(CASE
        WHEN d_moy = 2
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS feb_net,
      sum(CASE
        WHEN d_moy = 3
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS mar_net,
      sum(CASE
        WHEN d_moy = 4
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS apr_net,
      sum(CASE
        WHEN d_moy = 5
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS may_net,
      sum(CASE
        WHEN d_moy = 6
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS jun_net,
      sum(CASE
        WHEN d_moy = 7
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS jul_net,
      sum(CASE
        WHEN d_moy = 8
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS aug_net,
      sum(CASE
        WHEN d_moy = 9
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS sep_net,
      sum(CASE
        WHEN d_moy = 10
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS oct_net,
      sum(CASE
        WHEN d_moy = 11
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS nov_net,
      sum(CASE
        WHEN d_moy = 12
          THEN ws_net_paid * ws_quantity
        ELSE 0
      END) AS dec_net
    
    FROM web_sales, warehouse, date_dim, time_dim, ship_mode
    
    WHERE ws_warehouse_sk = w_warehouse_sk and ws_sold_date_sk = d_date_sk and ws_sold_time_sk = t_time_sk and ws_ship_mode_sk = sm_ship_mode_sk and d_year = 1998 and t_time BETWEEN 46866 and 46866 + 28800 and sm_carrier IN ('GREAT EASTERN', 'UPS')
    
    GROUP BY 
      w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
    
    UNION ALL
    
    SELECT 
      w_warehouse_name,
      w_warehouse_sq_ft,
      w_city,
      w_county,
      w_state,
      w_country,
      'GREAT EASTERN' || ',' || 'UPS' AS ship_carriers,
      d_year AS year,
      sum(CASE
        WHEN d_moy = 1
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS jan_sales,
      sum(CASE
        WHEN d_moy = 2
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS feb_sales,
      sum(CASE
        WHEN d_moy = 3
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS mar_sales,
      sum(CASE
        WHEN d_moy = 4
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS apr_sales,
      sum(CASE
        WHEN d_moy = 5
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS may_sales,
      sum(CASE
        WHEN d_moy = 6
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS jun_sales,
      sum(CASE
        WHEN d_moy = 7
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS jul_sales,
      sum(CASE
        WHEN d_moy = 8
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS aug_sales,
      sum(CASE
        WHEN d_moy = 9
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS sep_sales,
      sum(CASE
        WHEN d_moy = 10
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS oct_sales,
      sum(CASE
        WHEN d_moy = 11
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS nov_sales,
      sum(CASE
        WHEN d_moy = 12
          THEN cs_sales_price * cs_quantity
        ELSE 0
      END) AS dec_sales,
      sum(CASE
        WHEN d_moy = 1
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS jan_net,
      sum(CASE
        WHEN d_moy = 2
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS feb_net,
      sum(CASE
        WHEN d_moy = 3
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS mar_net,
      sum(CASE
        WHEN d_moy = 4
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS apr_net,
      sum(CASE
        WHEN d_moy = 5
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS may_net,
      sum(CASE
        WHEN d_moy = 6
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS jun_net,
      sum(CASE
        WHEN d_moy = 7
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS jul_net,
      sum(CASE
        WHEN d_moy = 8
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS aug_net,
      sum(CASE
        WHEN d_moy = 9
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS sep_net,
      sum(CASE
        WHEN d_moy = 10
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS oct_net,
      sum(CASE
        WHEN d_moy = 11
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS nov_net,
      sum(CASE
        WHEN d_moy = 12
          THEN cs_net_paid * cs_quantity
        ELSE 0
      END) AS dec_net
    
    FROM catalog_sales, warehouse, date_dim, time_dim, ship_mode
    
    WHERE cs_warehouse_sk = w_warehouse_sk and cs_sold_date_sk = d_date_sk and cs_sold_time_sk = t_time_sk and cs_ship_mode_sk = sm_ship_mode_sk and d_year = 1998 and t_time BETWEEN 46866 and 46866 + 28800 and sm_carrier IN ('GREAT EASTERN', 'UPS')
    
    GROUP BY 
      w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
   ) AS x
  
  GROUP BY 
    w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year
  
  ORDER BY w_warehouse_name
  
  LIMIT 100

),

SQLStatement_7 AS (

  SELECT sum(ss_quantity)
  
  FROM store_sales, store, customer_demographics, customer_address, date_dim
  
  WHERE s_store_sk = ss_store_sk and  ss_sold_date_sk = d_date_sk and d_year = 2001 and  
           ((cd_demo_sk = ss_cdemo_sk and 
             cd_marital_status = 'D' and 
             cd_education_status = 'Secondary' and 
             ss_sales_price BETWEEN 100.0 and 150.0) or
            (cd_demo_sk = ss_cdemo_sk and 
             cd_marital_status = 'W' and 
             cd_education_status = '2 yr Degree' and 
             ss_sales_price BETWEEN 50.0 and 100.0) or 
           (cd_demo_sk = ss_cdemo_sk and 
             cd_marital_status = 'U' and 
             cd_education_status = 'Unknown' and 
             ss_sales_price BETWEEN 150.0 and 200.0)) and
           ((ss_addr_sk = ca_address_sk and
            ca_country = 'United States' and
            ca_state IN ('VA', 'MI', 'FL') and ss_net_profit BETWEEN 0 and 2000) or
            (ss_addr_sk = ca_address_sk and
            ca_country = 'United States' and
            ca_state IN ('SC', 'GA', 'MN') and ss_net_profit BETWEEN 150 and 3000) or
            (ss_addr_sk = ca_address_sk and
            ca_country = 'United States' and
            ca_state IN ('OK', 'IA', 'TX') and ss_net_profit BETWEEN 50 and 25000))

),

Join_4 AS (

  SELECT 
    in1.W_WAREHOUSE_NAME AS W_WAREHOUSE_NAME,
    in1.W_WAREHOUSE_SQ_FT AS W_WAREHOUSE_SQ_FT,
    in1.JAN_SALES AS JAN_SALES
  
  FROM SQLStatement_7 AS in0
  INNER JOIN SQLStatement_6 AS in1
     ON in0."SUM(SS_QUANTITY)" != in1.W_WAREHOUSE_SQ_FT

)

SELECT *

FROM Join_4
