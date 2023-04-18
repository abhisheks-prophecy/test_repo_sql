WITH SQLStatement_0 AS (

  WITH year_total AS (
  
    SELECT 
      c_customer_id AS customer_id,
      c_first_name AS customer_first_name,
      c_last_name AS customer_last_name,
      c_preferred_cust_flag AS customer_preferred_cust_flag,
      c_birth_country AS customer_birth_country,
      c_login AS customer_login,
      c_email_address AS customer_email_address,
      d_year AS dyear,
      sum(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total,
      's' AS sale_type
    
    FROM customer, store_sales, date_dim
    
    WHERE c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk
    
    GROUP BY 
      c_customer_id, 
      c_first_name, 
      c_last_name, 
      c_preferred_cust_flag, 
      c_birth_country, 
      c_login, 
      c_email_address, 
      d_year
    
    UNION ALL
    
    SELECT 
      c_customer_id AS customer_id,
      c_first_name AS customer_first_name,
      c_last_name AS customer_last_name,
      c_preferred_cust_flag AS customer_preferred_cust_flag,
      c_birth_country AS customer_birth_country,
      c_login AS customer_login,
      c_email_address AS customer_email_address,
      d_year AS dyear,
      sum((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) AS year_total,
      'c' AS sale_type
    
    FROM customer, catalog_sales, date_dim
    
    WHERE c_customer_sk = cs_bill_customer_sk and cs_sold_date_sk = d_date_sk
    
    GROUP BY 
      c_customer_id, 
      c_first_name, 
      c_last_name, 
      c_preferred_cust_flag, 
      c_birth_country, 
      c_login, 
      c_email_address, 
      d_year
    
    UNION ALL
    
    SELECT 
      c_customer_id AS customer_id,
      c_first_name AS customer_first_name,
      c_last_name AS customer_last_name,
      c_preferred_cust_flag AS customer_preferred_cust_flag,
      c_birth_country AS customer_birth_country,
      c_login AS customer_login,
      c_email_address AS customer_email_address,
      d_year AS dyear,
      sum((((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) AS year_total,
      'w' AS sale_type
    
    FROM customer, web_sales, date_dim
    
    WHERE c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk
    
    GROUP BY 
      c_customer_id, 
      c_first_name, 
      c_last_name, 
      c_preferred_cust_flag, 
      c_birth_country, 
      c_login, 
      c_email_address, 
      d_year
  
  )
  
  SELECT 
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name,
    t_s_secyear.customer_login
  
  FROM year_total AS t_s_firstyear, year_total AS t_s_secyear, year_total AS t_c_firstyear, year_total AS t_c_secyear, year_total AS t_w_firstyear, year_total AS t_w_secyear
  
  WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id and t_s_firstyear.customer_id = t_c_secyear.customer_id and t_s_firstyear.customer_id = t_c_firstyear.customer_id and t_s_firstyear.customer_id = t_w_firstyear.customer_id and t_s_firstyear.customer_id = t_w_secyear.customer_id and t_s_firstyear.sale_type = 's' and t_c_firstyear.sale_type = 'c' and t_w_firstyear.sale_type = 'w' and t_s_secyear.sale_type = 's' and t_c_secyear.sale_type = 'c' and t_w_secyear.sale_type = 'w' and t_s_firstyear.dyear = 1999 and t_s_secyear.dyear = 1999 + 1 and t_c_firstyear.dyear = 1999 and t_c_secyear.dyear = 1999 + 1 and t_w_firstyear.dyear = 1999 and t_w_secyear.dyear = 1999 + 1 and t_s_firstyear.year_total > 0 and t_c_firstyear.year_total > 0 and t_w_firstyear.year_total > 0 and CASE
    WHEN t_c_firstyear.year_total > 0
      THEN t_c_secyear.year_total / t_c_firstyear.year_total
    ELSE NULL
  END > CASE
    WHEN t_s_firstyear.year_total > 0
      THEN t_s_secyear.year_total / t_s_firstyear.year_total
    ELSE NULL
  END and CASE
    WHEN t_c_firstyear.year_total > 0
      THEN t_c_secyear.year_total / t_c_firstyear.year_total
    ELSE NULL
  END > CASE
    WHEN t_w_firstyear.year_total > 0
      THEN t_w_secyear.year_total / t_w_firstyear.year_total
    ELSE NULL
  END
  
  ORDER BY t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name, t_s_secyear.customer_login
  
  LIMIT 100

),

SQLStatement_3 AS (

  WITH ssr AS (
  
    SELECT 
      s_store_id,
      sum(sales_price) AS sales,
      sum(profit) AS profit,
      sum(return_amt) AS returns,
      sum(net_loss) AS profit_loss
    
    FROM (
      SELECT 
        ss_store_sk AS store_sk,
        ss_sold_date_sk AS date_sk,
        ss_ext_sales_price AS sales_price,
        ss_net_profit AS profit,
        CAST(0 AS decimal (7, 2)) AS return_amt,
        CAST(0 AS decimal (7, 2)) AS net_loss
      
      FROM store_sales
      
      UNION ALL
      
      SELECT 
        sr_store_sk AS store_sk,
        sr_returned_date_sk AS date_sk,
        CAST(0 AS decimal (7, 2)) AS sales_price,
        CAST(0 AS decimal (7, 2)) AS profit,
        sr_return_amt AS return_amt,
        sr_net_loss AS net_loss
      
      FROM store_returns
     ) AS salesreturns, date_dim, store
    
    WHERE date_sk = d_date_sk and d_date BETWEEN CAST('2002-08-09' AS date) and dateadd(DAY, 14, CAST('2002-08-09' AS date)) and store_sk = s_store_sk
    
    GROUP BY s_store_id
  
  ),
  
  csr AS (
  
    SELECT 
      cp_catalog_page_id,
      sum(sales_price) AS sales,
      sum(profit) AS profit,
      sum(return_amt) AS returns,
      sum(net_loss) AS profit_loss
    
    FROM (
      SELECT 
        cs_catalog_page_sk AS page_sk,
        cs_sold_date_sk AS date_sk,
        cs_ext_sales_price AS sales_price,
        cs_net_profit AS profit,
        CAST(0 AS decimal (7, 2)) AS return_amt,
        CAST(0 AS decimal (7, 2)) AS net_loss
      
      FROM catalog_sales
      
      UNION ALL
      
      SELECT 
        cr_catalog_page_sk AS page_sk,
        cr_returned_date_sk AS date_sk,
        CAST(0 AS decimal (7, 2)) AS sales_price,
        CAST(0 AS decimal (7, 2)) AS profit,
        cr_return_amount AS return_amt,
        cr_net_loss AS net_loss
      
      FROM catalog_returns
     ) AS salesreturns, date_dim, catalog_page
    
    WHERE date_sk = d_date_sk and d_date BETWEEN CAST('2002-08-09' AS date) and dateadd(DAY, 14, CAST('2002-08-09' AS date)) and page_sk = cp_catalog_page_sk
    
    GROUP BY cp_catalog_page_id
  
  ),
  
  wsr AS (
  
    SELECT 
      web_site_id,
      sum(sales_price) AS sales,
      sum(profit) AS profit,
      sum(return_amt) AS returns,
      sum(net_loss) AS profit_loss
    
    FROM (
      SELECT 
        ws_web_site_sk AS wsr_web_site_sk,
        ws_sold_date_sk AS date_sk,
        ws_ext_sales_price AS sales_price,
        ws_net_profit AS profit,
        CAST(0 AS decimal (7, 2)) AS return_amt,
        CAST(0 AS decimal (7, 2)) AS net_loss
      
      FROM web_sales
      
      UNION ALL
      
      SELECT 
        ws_web_site_sk AS wsr_web_site_sk,
        wr_returned_date_sk AS date_sk,
        CAST(0 AS decimal (7, 2)) AS sales_price,
        CAST(0 AS decimal (7, 2)) AS profit,
        wr_return_amt AS return_amt,
        wr_net_loss AS net_loss
      
      FROM web_returns
      LEFT JOIN web_sales
         ON (wr_item_sk = ws_item_sk and wr_order_number = ws_order_number)
     ) AS salesreturns, date_dim, web_site
    
    WHERE date_sk = d_date_sk and d_date BETWEEN CAST('2002-08-09' AS date) and dateadd(DAY, 14, CAST('2002-08-09' AS date)) and wsr_web_site_sk = web_site_sk
    
    GROUP BY web_site_id
  
  )
  
  SELECT 
    channel,
    id,
    sum(sales) AS sales,
    sum(returns) AS returns,
    sum(profit) AS profit
  
  FROM (
    SELECT 
      'store channel' AS channel,
      'store' || s_store_id AS id,
      sales,
      returns,
      (profit - profit_loss) AS profit
    
    FROM ssr
    
    UNION ALL
    
    SELECT 
      'catalog channel' AS channel,
      'catalog_page' || cp_catalog_page_id AS id,
      sales,
      returns,
      (profit - profit_loss) AS profit
    
    FROM csr
    
    UNION ALL
    
    SELECT 
      'web channel' AS channel,
      'web_site' || web_site_id AS id,
      sales,
      returns,
      (profit - profit_loss) AS profit
    
    FROM wsr
   ) AS x
  
  GROUP BY ROLLUP(channel, id)
  
  ORDER BY channel, id
  
  LIMIT 100

),

Join_1 AS (

  SELECT 
    in0.CUSTOMER_ID AS CUSTOMER_ID,
    in0.CUSTOMER_FIRST_NAME AS CUSTOMER_FIRST_NAME,
    in0.CUSTOMER_LAST_NAME AS CUSTOMER_LAST_NAME,
    in0.CUSTOMER_LOGIN AS CUSTOMER_LOGIN
  
  FROM SQLStatement_0 AS in0
  INNER JOIN SQLStatement_3 AS in1
     ON in0.CUSTOMER_ID != in1.ID

),

SQLStatement_1 AS (

  WITH wscs AS (
  
    SELECT 
      sold_date_sk,
      sales_price
    
    FROM (
      SELECT 
        ws_sold_date_sk AS sold_date_sk,
        ws_ext_sales_price AS sales_price
      
      FROM web_sales
      
      UNION ALL
      
      SELECT 
        cs_sold_date_sk AS sold_date_sk,
        cs_ext_sales_price AS sales_price
      
      FROM catalog_sales
     ) AS x
  
  ),
  
  wswscs AS (
  
    SELECT 
      d_week_seq,
      sum(CASE
        WHEN (d_day_name = 'Sunday')
          THEN sales_price
        ELSE NULL
      END) AS sun_sales,
      sum(CASE
        WHEN (d_day_name = 'Monday')
          THEN sales_price
        ELSE NULL
      END) AS mon_sales,
      sum(CASE
        WHEN (d_day_name = 'Tuesday')
          THEN sales_price
        ELSE NULL
      END) AS tue_sales,
      sum(CASE
        WHEN (d_day_name = 'Wednesday')
          THEN sales_price
        ELSE NULL
      END) AS wed_sales,
      sum(CASE
        WHEN (d_day_name = 'Thursday')
          THEN sales_price
        ELSE NULL
      END) AS thu_sales,
      sum(CASE
        WHEN (d_day_name = 'Friday')
          THEN sales_price
        ELSE NULL
      END) AS fri_sales,
      sum(CASE
        WHEN (d_day_name = 'Saturday')
          THEN sales_price
        ELSE NULL
      END) AS sat_sales
    
    FROM wscs, date_dim
    
    WHERE d_date_sk = sold_date_sk
    
    GROUP BY d_week_seq
  
  )
  
  SELECT 
    d_week_seq1,
    round(sun_sales1 / sun_sales2, 2),
    round(mon_sales1 / mon_sales2, 2),
    round(tue_sales1 / tue_sales2, 2),
    round(wed_sales1 / wed_sales2, 2),
    round(thu_sales1 / thu_sales2, 2),
    round(fri_sales1 / fri_sales2, 2),
    round(sat_sales1 / sat_sales2, 2)
  
  FROM (
    SELECT 
      wswscs.d_week_seq AS d_week_seq1,
      sun_sales AS sun_sales1,
      mon_sales AS mon_sales1,
      tue_sales AS tue_sales1,
      wed_sales AS wed_sales1,
      thu_sales AS thu_sales1,
      fri_sales AS fri_sales1,
      sat_sales AS sat_sales1
    
    FROM wswscs, date_dim
    
    WHERE date_dim.d_week_seq = wswscs.d_week_seq and
                    d_year = 1999
   ) AS y, (
    SELECT 
      wswscs.d_week_seq AS d_week_seq2,
      sun_sales AS sun_sales2,
      mon_sales AS mon_sales2,
      tue_sales AS tue_sales2,
      wed_sales AS wed_sales2,
      thu_sales AS thu_sales2,
      fri_sales AS fri_sales2,
      sat_sales AS sat_sales2
    
    FROM wswscs, date_dim
    
    WHERE date_dim.d_week_seq = wswscs.d_week_seq and
                    d_year = 1999 + 1
   ) AS z
  
  WHERE d_week_seq1 = d_week_seq2 - 53
  
  ORDER BY d_week_seq1

),

SQLStatement_2 AS (

  SELECT 
    s_store_name,
    sum(ss_net_profit)
  
  FROM store_sales, date_dim, store, (
    SELECT ca_zip
    
    FROM (
      SELECT substr(ca_zip, 1, 5) AS ca_zip
      
      FROM customer_address
      
      WHERE substr(ca_zip, 1, 5) IN 
      (  '10338',
        '56623',
        '51423',
        '26456',
        '19500',
        '65832',
        '17178',
        '68879',
        '49935',
        '49849',
        '93956',
        '71765',
        '45100',
        '50587',
        '68389',
        '41899',
        '98316',
        '56217',
        '94686',
        '59350',
        '32857',
        '14925',
        '31266',
        '37817',
        '27519',
        '20787',
        '26967',
        '49045',
        '39397',
        '32010',
        '23144',
        '53580',
        '15491',
        '74151',
        '18442',
        '51916',
        '17730',
        '22824',
        '28290',
        '21657',
        '45460',
        '39386',
        '21133',
        '35017',
        '19894',
        '21759',
        '79293',
        '86733',
        '76777',
        '41688',
        '13810',
        '49053',
        '17992',
        '13395',
        '19869',
        '40785',
        '63897',
        '65049',
        '27388',
        '94701',
        '41482',
        '97923',
        '23951',
        '88284',
        '61718',
        '94317',
        '72294',
        '63544',
        '31306',
        '41242',
        '28830',
        '75535',
        '86189',
        '88177',
        '16147',
        '12902',
        '48271',
        '54036',
        '20936',
        '27802',
        '96741',
        '70286',
        '75710',
        '16034',
        '90285',
        '22058',
        '52590',
        '40584',
        '62441',
        '64039',
        '68999',
        '64327',
        '33844',
        '52497',
        '88495',
        '25989',
        '67814',
        '13767',
        '83194',
        '99395',
        '35524',
        '89640',
        '48834',
        '51875',
        '71073',
        '25383',
        '19129',
        '57805',
        '47962',
        '61905',
        '19557',
        '74159',
        '98032',
        '13917',
        '50936',
        '47993',
        '41606',
        '17592',
        '11470',
        '28216',
        '19732',
        '97958',
        '60997',
        '85688',
        '96863',
        '16605',
        '10898',
        '31340',
        '71340',
        '72902',
        '98949',
        '74440',
        '53057',
        '30323',
        '76166',
        '27195',
        '11204',
        '32771',
        '38189',
        '83221',
        '22295',
        '15325',
        '20844',
        '65549',
        '69207',
        '71903',
        '63929',
        '56922',
        '25733',
        '75482',
        '14986',
        '79223',
        '73692',
        '98769',
        '70275',
        '33793',
        '13057',
        '30142',
        '95737',
        '30072',
        '32097',
        '25845',
        '50282',
        '19289',
        '92221',
        '59533',
        '37375',
        '29706',
        '48186',
        '22385',
        '55809',
        '17416',
        '10592',
        '55385',
        '71829',
        '91975',
        '73557',
        '38036',
        '10448',
        '95252',
        '51386',
        '14190',
        '15247',
        '39907',
        '79438',
        '78053',
        '66623',
        '27720',
        '84139',
        '74147',
        '58637',
        '11434',
        '36573',
        '10081',
        '53536',
        '41724',
        '97898',
        '36752',
        '50384',
        '87352',
        '35696',
        '69486',
        '50026',
        '27837',
        '42592',
        '58865',
        '80523',
        '53682',
        '65423',
        '77611',
        '98529',
        '13909',
        '13727',
        '52190',
        '36152',
        '48355',
        '62496',
        '16527',
        '18143',
        '98830',
        '75198',
        '73043',
        '64043',
        '63042',
        '67797',
        '50656',
        '27700',
        '60687',
        '57905',
        '94404',
        '15733',
        '80809',
        '74562',
        '84493',
        '67977',
        '11213',
        '19125',
        '84496',
        '16435',
        '97510',
        '46040',
        '33968',
        '20256',
        '42332',
        '16480',
        '54277',
        '82819',
        '93799',
        '69101',
        '57689',
        '42821',
        '68073',
        '49342',
        '46915',
        '25825',
        '92332',
        '20219',
        '96577',
        '49463',
        '19221',
        '35814',
        '64783',
        '97303',
        '52061',
        '24357',
        '58167',
        '56286',
        '64474',
        '99847',
        '53626',
        '39703',
        '24880',
        '24365',
        '50652',
        '29611',
        '90638',
        '59246',
        '27171',
        '30483',
        '11708',
        '38630',
        '81914',
        '48269',
        '11720',
        '88662',
        '68844',
        '54838',
        '93795',
        '38102',
        '33481',
        '97546',
        '49306',
        '97216',
        '49032',
        '14270',
        '72418',
        '32540',
        '53208',
        '15588',
        '29990',
        '10407',
        '92334',
        '48543',
        '51495',
        '77996',
        '53686',
        '14827',
        '30978',
        '30482',
        '86296',
        '48869',
        '59600',
        '29495',
        '24775',
        '34645',
        '19763',
        '98602',
        '20456',
        '10468',
        '13887',
        '65714',
        '74740',
        '37096',
        '96240',
        '44111',
        '54109',
        '62693',
        '87874',
        '64295',
        '62027',
        '86027',
        '54341',
        '68582',
        '67809',
        '44159',
        '97913',
        '79150',
        '38974',
        '64754',
        '73946',
        '20840',
        '16138',
        '58939',
        '20428',
        '19890',
        '70842',
        '78648',
        '55576',
        '37267',
        '40470',
        '12957',
        '57553',
        '53593',
        '34067',
        '22555',
        '79719',
        '25809',
        '28496',
        '11083',
        '87624',
        '83622',
        '84898',
        '28678',
        '14297',
        '79461',
        '22910',
        '87129',
        '49941',
        '64817',
        '93905',
        '39721',
        '81837',
        '18753',
        '86432',
        '67821',
        '66080',
        '28246',
        '13466',
        '16363',
        '56950',
        '35446',
        '58326',
        '11760',
        '33962',
        '28399',
        '45848',
        '52560',
        '66894',
        '15169',
        '20988',
        '85925',
        '38582',
        '34825',
        '94227',
        '56758',
        '24801',
        '14128',
        '14012',
        '35824',
        '49784'
      )
      
      INTERSECT
      
      SELECT ca_zip
      
      FROM (
        SELECT 
          substr(ca_zip, 1, 5) AS ca_zip,
          count(*) AS cnt
        
        FROM customer_address, customer
        
        WHERE ca_address_sk = c_current_addr_sk and
                                        c_preferred_cust_flag = 'Y'
        
        GROUP BY ca_zip
        
        HAVING count(*) > 10
       ) AS A1
     ) AS A2
   ) AS V1
  
  WHERE ss_store_sk = s_store_sk and ss_sold_date_sk = d_date_sk and d_qoy = 1 and d_year = 2002 and (substr(s_zip, 1, 2) = substr(V1.ca_zip, 1, 2))
  
  GROUP BY s_store_name
  
  ORDER BY s_store_name
  
  LIMIT 100

),

Join_2 AS (

  SELECT 
    in1.S_STORE_NAME AS S_STORE_NAME,
    in1."SUM(SS_NET_PROFIT)" AS SS_NET_PROFIT,
    in0."ROUND(SUN_SALES1 / SUN_SALES2, 2)" AS ROUND_SUN_SALES1_SUN_SALES2
  
  FROM SQLStatement_1 AS in0
  INNER JOIN SQLStatement_2 AS in1
     ON in0."ROUND(FRI_SALES1 / FRI_SALES2, 2)" != in1."SUM(SS_NET_PROFIT)"

),

SQLStatement_4 AS (

  WITH ssales AS (
  
    SELECT 
      c_last_name,
      c_first_name,
      s_store_name,
      ca_state,
      s_state,
      i_color,
      i_current_price,
      i_manager_id,
      i_units,
      i_size,
      sum(ss_net_paid_inc_tax) AS netpaid
    
    FROM store_sales, store_returns, store, item, customer, customer_address
    
    WHERE ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk and ss_customer_sk = c_customer_sk and ss_item_sk = i_item_sk and ss_store_sk = s_store_sk and c_current_addr_sk = ca_address_sk and c_birth_country <> upper(ca_country) and s_zip = ca_zip and s_market_id = 8
    
    GROUP BY 
      c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size
  
  )
  
  SELECT 
    c_last_name,
    c_first_name,
    s_store_name,
    sum(netpaid) AS paid
  
  FROM ssales
  
  WHERE i_color = 'cornsilk'
  
  GROUP BY 
    c_last_name, c_first_name, s_store_name
  
  HAVING sum(netpaid) > (
    SELECT 0.05 * avg(netpaid)
    
    FROM ssales
   )
  
  ORDER BY c_last_name, c_first_name, s_store_name

),

SQLStatement_5 AS (

  WITH ss AS (
  
    SELECT 
      ca_county,
      d_qoy,
      d_year,
      sum(ss_ext_sales_price) AS store_sales
    
    FROM store_sales, date_dim, customer_address
    
    WHERE ss_sold_date_sk = d_date_sk and ss_addr_sk = ca_address_sk
    
    GROUP BY 
      ca_county, d_qoy, d_year
  
  ),
  
  ws AS (
  
    SELECT 
      ca_county,
      d_qoy,
      d_year,
      sum(ws_ext_sales_price) AS web_sales
    
    FROM web_sales, date_dim, customer_address
    
    WHERE ws_sold_date_sk = d_date_sk and ws_bill_addr_sk = ca_address_sk
    
    GROUP BY 
      ca_county, d_qoy, d_year
  
  )
  
  SELECT 
    ss1.ca_county,
    ss1.d_year,
    ws2.web_sales / ws1.web_sales AS web_q1_q2_increase,
    ss2.store_sales / ss1.store_sales AS store_q1_q2_increase,
    ws3.web_sales / ws2.web_sales AS web_q2_q3_increase,
    ss3.store_sales / ss2.store_sales AS store_q2_q3_increase
  
  FROM ss AS ss1, ss AS ss2, ss AS ss3, ws AS ws1, ws AS ws2, ws AS ws3
  
  WHERE ss1.d_qoy = 1 and ss1.d_year = 1999 and ss1.ca_county = ss2.ca_county and ss2.d_qoy = 2 and ss2.d_year = 1999 and ss2.ca_county = ss3.ca_county and ss3.d_qoy = 3 and ss3.d_year = 1999 and ss1.ca_county = ws1.ca_county and ws1.d_qoy = 1 and ws1.d_year = 1999 and ws1.ca_county = ws2.ca_county and ws2.d_qoy = 2 and ws2.d_year = 1999 and ws1.ca_county = ws3.ca_county and ws3.d_qoy = 3 and ws3.d_year = 1999 and CASE
    WHEN ws1.web_sales > 0
      THEN ws2.web_sales / ws1.web_sales
    ELSE NULL
  END > CASE
    WHEN ss1.store_sales > 0
      THEN ss2.store_sales / ss1.store_sales
    ELSE NULL
  END and CASE
    WHEN ws2.web_sales > 0
      THEN ws3.web_sales / ws2.web_sales
    ELSE NULL
  END > CASE
    WHEN ss2.store_sales > 0
      THEN ss3.store_sales / ss2.store_sales
    ELSE NULL
  END
  
  ORDER BY web_q1_q2_increase

),

Join_3 AS (

  SELECT 
    in0.C_LAST_NAME AS C_LAST_NAME,
    in0.PAID AS PAID,
    in1.D_YEAR AS D_YEAR
  
  FROM SQLStatement_4 AS in0
  INNER JOIN SQLStatement_5 AS in1
     ON in0.C_LAST_NAME != in1.CA_COUNTY

),

SQLStatement_6 AS (

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

),

Join_5 AS (

  SELECT * 
  
  FROM Join_3 AS in0
  INNER JOIN Join_4 AS in1
     ON in0.C_LAST_NAME != in1.W_WAREHOUSE_NAME

),

Join_6 AS (

  SELECT 
    in0.CUSTOMER_ID AS CUSTOMER_ID,
    in0.CUSTOMER_FIRST_NAME AS CUSTOMER_FIRST_NAME,
    in1.SS_NET_PROFIT AS SUM_SS_NET_PROFIT
  
  FROM Join_1 AS in0
  INNER JOIN Join_2 AS in1
     ON in0.CUSTOMER_ID != in1.S_STORE_NAME

),

Join_7 AS (

  SELECT 
    in0.CUSTOMER_ID AS CUSTOMER_ID,
    in0.CUSTOMER_FIRST_NAME AS CUSTOMER_FIRST_NAME,
    in1.D_YEAR AS D_YEAR
  
  FROM Join_6 AS in0
  INNER JOIN Join_5 AS in1
     ON in0.CUSTOMER_ID != in1.W_WAREHOUSE_NAME

)

SELECT *

FROM Join_7
