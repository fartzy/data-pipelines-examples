-- Databricks notebook source
 SELECT * 
 FROM eligibility_pre

-- COMMAND ------------

SELECT 
  s1.memnum
  , t1.begindt
  --, t1.EndDate 
  , s1.enddt AS enddt
FROM eligibility_pre s1 
INNER JOIN eligibility_pre t1 
  ON s1.begindt <= date_add(t1.enddt, 1)
  AND s1.memnum = t1.memnum
  GROUP BY s1.memnum
ORDER BY memnum ASC, t1.begindt ASC

-- COMMAND ------------

  SELECT memnum
       , begindt AS ts, +1 AS type, 1 AS sub
  FROM eligibility_pre
  
  UNION ALL
  
  SELECT memnum
       , date_add(enddt, 1) AS ts
       , -1 AS type
       , 0 AS sub
  FROM eligibility_pre
  ORDER BY memnum ASC, type DESC

-- COMMAND ------------

 WITH C1 AS
 (
   SELECT memnum
        , begindt AS ts
        , 1 AS type
        , 1 AS sub
   FROM eligibility_pre
   
   UNION ALL
   
   SELECT memnum
        , date_add(enddt, 1) AS ts
        , -1 AS type
        , 0 AS sub
   FROM eligibility_pre
 )
 
  SELECT C1.*
     , SUM(type) OVER (
         PARTITION BY memnum 
         ORDER BY ts ASC, type DESC
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) - sub AS cnt
  FROM C1
  ORDER BY memnum ASC
         , ts ASC

-- COMMAND ------------

WITH C1 AS
(
  SELECT memnum
       , begindt AS ts
       , 1 AS type
       , 1 AS sub
  FROM eligibility_pre
  
  UNION ALL
  
  SELECT memnum
       , date_add(enddt, 1) AS ts
       , -1 AS type
       , 0 AS sub
  FROM eligibility_pre
),
C2 AS
(
  SELECT C1.*
    , SUM(type) OVER (
        PARTITION BY memnum 
        ORDER BY ts, type DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) - sub AS cnt
  FROM C1
),
C3 AS
(
  SELECT memnum
    , ts
    , FLOOR(
      (ROW_NUMBER() OVER (PARTITION BY memnum ORDER BY ts) - 1) / 2 + 1
      )
  AS grpnum
  FROM C2
  WHERE cnt = 0
)

SELECT memnum
    , MIN(ts) AS begindt
    , date_add(max(ts), -1) AS enddt
FROM C3
GROUP BY memnum, grpnum
ORDER BY memnum, begindt--