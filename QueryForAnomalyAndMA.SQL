SELECT
    $__unixEpochGroupAlias(timestamp,$__interval),
    "demandaNAC2" AS "Demanda SIN",
    "enlaceNAC" AS "Cenace Forecast Fixed",
    forecasted as "Moving Average Forecasted",
    avg("demandaNAC2") OVER (ORDER BY timestamp ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as "Moving Average 20 MIN",
  CASE
    WHEN abs(("demandaNAC2" - avg("demandaNAC2") OVER (ORDER BY timestamp ROWS BETWEEN 3 PRECEDING AND CURRENT ROW))/ (avg("demandaNAC2") OVER (ORDER BY timestamp ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) ) > 0.2
    THEN avg("demandaNAC2") OVER (ORDER BY timestamp ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
    ELSE "demandaNAC2"
END AS "Demanda Cleansed"  

FROM
    cenacepower.forecast
WHERE
  $__unixEpochFilter(timestamp)
ORDER BY timestamp;