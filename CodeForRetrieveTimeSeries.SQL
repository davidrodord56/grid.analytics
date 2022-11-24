SELECT
    $__unixEpochGroupAlias(timestamp,$__interval),
    "demandaNAC2" AS "Demanda SIN",
    "enlaceNAC" AS "Cenace Forecast",
    CASE
        WHEN "enlaceNAC" <> 0 THEN "enlaceNAC"
        ELSE ( 
            SELECT
                "enlaceNAC"
            FROM
                cenacepower.webg X
            WHERE
                "enlaceNAC" <> 0
                AND timestamp <= cenacepower.webg.timestamp
            ORDER BY
                timestamp DESC
            LIMIT 1 
        )
    END AS "Pronostico SIN"
FROM
    cenacepower.webg
WHERE
  $__unixEpochFilter(timestamp)
ORDER BY timestamp;