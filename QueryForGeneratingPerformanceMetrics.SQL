SELECT sum(("forecasted" - "demandaNAC2")^2) as "(Y^ - Yt)^2: ",
count("demandaNAC2") as "N: ",
sqrt(sum(("forecasted" - "demandaNAC2")^2) / count("demandaNAC2")) as "RMSE: ",
avg("demandaNAC2") as "AVG(y): ",
sqrt(sum(("forecasted" - "demandaNAC2")^2) / count("demandaNAC2")) / avg("demandaNAC2") as "NRMSE: "


FROM cenacepower.forecast
WHERE timestamp >= (SELECT min(timestamp)
FROM cenacepower.forecast
WHERE forecasted IS NOT NULL)