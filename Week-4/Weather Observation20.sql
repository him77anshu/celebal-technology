SELECT ROUND(AVG(LAT_N), 4) AS MEDIAN_LAT
FROM (
    SELECT LAT_N
    FROM (
        SELECT LAT_N, @rownum := @rownum + 1 AS rn
        FROM STATION, (SELECT @rownum := 0) r
        ORDER BY LAT_N
    ) AS ordered_lat
    WHERE rn IN (
        FLOOR((@rownum + 1) / 2),
        CEIL((@rownum + 1) / 2)
    )
) AS median_vals