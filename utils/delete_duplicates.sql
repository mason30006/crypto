WITH CTE AS
(
	SELECT *,ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY timestamp) AS RN
	FROM dbo.hist_minute_bars
)
DELETE FROM CTE WHERE RN<>1