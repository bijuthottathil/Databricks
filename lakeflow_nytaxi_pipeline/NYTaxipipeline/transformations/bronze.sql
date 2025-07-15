CREATE OR REFRESH STREAMING LIVE TABLE nytaxi_bronzenew
COMMENT "Raw NYC Taxi data from Delta source"
AS
SELECT * FROM stream(samples.nyctaxi.trips)
LIMIT 10