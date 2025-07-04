-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW `sample_trips_cdvpipeline_2025_07_04_10_03` AS
SELECT
    pickup_zip,
    fare_amount
FROM samples.nyctaxi.trips
