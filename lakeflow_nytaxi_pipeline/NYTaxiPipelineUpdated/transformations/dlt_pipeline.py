import dlt
from pyspark.sql.functions import col


import dlt
from pyspark.sql.functions import col, date_trunc

# Bronze layer - Ingest NYC Taxi data as streaming source
@dlt.table(
    name="nytaxi_bronzetogoldtransformaionnew",
    comment="Bronze layer"
)
def bronze():
    return spark.table("samples.nyctaxi.trips")
    

