import duckdb

df = duckdb.query(
    """
                Select request_datetime,pickup_datetime,dropoff_datetime
                From './datasets/fhvhv_tripdata_2025-01.parquet'
                Limit 100
                
                """
).df()

print(df)
