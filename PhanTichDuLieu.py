import pandas as pd
import pyarrow.parquet as pq
import os

from fastparquet.api import row_groups_map

file_path = "datasets/2018_taxi_trips.csv"

for cnt in range(1, 12):
    file_path = f"datasets/fhvhv_tripdata_2025-{cnt:02d}.parquet"

    pf = pq.ParquetFile(file_path)

    print(f"tháng {cnt}:")
    schema = pf.schema_arrow
    print("schema: ")
    print(schema)
    metadata = pf.metadata
    print("metadata: ")
    print(metadata)


