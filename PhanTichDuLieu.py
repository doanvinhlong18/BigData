import pandas as pd
import pyarrow.parquet as pq
import os

from fastparquet.api import row_groups_map

file_path_csv = "datasets/weather_full.csv"

for cnt in range(1, 12):
    file_path = f"datasets/fhvhv_tripdata_2025-{cnt:02d}.parquet"

    df = pd.read_csv(file_path_csv)
    pf = pq.ParquetFile(file_path)

    batch = next(pf.iter_batches(batch_size=10000))
    pdf = batch.to_pandas()
    print(f"tháng {cnt}:")
    # print(df.info())
    print(df["datetime"].min())
    print(df["datetime"].max())
    print("-----")
    print(pdf["request_datetime"].min())
    print(pdf["request_datetime"].max())

    # print(f"tháng {cnt}:")
    # schema = pf.schema_arrow
    # print("schema: ")
    # print(schema)
    # metadata = pf.metadata
    # print("metadata: ")
    # print(metadata)


