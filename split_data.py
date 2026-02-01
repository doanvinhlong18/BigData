import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

input_dir = Path("datasets")
output_request_dir = Path("datasets/request_table")
output_response_dir = Path("datasets/response_table")

output_request_dir.mkdir(parents=True, exist_ok=True)
output_response_dir.mkdir(parents=True, exist_ok=True)

request_cols = [
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime",
    "PULocationID",
    "shared_request_flag",
    "access_a_ride_flag",
    "wav_request_flag"
]

response_cols = [
    "hvfhs_license_num",
    "dispatching_base_num",
    "pickup_datetime",
    "on_scene_datetime",
    "dropoff_datetime",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "shared_match_flag",
    "wav_match_flag",
    "driver_pay"
]

for cnt in range(1, 12):
    file_path = input_dir / f"fhvhv_tripdata_2025-{cnt:02d}.parquet"
    print(f"Processing {file_path} ...")
    dataset = ds.dataset(file_path, format="parquet")
    request_table = dataset.to_table(columns=request_cols)
    pq.write_table(
        request_table,
        output_request_dir / f"request_2025_{cnt:02d}.parquet",
        compression="snappy"
    )

    response_table = dataset.to_table(columns=response_cols)
    pq.write_table(
        response_table,
        output_response_dir / f"response_2025_{cnt:02d}.parquet",
        compression="snappy"
    )

    print(f"Done month {cnt:02d}")

print("✅ All files processed.")
