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

global_trip_counter = 0

for cnt in range(1, 2):
    file_path = input_dir / f"fhvhv_tripdata_2025-{cnt:02d}.parquet"
    print(f"Processing {file_path} ...")

    dataset = ds.dataset(file_path, format="parquet")

    # Scanner đọc từng batch thay vì load full
    scanner = dataset.scanner(batch_size=100_000)

    request_writer = None
    response_writer = None

    for batch in scanner.to_batches():
        num_rows = batch.num_rows

        # ===== trip_id chung =====
        trip_ids = pa.array(range(global_trip_counter, global_trip_counter + num_rows))
        global_trip_counter += num_rows

        # ===== REQUEST TABLE =====
        request_batch = batch.select(request_cols)
        request_ids = pa.array(range(num_rows))
        request_batch = request_batch.append_column("trip_id", trip_ids)
        request_batch = request_batch.append_column("request_id", request_ids)

        if request_writer is None:
            request_writer = pq.ParquetWriter(
                output_request_dir / f"request_2025_{cnt:02d}.parquet",
                request_batch.schema,
                compression="snappy"
            )
        request_writer.write_table(pa.Table.from_batches([request_batch]))

        # ===== RESPONSE TABLE =====
        response_batch = batch.select(response_cols)
        response_ids = pa.array(range(num_rows))
        response_batch = response_batch.append_column("trip_id", trip_ids)
        response_batch = response_batch.append_column("response_id", response_ids)

        if response_writer is None:
            response_writer = pq.ParquetWriter(
                output_response_dir / f"response_2025_{cnt:02d}.parquet",
                response_batch.schema,
                compression="snappy"
            )
        response_writer.write_table(pa.Table.from_batches([response_batch]))

    if request_writer:
        request_writer.close()
    if response_writer:
        response_writer.close()

    print(f"Done month {cnt:02d}")

print("✅ All files processed without loading full files into RAM.")
