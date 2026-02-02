import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
from pathlib import Path
import shutil

def split_sort(input_file, tmp_dir, time_col):
    dataset = ds.dataset(input_file, format="parquet")
    scanner = dataset.scanner(batch_size=200_000)

    tmp_dir.mkdir(parents=True, exist_ok=True)
    part = 0

    for batch in scanner.to_batches():
        sort_idx = pc.sort_indices(batch, sort_keys=[(time_col, "ascending")])
        batch = pc.take(batch, sort_idx)

        pq.write_table(
            pa.Table.from_batches([batch]),
            tmp_dir / f"chunk_{part}.parquet",
            compression="snappy"
        )
        part += 1


def merge_sorted_chunks(tmp_dir, output_file, time_col):
    files = sorted(tmp_dir.glob("chunk_*.parquet"))

    batches = [pq.read_table(f).to_batches()[0] for f in files]
    pointers = [0] * len(batches)

    writer = None

    while True:
        min_val = None
        min_i = None

        for i, batch in enumerate(batches):
            if pointers[i] < batch.num_rows:
                val = batch.column(time_col)[pointers[i]].as_py()
                if min_val is None or val < min_val:
                    min_val = val
                    min_i = i

        if min_i is None:
            break

        row = batches[min_i].slice(pointers[min_i], 1)
        pointers[min_i] += 1

        if writer is None:
            writer = pq.ParquetWriter(output_file, row.schema, compression="snappy")

        writer.write_table(pa.Table.from_batches([row]))

    if writer:
        writer.close()


input_dir = Path("datasets")
request_dir = input_dir / "request_table"
response_dir = input_dir / "response_table"

for cnt in range(1, 12):
    print(f"🔄 Sorting month {cnt:02d}...")

    req_file = request_dir / f"request_2025_{cnt:02d}.parquet"
    tmp_req = request_dir / f"tmp_req_{cnt:02d}"

    split_sort(req_file, tmp_req, "request_datetime")
    merge_sorted_chunks(tmp_req, req_file, "request_datetime")
    shutil.rmtree(tmp_req)

    res_file = response_dir / f"response_2025_{cnt:02d}.parquet"
    tmp_res = response_dir / f"tmp_res_{cnt:02d}"

    split_sort(res_file, tmp_res, "dropoff_datetime")
    merge_sorted_chunks(tmp_res, res_file, "dropoff_datetime")
    shutil.rmtree(tmp_res)

    print(f"✅ Done month {cnt:02d}")

print("🚀 External sort completed without RAM explosion.")
