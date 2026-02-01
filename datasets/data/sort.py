import duckdb
import glob
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# INPUT_FOLDER = os.path.join(BASE_DIR, "request_table", "*.parquet")
# OUTPUT_FOLDER = os.path.join(BASE_DIR, "sorted_request_table")

INPUT_FOLDER = os.path.join(BASE_DIR, "response_table", "*.parquet")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "sorted_response_table")


os.makedirs(OUTPUT_FOLDER, exist_ok=True)

files = sorted(glob.glob(INPUT_FOLDER))

print("🔍 Searching parquet in:", INPUT_FOLDER)

if not files:
    raise FileNotFoundError(f"❌ No parquet files found at: {INPUT_FOLDER}")

print(f"📦 Found {len(files)} parquet files")

total_start = time.time()

for i, f in enumerate(files, 1):
    filename = os.path.basename(f)
    output_file = os.path.join(OUTPUT_FOLDER, filename)

    print(f"\n🚀 [{i}/{len(files)}] Sorting: {filename}")
    start = time.time()

    # duckdb.sql(f"""
    # COPY (
    #     SELECT *
    #     FROM read_parquet('{f}')
    #     ORDER BY request_datetime ASC
    # )
    # TO '{output_file}';
    # """)

    duckdb.sql(f"""
    COPY (
        SELECT *
        FROM read_parquet('{f}')
        ORDER BY dropoff_datetime ASC
    )
    TO '{output_file}';
    """)

    elapsed = time.time() - start
    print(f"✅ Saved: {output_file} ({elapsed:.2f}s)")

total_elapsed = time.time() - total_start
print(f"\n🎉 All files sorted successfully in {total_elapsed:.2f} seconds")
