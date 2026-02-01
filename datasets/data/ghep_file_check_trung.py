import duckdb
import glob
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_FOLDER = os.path.join(BASE_DIR, "sorted_request_table", "*.parquet")
OUTPUT_FILE = os.path.join(BASE_DIR, "merged_dedup_requests.parquet")

files = sorted(glob.glob(INPUT_FOLDER))

if not files:
    raise FileNotFoundError("❌ No sorted parquet files found")

print(f"📦 Found {len(files)} parquet files")

start_total = time.time()

# 1️⃣ Count total rows before merge
total_before = duckdb.sql(f"""
SELECT COUNT(*) 
FROM read_parquet({files})
""").fetchone()[0]

print(f"📊 Total rows before merge: {total_before:,}")

# 2️⃣ Count duplicates
dup_count = duckdb.sql(f"""
SELECT COUNT(*) 
FROM (
    SELECT request_id, request_datetime, COUNT(*) c
    FROM read_parquet({files})
    GROUP BY request_id, request_datetime
    HAVING COUNT(*) > 1
)
""").fetchone()[0]

print(f"⚠️ Duplicate groups found: {dup_count:,}")

# 3️⃣ Merge + Deduplicate + Export
print("🚀 Merging + removing duplicates...")

duckdb.sql(f"""
COPY (
    SELECT DISTINCT *
    FROM read_parquet({files})
)
TO '{OUTPUT_FILE}';
""")

# 4️⃣ Count final rows
total_after = duckdb.sql(f"""
SELECT COUNT(*) 
FROM read_parquet('{OUTPUT_FILE}')
""").fetchone()[0]

removed = total_before - total_after

elapsed = time.time() - start_total

print("\n✅ DONE")
print(f"📉 Rows removed (duplicates): {removed:,}")
print(f"📊 Final row count: {total_after:,}")
print(f"📁 Output file: {OUTPUT_FILE}")
print(f"⏱ Time: {elapsed:.2f}s")
