import duckdb
import glob
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_FOLDER = os.path.join(BASE_DIR, "sorted_request_table", "*.parquet")
OUTPUT_FILE = os.path.join(BASE_DIR, "duplicate_requests.csv")

files = sorted(glob.glob(INPUT_FOLDER))

if not files:
    raise FileNotFoundError("❌ No parquet files found")

print(f"📦 Found {len(files)} parquet files")

start_total = time.time()

# 1️⃣ Count total rows
total_before = duckdb.sql(f"""
SELECT COUNT(*) 
FROM read_parquet({files})
""").fetchone()[0]

print(f"📊 Total rows: {total_before:,}")

# 2️⃣ Find duplicate groups
dup_groups = duckdb.sql(f"""
SELECT COUNT(*) 
FROM (
    SELECT request_id, request_datetime
    FROM read_parquet({files})
    GROUP BY request_id, request_datetime
    HAVING COUNT(*) > 1
)
""").fetchone()[0]

print(f"⚠️ Duplicate groups: {dup_groups:,}")

# 3️⃣ Export ALL duplicated rows
print("🚀 Exporting duplicate rows to CSV...")

duckdb.sql(f"""
COPY (
    SELECT *
    FROM read_parquet({files})
    WHERE (request_id, request_datetime) IN (
        SELECT request_id, request_datetime
        FROM read_parquet({files})
        GROUP BY request_id, request_datetime
        HAVING COUNT(*) > 1
    )
)
TO '{OUTPUT_FILE}'
WITH (HEADER, DELIMITER ',');
""")

# 4️⃣ Count exported duplicate rows
dup_rows = duckdb.sql(f"""
SELECT COUNT(*) 
FROM read_csv_auto('{OUTPUT_FILE}')
""").fetchone()[0]

elapsed = time.time() - start_total

print("\n✅ DONE")
print(f"📄 Duplicate rows exported: {dup_rows:,}")
print(f"📁 Output file: {OUTPUT_FILE}")
print(f"⏱ Time: {elapsed:.2f}s")
