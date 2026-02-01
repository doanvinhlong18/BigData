import duckdb
import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# INPUT_FOLDER = os.path.join(BASE_DIR, "sorted_request_table", "*.parquet")
# OUTPUT_FOLDER = os.path.join(BASE_DIR, "preview_csv")

INPUT_FOLDER = os.path.join(BASE_DIR, "sorted_response_table", "*.parquet")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "preview_csv_2")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

files = sorted(glob.glob(INPUT_FOLDER))

if not files:
    raise FileNotFoundError(f"❌ No parquet files found at: {INPUT_FOLDER}")

print(f"📦 Found {len(files)} sorted parquet files")

for f in files:
    filename = os.path.basename(f).replace(".parquet", "_preview.csv")
    output_file = os.path.join(OUTPUT_FOLDER, filename)

    print(f"📤 Exporting preview: {filename}")

    duckdb.sql(f"""
    COPY (
        WITH 
        first_rows AS (
            SELECT *
            FROM read_parquet('{f}')
            ORDER BY dropoff_datetime ASC
            LIMIT 50
        ),
        last_rows AS (
            SELECT *
            FROM read_parquet('{f}')
            ORDER BY dropoff_datetime DESC
            LIMIT 50
        )
        SELECT * FROM first_rows
        UNION ALL
        SELECT * FROM last_rows
        ORDER BY dropoff_datetime ASC
    )
    TO '{output_file}' WITH (HEADER true);
    """)

print("✅ Done! Exported 50 first + 50 last rows per file.")
