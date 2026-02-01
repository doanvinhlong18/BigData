import duckdb
import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

OLD_FOLDER = os.path.join(BASE_DIR, "request_table", "*.parquet")
NEW_FOLDER = os.path.join(BASE_DIR, "sorted_request_table", "*.parquet")

old_files = sorted(glob.glob(OLD_FOLDER))
new_files = sorted(glob.glob(NEW_FOLDER))

if not old_files:
    raise FileNotFoundError("❌ No original parquet files found")

if not new_files:
    raise FileNotFoundError("❌ No sorted parquet files found")

print(f"📦 Old files: {len(old_files)}")
print(f"📦 New files: {len(new_files)}")

print("\n🔍 Checking row count & column consistency...\n")

for old_file, new_file in zip(old_files, new_files):
    old_name = os.path.basename(old_file)
    new_name = os.path.basename(new_file)

    print(f"📄 Comparing: {old_name}  <->  {new_name}")

    # Row count
    old_rows = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{old_file}')").fetchone()[0]
    new_rows = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{new_file}')").fetchone()[0]

    # Column info
    old_cols = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{old_file}')").df()
    new_cols = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{new_file}')").df()

    old_col_names = old_cols["column_name"].tolist()
    new_col_names = new_cols["column_name"].tolist()

    # Report
    print(f"   Rows old: {old_rows}")
    print(f"   Rows new: {new_rows}")

    if old_rows != new_rows:
        print("   ❌ ROW COUNT MISMATCH!")
    else:
        print("   ✅ Row count OK")

    if old_col_names != new_col_names:
        print("   ❌ COLUMN STRUCTURE MISMATCH!")
        print("   Old:", old_col_names)
        print("   New:", new_col_names)
    else:
        print("   ✅ Column structure OK")

    print("-" * 60)

print("\n🎉 Validation completed.")
