# ...existing code...
import os
import shutil
from pathlib import Path
import duckdb

src_path = r"c:\D\nam4_ki2\BigData\datasets\fvhfv\2022"
tmp_path = r"c:\D\nam4_ki2\BigData\datasets\fvhfv\2022__tmp_duckdb_cast"
out_file = os.path.join(tmp_path, "data.parquet")

if os.path.exists(tmp_path):
    shutil.rmtree(tmp_path)
os.makedirs(tmp_path, exist_ok=True)

con = duckdb.connect()

glob_path = (Path(src_path).as_posix() + "/**/*.parquet").replace("'", "''")
out_file_sql = Path(out_file).as_posix().replace("'", "''")

sql = f"""
CREATE OR REPLACE TEMP VIEW v2022 AS
SELECT
  * REPLACE (
    TRY_CAST(PULocationID AS INTEGER) AS PULocationID,
    TRY_CAST(DOLocationID AS INTEGER) AS DOLocationID
  )
FROM read_parquet('{glob_path}', union_by_name=true, hive_partitioning=true);

COPY (SELECT * FROM v2022)
TO '{out_file_sql}' (FORMAT PARQUET, COMPRESSION ZSTD);
"""
con.execute(sql)
con.close()

if os.path.exists(src_path):
    shutil.rmtree(src_path)
os.replace(tmp_path, src_path)

print("✅ Done: cast PULocationID, DOLocationID -> INTEGER và đã lưu đè folder 2022")
# ...existing code...
